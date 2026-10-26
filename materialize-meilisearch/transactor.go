package connector

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/meilisearch/meilisearch-go"
	"golang.org/x/sync/errgroup"
)

const (
	// Number of document IDs fetched by a single Load request.
	loadBatchSize = 1000
	loadWorkers   = 4

	// Size of the NDJSON payload of a single document addition request.
	// Meilisearch rejects payloads larger than its http-payload-size-limit,
	// which defaults to 100MB. Larger payloads index more efficiently, but
	// Meilisearch also automatically batches consecutive additions to the
	// same index, so there's little to gain from going much larger.
	storeBatchBytes = 10 * 1024 * 1024
	// Number of document IDs deleted by a single request.
	deleteBatchSize = 10000
	storeWorkers    = 4

	// Document IDs are limited to 511 bytes.
	// See https://www.meilisearch.com/docs/learn/resources/known_limitations#length-of-primary-key-values
	maxDocumentIDBytes = 511
)

type binding struct {
	index        string
	path         []string
	deltaUpdates bool

	// JSON-encoded attribute names of the selected key and value fields,
	// each followed by a colon, in the order of the stored key and values
	// tuples. Pre-encoding these avoids building a map for every document.
	fieldPrefixes [][]byte

	// Attribute name of the root document, empty if it is not selected.
	docField       string
	docFieldPrefix []byte
}

type transactor struct {
	cfg      config
	client   *client
	bindings []binding
	be       *m.BindingEvents
}

func newBinding(path []string, deltaUpdates bool, fields []string, docField string) binding {
	b := binding{
		index:        path[0],
		path:         path,
		deltaUpdates: deltaUpdates,
		docField:     docField,
	}
	for _, f := range fields {
		b.fieldPrefixes = append(b.fieldPrefixes, attributePrefix(f))
	}
	if docField != "" {
		b.docFieldPrefix = attributePrefix(docField)
	}
	return b
}

func attributePrefix(attr string) []byte {
	enc, _ := json.Marshal(attr) // A string always marshals.
	return append(enc, ':')
}

// documentID returns the ID of the document with the given packed collection
// key. Document IDs may only contain alphanumerics, '-' and '_', which the
// URL-safe base64 alphabet satisfies. IDs of keys that are too long are
// replaced by a hash of the key.
func documentID(packedKey []byte) string {
	id := base64.RawURLEncoding.EncodeToString(packedKey)
	if len(id) > maxDocumentIDBytes {
		sum := sha256.Sum256(packedKey)
		id = "sha256-" + hex.EncodeToString(sum[:])
	}
	return id
}

// deltaDocumentID returns the ID of a delta update document.
//
// The root document has a unique UUID in its metadata, so together with the
// key it identifies the document. When a transaction is replayed after a
// failure, documents that were already stored are then replaced rather than
// stored twice. The root document is only available when it is selected,
// though, and the other fields may be the same for different documents, so
// documents are otherwise given a random ID, and may be stored twice by a
// replayed transaction.
func deltaDocumentID(packedKey []byte, doc json.RawMessage) string {
	if len(doc) == 0 {
		return uuid.NewString()
	}

	h := sha256.New()
	h.Write(binary.AppendUvarint(nil, uint64(len(packedKey))))
	h.Write(packedKey)
	h.Write(doc)
	return "d_" + hex.EncodeToString(h.Sum(nil))
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, range_ pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error { return nil }

func (t *transactor) Flush(context.Context, map[int]time.Time) error {
	return nil
}

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return nil, nil
}

func (t *transactor) Destroy() {}

type loadBatch struct {
	binding int
	ids     []string
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	// Canceling the context on return stops the workers if the stores or
	// loads end early with an error.
	ctx, cancel := context.WithCancel(it.Context())
	defer cancel()

	// We are evaluating loads as they come, so we must wait for the runtime's
	// ack of the previous commit. Store doesn't return until all of its tasks
	// have succeeded, so the documents of that commit are visible by then.
	it.WaitForAcknowledged()

	var mu sync.Mutex
	loadFn := func(b int, d json.RawMessage) error {
		mu.Lock()
		defer mu.Unlock()
		return loaded(b, d)
	}

	batchCh := make(chan loadBatch)
	group, groupCtx := errgroup.WithContext(ctx)
	for range loadWorkers {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case batch, ok := <-batchCh:
					if !ok {
						return nil
					} else if err := t.loadDocs(groupCtx, batch, loadFn); err != nil {
						return err
					}
				}
			}
		})
	}

	sendBatch := func(b loadBatch) error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case batchCh <- b:
			return nil
		}
	}

	pending := make([][]string, len(t.bindings))
	for it.Next() {
		ids := append(pending[it.Binding], documentID(it.PackedKey))
		if len(ids) >= loadBatchSize {
			if err := sendBatch(loadBatch{binding: it.Binding, ids: ids}); err != nil {
				return err
			}
			ids = nil
		}
		pending[it.Binding] = ids
	}
	if err := it.Err(); err != nil {
		close(batchCh)
		group.Wait()
		return err
	}

	for b, ids := range pending {
		if len(ids) > 0 {
			if err := sendBatch(loadBatch{binding: b, ids: ids}); err != nil {
				return err
			}
		}
	}

	close(batchCh)
	return group.Wait()
}

func (t *transactor) loadDocs(ctx context.Context, batch loadBatch, loaded func(int, json.RawMessage) error) error {
	b := t.bindings[batch.binding]
	idx := t.client.ms.Index(b.index)

	var res meilisearch.DocumentsResult
	if _, err := withRetry(ctx, "fetch documents", func() (struct{}, error) {
		return struct{}{}, idx.GetDocumentsWithContext(ctx, &meilisearch.DocumentsQuery{
			Ids:    batch.ids,
			Fields: []string{b.docField},
			Limit:  int64(len(batch.ids)),
		}, &res)
	}); err != nil {
		return fmt.Errorf("fetching documents from index %q: %w", b.index, err)
	}

	// Documents that don't exist are left out of the results.
	if len(res.Results) > len(batch.ids) {
		return fmt.Errorf("fetching documents from index %q: requested %d documents but got %d", b.index, len(batch.ids), len(res.Results))
	}

	for _, hit := range res.Results {
		doc, ok := hit[b.docField]
		if !ok {
			return fmt.Errorf("document loaded from index %q is missing the %q attribute", b.index, b.docField)
		}
		if err := loaded(batch.binding, json.RawMessage(doc)); err != nil {
			return err
		}
	}

	return nil
}

// storeBatch is a single request to send to Meilisearch: either an NDJSON
// payload of documents to add or replace, or the IDs of documents to delete.
type storeBatch struct {
	binding   int
	documents []byte
	deleteIDs []string
}

// pendingStores accumulates the stores of a binding until there are enough of
// them to send.
type pendingStores struct {
	documents []byte
	deleteIDs []string

	inserted, updated, deleted int64
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	// Canceling the context on return stops the workers if the stores or
	// loads end early with an error.
	ctx, cancel := context.WithCancel(it.Context())
	defer cancel()

	var mu sync.Mutex
	var taskUIDs []int64

	batchCh := make(chan storeBatch)
	group, groupCtx := errgroup.WithContext(ctx)
	for range storeWorkers {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case batch, ok := <-batchCh:
					if !ok {
						return nil
					}
					uid, err := t.sendStoreBatch(groupCtx, batch)
					if err != nil {
						return err
					}
					mu.Lock()
					taskUIDs = append(taskUIDs, uid)
					mu.Unlock()
				}
			}
		})
	}

	sendBatch := func(b storeBatch) error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case batchCh <- b:
			return nil
		}
	}

	pending := make([]pendingStores, len(t.bindings))
	// Skip deleted, non-existent documents iff HardDelete is enabled.
	for it.Next(t.cfg.HardDelete) {
		b := t.bindings[it.Binding]
		p := &pending[it.Binding]

		if it.Delete && t.cfg.HardDelete {
			// Deleted documents which don't exist were skipped by Next.
			p.deleteIDs = append(p.deleteIDs, documentID(it.PackedKey))
			p.deleted++
			if len(p.deleteIDs) >= deleteBatchSize {
				if err := sendBatch(storeBatch{binding: it.Binding, deleteIDs: p.deleteIDs}); err != nil {
					return nil, err
				}
				p.deleteIDs = nil
			}
			continue
		}

		var id string
		if b.deltaUpdates {
			// Every delta update is a separate document, so it can't be keyed
			// by the collection key alone.
			id = deltaDocumentID(it.PackedKey, it.RawJSON)
		} else {
			id = documentID(it.PackedKey)
		}

		var err error
		if p.documents, err = appendDocument(p.documents, b, id, it.Key, it.Values, it.RawJSON); err != nil {
			return nil, err
		}
		if it.Exists {
			p.updated++
		} else {
			p.inserted++
		}

		if len(p.documents) >= storeBatchBytes {
			if err := sendBatch(storeBatch{binding: it.Binding, documents: p.documents}); err != nil {
				return nil, err
			}
			p.documents = nil
		}
	}
	if err := it.Err(); err != nil {
		close(batchCh)
		group.Wait()
		return nil, err
	}

	for idx, p := range pending {
		if len(p.documents) > 0 {
			if err := sendBatch(storeBatch{binding: idx, documents: p.documents}); err != nil {
				return nil, err
			}
		}
		if len(p.deleteIDs) > 0 {
			if err := sendBatch(storeBatch{binding: idx, deleteIDs: p.deleteIDs}); err != nil {
				return nil, err
			}
		}
	}

	close(batchCh)
	if err := group.Wait(); err != nil {
		return nil, err
	}

	// Writes are processed asynchronously, and the transaction must not commit
	// until all of them have been applied: a write task can still fail, for
	// example because a document is invalid.
	if err := t.client.waitForTasks(ctx, taskUIDs); err != nil {
		return nil, fmt.Errorf("storing documents: %w", err)
	}

	for idx, p := range pending {
		if p.inserted+p.updated+p.deleted > 0 {
			t.be.ReportRowStats(it.Round, t.bindings[idx].path, m.ExactRowStats(p.inserted, p.updated, p.deleted))
		}
	}

	return nil, nil
}

// sendStoreBatch enqueues the task for a batch and returns its UID.
func (t *transactor) sendStoreBatch(ctx context.Context, batch storeBatch) (int64, error) {
	b := t.bindings[batch.binding]

	// A new index object is created for each request since the SDK records
	// the primary key option on it, which would race between workers.
	idx := t.client.ms.Index(b.index)

	var info *meilisearch.TaskInfo
	var err error
	if batch.deleteIDs != nil {
		info, err = withRetry(ctx, "delete documents", func() (*meilisearch.TaskInfo, error) {
			return idx.DeleteDocumentsWithContext(ctx, batch.deleteIDs, nil)
		})
	} else {
		// Setting the primary key makes sure that an index which was
		// unexpectedly deleted is recreated with the right primary key, rather
		// than one that Meilisearch infers from the documents.
		pk := primaryKeyAttribute
		info, err = withRetry(ctx, "add documents", func() (*meilisearch.TaskInfo, error) {
			return idx.AddDocumentsNdjsonWithContext(ctx, batch.documents, &meilisearch.DocumentOptions{PrimaryKey: &pk})
		})
	}
	if err != nil {
		if statusCode(err) == 413 {
			return 0, fmt.Errorf("writing to index %q: the request was larger than the Meilisearch payload size limit, which can be raised with the --http-payload-size-limit option: %w", b.index, err)
		}
		return 0, fmt.Errorf("writing to index %q: %w", b.index, err)
	}

	return info.TaskUID, nil
}

// appendDocument appends the JSON encoding of a document, followed by a
// newline, to buf.
func appendDocument(buf []byte, b binding, id string, key, values tuple.Tuple, rawJSON json.RawMessage) ([]byte, error) {
	buf = append(buf, `{"`+primaryKeyAttribute+`":"`...)
	// Document IDs never need escaping.
	buf = append(buf, id...)
	buf = append(buf, '"')

	idx := 0
	for _, tup := range [2]tuple.Tuple{key, values} {
		for _, v := range tup {
			buf = append(buf, ',')
			buf = append(buf, b.fieldPrefixes[idx]...)
			idx++

			switch vv := v.(type) {
			case []byte:
				// Objects and arrays are provided as raw JSON.
				buf = append(buf, vv...)
			case json.RawMessage:
				buf = append(buf, vv...)
			default:
				enc, err := json.Marshal(vv)
				if err != nil {
					return nil, fmt.Errorf("encoding value of attribute %s: %w", b.fieldPrefixes[idx-1], err)
				}
				buf = append(buf, enc...)
			}
		}
	}

	if b.docField != "" {
		buf = append(buf, ',')
		buf = append(buf, b.docFieldPrefix...)
		buf = append(buf, rawJSON...)
	}

	buf = append(buf, '}', '\n')
	return buf, nil
}
