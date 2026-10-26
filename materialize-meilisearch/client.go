package connector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/meilisearch/meilisearch-go"
	log "github.com/sirupsen/logrus"
)

const (
	// The SDK retries requests that fail with one of these statuses, waiting
	// one second longer before each attempt.
	requestMaxRetries = 10

	// Requests that fail before receiving a response, for example because the
	// connection was reset, are retried by the connector rather than the SDK.
	transportMaxAttempts = 5

	// Task statuses are polled in groups of this many task UIDs, which keeps
	// the request URL reasonably short.
	taskPollBatchSize = 100
	minTaskPollDelay  = 50 * time.Millisecond
	maxTaskPollDelay  = 2 * time.Second
	taskWaitLogEvery  = time.Minute

	listIndexesPageSize = 1000
)

var retryOnStatus = []int{429, 502, 503, 504}

type client struct {
	ms meilisearch.ServiceManager
	// The SSH tunnel requests are sent through, if one is configured.
	tunnel *networkTunnel.SshTunnel
}

// toClient initializes a client for connecting to Meilisearch, starting the
// network tunnel if configured.
func (c config) toClient() (*client, error) {
	var out client
	opts := []meilisearch.Option{
		meilisearch.WithAPIKey(c.APIKey),
		meilisearch.WithCustomRetries(retryOnStatus, requestMaxRetries),
	}

	// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
	if c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("parsing endpoint URL: %w", err)
		}

		forwardPort := u.Port()
		if forwardPort == "" && u.Scheme == "https" {
			forwardPort = "443"
		} else if forwardPort == "" {
			forwardPort = "80"
		}

		localPort, err := freeLocalPort()
		if err != nil {
			return nil, fmt.Errorf("finding a local port for the network tunnel: %w", err)
		}

		var sshConfig = &networkTunnel.SshConfig{
			SshEndpoint: c.NetworkTunnel.SshForwarding.SshEndpoint,
			PrivateKey:  []byte(c.NetworkTunnel.SshForwarding.PrivateKey),
			ForwardHost: u.Hostname(),
			ForwardPort: forwardPort,
			LocalPort:   localPort,
		}
		out.tunnel = sshConfig.CreateTunnel()

		if err := out.tunnel.Start(); err != nil {
			return nil, err
		}

		// Requests keep the endpoint URL, so that the Host header and the TLS
		// server name are those of the endpoint, which a proxy in front of
		// Meilisearch may route on. Only the connection is made to the local
		// end of the tunnel.
		opts = append(opts, meilisearch.WithCustomClient(&http.Client{Transport: redirectingTransport(
			net.JoinHostPort(u.Hostname(), forwardPort),
			net.JoinHostPort("127.0.0.1", localPort),
		)}))
	}

	// The SDK appends API paths directly to the host, so a trailing slash
	// would produce paths like "//indexes".
	out.ms = meilisearch.New(strings.TrimSuffix(c.Endpoint, "/"), opts...)
	return &out, nil
}

// redirectingTransport returns an HTTP transport which connects to localAddr
// instead of remoteAddr, without changing the requests themselves.
func redirectingTransport(remoteAddr, localAddr string) *http.Transport {
	var dialer net.Dialer
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr == remoteAddr {
			addr = localAddr
		}
		return dialer.DialContext(ctx, network, addr)
	}
	return transport
}

// freeLocalPort returns a local TCP port which is not in use.
func freeLocalPort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port), nil
}

func (c *client) close() {
	c.ms.Close()
	c.tunnel.Stop()
}

// apiErrorCode returns the Meilisearch error code of a failed request, for
// example "index_not_found", or an empty string if the error did not come from
// the Meilisearch API.
func apiErrorCode(err error) string {
	var me *meilisearch.Error
	if errors.As(err, &me) {
		return me.MeilisearchApiError.Code
	}
	return ""
}

// statusCode returns the HTTP status code of a failed request, or 0 if no
// response was received.
func statusCode(err error) int {
	var me *meilisearch.Error
	if errors.As(err, &me) {
		return me.StatusCode
	}
	return 0
}

// isTransient reports whether err is a failure to communicate with
// Meilisearch, where the request may not have reached the server.
//
// Requests which exhausted the retries of the SDK are retried as well: the SDK
// (as of v0.36.2) reports that error even when the last of its retries
// succeeded.
func isTransient(err error) bool {
	var me *meilisearch.Error
	return errors.As(err, &me) &&
		(me.ErrCode == meilisearch.MeilisearchCommunicationError || me.ErrCode == meilisearch.MeilisearchMaxRetriesExceeded)
}

// isUnreachable reports whether err means that Meilisearch could not be
// reached, or did not respond in time.
func isUnreachable(err error) bool {
	var me *meilisearch.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &me) &&
		(me.ErrCode == meilisearch.MeilisearchCommunicationError || me.ErrCode == meilisearch.MeilisearchTimeoutError))
}

// withRetry calls fn until it succeeds, fails with a non-transient error, or
// has been attempted transportMaxAttempts times.
//
// Retrying an enqueued write is safe: documents are keyed by their collection
// key, so a write that landed twice has the same result as a single write.
func withRetry[T any](ctx context.Context, desc string, fn func() (T, error)) (T, error) {
	for attempt := 1; ; attempt++ {
		res, err := fn()
		if err == nil || !isTransient(err) || attempt >= transportMaxAttempts {
			return res, err
		}

		d := min(time.Duration(1<<(attempt-1))*time.Second, 10*time.Second)
		log.WithFields(log.Fields{
			"request": desc,
			"attempt": attempt,
			"delay":   d.String(),
			"error":   err.Error(),
		}).Warn("retrying request after transient error")

		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(d):
		}
	}
}

// taskError is a Meilisearch task which did not succeed. Write requests are
// processed asynchronously, so errors with the request's content (an invalid
// document, for example) are only reported through the task.
type taskError struct {
	task meilisearch.Task
}

func (e *taskError) Error() string {
	if e.task.Status == meilisearch.TaskStatusCanceled {
		return fmt.Sprintf("%s task %d for index %q was canceled", e.task.Type, e.task.UID, e.task.IndexUID)
	}
	return fmt.Sprintf("%s task %d for index %q failed: %s (%s)", e.task.Type, e.task.UID, e.task.IndexUID, e.task.Error.Message, e.task.Error.Code)
}

func (e *taskError) code() string {
	return e.task.Error.Code
}

// waitForTasks waits for every task to finish processing, and returns an error
// if any of them failed or was canceled.
//
// Rather than waiting for each task in turn, all pending tasks are polled
// together, starting with a short delay that grows while tasks remain.
func (c *client) waitForTasks(ctx context.Context, uids []int64) error {
	pending := slices.Clone(uids)
	delay := minTaskPollDelay
	start := time.Now()
	lastLog := start

	for len(pending) > 0 {
		var stillPending []int64
		for chunk := range slices.Chunk(pending, taskPollBatchSize) {
			res, err := withRetry(ctx, "get tasks", func() (*meilisearch.TaskResult, error) {
				return c.ms.GetTasksWithContext(ctx, &meilisearch.TasksQuery{
					UIDS:  chunk,
					Limit: int64(len(chunk)),
				})
			})
			if err != nil {
				return fmt.Errorf("getting task statuses: %w", err)
			}

			found := make(map[int64]bool, len(chunk))
			for _, task := range res.Results {
				found[task.UID] = true
				switch task.Status {
				case meilisearch.TaskStatusSucceeded:
				case meilisearch.TaskStatusFailed, meilisearch.TaskStatusCanceled:
					return &taskError{task: task}
				default:
					stillPending = append(stillPending, task.UID)
				}
			}

			for _, uid := range chunk {
				if !found[uid] {
					// Meilisearch deletes the oldest finished tasks once its task
					// queue is full, so a task that isn't found can't be assumed
					// to have succeeded.
					return fmt.Errorf("task %d was not found: it may have been deleted before its status could be read, which happens when the Meilisearch task queue is full and can be avoided by deleting finished tasks", uid)
				}
			}
		}

		pending = stillPending
		if len(pending) == 0 {
			break
		}

		if time.Since(lastLog) >= taskWaitLogEvery {
			log.WithFields(log.Fields{
				"pendingTasks": len(pending),
				"elapsed":      time.Since(start).Truncate(time.Second).String(),
			}).Info("waiting for Meilisearch to process tasks")
			lastLog = time.Now()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, maxTaskPollDelay)
	}

	return nil
}

// enqueue sends a request that enqueues a task and waits for the task to
// finish.
func (c *client) enqueue(ctx context.Context, desc string, fn func() (*meilisearch.TaskInfo, error)) error {
	info, err := withRetry(ctx, desc, fn)
	if err != nil {
		return fmt.Errorf("%s: %w", desc, err)
	}
	if err := c.waitForTasks(ctx, []int64{info.TaskUID}); err != nil {
		return fmt.Errorf("%s: %w", desc, err)
	}
	return nil
}

func (c *client) listIndexes(ctx context.Context) ([]*meilisearch.IndexResult, error) {
	var out []*meilisearch.IndexResult
	for offset := int64(0); ; offset += listIndexesPageSize {
		res, err := withRetry(ctx, "list indexes", func() (*meilisearch.IndexesResults, error) {
			return c.ms.ListIndexesWithContext(ctx, &meilisearch.IndexesQuery{Limit: listIndexesPageSize, Offset: offset})
		})
		if err != nil {
			return nil, fmt.Errorf("listing indexes: %w", err)
		}

		out = append(out, res.Results...)
		if len(res.Results) < listIndexesPageSize || int64(len(out)) >= res.Total {
			return out, nil
		}
	}
}

// getIndex returns the index with the given UID, or nil if it doesn't exist.
func (c *client) getIndex(ctx context.Context, uid string) (*meilisearch.IndexResult, error) {
	res, err := withRetry(ctx, "get index", func() (*meilisearch.IndexResult, error) {
		return c.ms.GetIndexWithContext(ctx, uid)
	})
	if apiErrorCode(err) == "index_not_found" {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("getting index %q: %w", uid, err)
	}
	return res, nil
}

// createIndex creates an index which uses the connector's document ID
// attribute as its primary key. An index which already exists is used as long
// as its primary key is compatible.
func (c *client) createIndex(ctx context.Context, uid string) error {
	err := c.enqueue(ctx, fmt.Sprintf("creating index %q", uid), func() (*meilisearch.TaskInfo, error) {
		return c.ms.CreateIndexWithContext(ctx, &meilisearch.IndexConfig{Uid: uid, PrimaryKey: primaryKeyAttribute})
	})

	var te *taskError
	if !errors.As(err, &te) || te.code() != "index_already_exists" {
		return err
	}

	existing, err := c.getIndex(ctx, uid)
	if err != nil {
		return err
	} else if existing == nil {
		return fmt.Errorf("index %q was deleted while it was being created", uid)
	}

	switch existing.PrimaryKey {
	case primaryKeyAttribute:
		return nil
	case "":
		// The primary key of an index is set when the first documents are
		// added if it was not set explicitly, so an index without one can't
		// have any documents yet.
		return c.enqueue(ctx, fmt.Sprintf("setting primary key of index %q", uid), func() (*meilisearch.TaskInfo, error) {
			return c.ms.Index(uid).UpdateIndexWithContext(ctx, &meilisearch.UpdateIndexRequestParams{PrimaryKey: primaryKeyAttribute})
		})
	default:
		return fmt.Errorf("index %q already exists with primary key %q: the connector requires the primary key to be %q, so the index must be deleted or a different index name must be used", uid, existing.PrimaryKey, primaryKeyAttribute)
	}
}

func (c *client) deleteIndex(ctx context.Context, uid string) error {
	err := c.enqueue(ctx, fmt.Sprintf("deleting index %q", uid), func() (*meilisearch.TaskInfo, error) {
		return c.ms.DeleteIndexWithContext(ctx, uid)
	})

	var te *taskError
	if errors.As(err, &te) && te.code() == "index_not_found" {
		return nil
	}
	return err
}

func (c *client) deleteAllDocuments(ctx context.Context, uid string) error {
	return c.enqueue(ctx, fmt.Sprintf("deleting all documents from index %q", uid), func() (*meilisearch.TaskInfo, error) {
		return c.ms.Index(uid).DeleteAllDocumentsWithContext(ctx, nil)
	})
}

// indexSettings are the settings of an index that the connector manages.
type indexSettings struct {
	searchable []string
	// Filterable attributes may be configured as plain attribute names or as
	// objects with attribute patterns and features, which are kept as-is.
	filterable []any
	sortable   []string
}

func (c *client) getSettings(ctx context.Context, uid string) (indexSettings, error) {
	idx := c.ms.Index(uid)
	var out indexSettings

	searchable, err := withRetry(ctx, "get searchable attributes", func() (*[]string, error) {
		return idx.GetSearchableAttributesWithContext(ctx)
	})
	if err != nil {
		return out, fmt.Errorf("getting searchable attributes of index %q: %w", uid, err)
	}
	filterable, err := withRetry(ctx, "get filterable attributes", func() (*[]any, error) {
		return idx.GetFilterableAttributesWithContext(ctx)
	})
	if err != nil {
		return out, fmt.Errorf("getting filterable attributes of index %q: %w", uid, err)
	}
	sortable, err := withRetry(ctx, "get sortable attributes", func() (*[]string, error) {
		return idx.GetSortableAttributesWithContext(ctx)
	})
	if err != nil {
		return out, fmt.Errorf("getting sortable attributes of index %q: %w", uid, err)
	}

	if searchable != nil {
		out.searchable = *searchable
	}
	if filterable != nil {
		out.filterable = *filterable
	}
	if sortable != nil {
		out.sortable = *sortable
	}
	return out, nil
}

// updateSettings applies the given settings to an index. Each kind of setting
// is only sent if it is set, so settings that don't change are left alone.
func (c *client) updateSettings(ctx context.Context, uid string, s indexSettings) error {
	idx := c.ms.Index(uid)
	var updates []func() (*meilisearch.TaskInfo, error)

	if s.searchable != nil {
		updates = append(updates, func() (*meilisearch.TaskInfo, error) {
			return idx.UpdateSearchableAttributesWithContext(ctx, &s.searchable)
		})
	}
	if s.filterable != nil {
		updates = append(updates, func() (*meilisearch.TaskInfo, error) {
			return idx.UpdateFilterableAttributesWithContext(ctx, &s.filterable)
		})
	}
	if s.sortable != nil {
		updates = append(updates, func() (*meilisearch.TaskInfo, error) {
			return idx.UpdateSortableAttributesWithContext(ctx, &s.sortable)
		})
	}

	// Settings tasks are enqueued together and then waited for together,
	// since Meilisearch processes tasks for the same index in order anyway.
	var uids []int64
	for _, update := range updates {
		info, err := withRetry(ctx, "update settings", update)
		if err != nil {
			return fmt.Errorf("updating settings of index %q: %w", uid, err)
		}
		uids = append(uids, info.TaskUID)
	}

	if err := c.waitForTasks(ctx, uids); err != nil {
		return fmt.Errorf("updating settings of index %q: %w", uid, err)
	}
	return nil
}

// existingAttributes returns the attributes of an index: those that its
// documents have, and those named by its settings.
func (c *client) existingAttributes(ctx context.Context, uid string) ([]string, error) {
	stats, err := withRetry(ctx, "get index stats", func() (*meilisearch.StatsIndex, error) {
		return c.ms.Index(uid).GetStatsWithContext(ctx)
	})
	if err != nil {
		return nil, fmt.Errorf("getting stats of index %q: %w", uid, err)
	}
	settings, err := c.getSettings(ctx, uid)
	if err != nil {
		return nil, err
	}

	var out []string
	add := func(attr string) {
		if attr != "*" && !slices.Contains(out, attr) {
			out = append(out, attr)
		}
	}
	for attr := range stats.FieldDistribution {
		add(attr)
	}
	for _, attr := range settings.searchable {
		add(attr)
	}
	for _, attr := range settings.filterable {
		if s, ok := attr.(string); ok {
			add(s)
		}
	}
	for _, attr := range settings.sortable {
		add(attr)
	}
	slices.Sort(out)
	return out, nil
}
