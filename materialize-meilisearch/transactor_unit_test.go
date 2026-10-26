package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/meilisearch/meilisearch-go"
	"github.com/stretchr/testify/require"
)

func TestDocumentID(t *testing.T) {
	short := tuple.Tuple{"a/b+c", int64(42)}.Pack()
	id := documentID(short)
	for _, r := range id {
		require.True(t, isIdentifierRune(r), "invalid character %q in %q", r, id)
	}
	require.Equal(t, id, documentID(short), "IDs must be deterministic")
	require.NotEqual(t, id, documentID(tuple.Tuple{"a/b+c", int64(43)}.Pack()))

	long := tuple.Tuple{strings.Repeat("x", 1000)}.Pack()
	longID := documentID(long)
	require.LessOrEqual(t, len(longID), maxDocumentIDBytes)
	require.True(t, strings.HasPrefix(longID, "sha256-"))
	require.NotEqual(t, longID, documentID(tuple.Tuple{strings.Repeat("y", 1000)}.Pack()))
}

func TestAppendDocument(t *testing.T) {
	b := newBinding([]string{"idx"}, false, []string{"id", "name", "obj", "quote\"d"}, "flow_document")

	buf, err := appendDocument(nil, b, "abc",
		tuple.Tuple{int64(1)},
		tuple.Tuple{"<hello>", []byte(`{"a":[1,2]}`), nil},
		json.RawMessage(`{"id":1}`),
	)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(string(buf), "\n"))

	var got map[string]any
	require.NoError(t, json.Unmarshal(buf, &got))
	require.Equal(t, map[string]any{
		"flow_id":       "abc",
		"id":            float64(1),
		"name":          "<hello>",
		"obj":           map[string]any{"a": []any{float64(1), float64(2)}},
		"quote\"d":      nil,
		"flow_document": map[string]any{"id": float64(1)},
	}, got)

	// Documents are appended to the same buffer, one per line.
	buf, err = appendDocument(buf, newBinding([]string{"idx"}, true, []string{"id"}, ""), "def", tuple.Tuple{int64(2)}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(string(buf), "\n"))
}

// fakeTasks serves the tasks endpoint of a Meilisearch instance. Each task
// reports as processing for a number of polls before reaching its final
// status.
type fakeTasks struct {
	mu        sync.Mutex
	remaining map[int64]int
	final     map[int64]string
	polls     int
}

func (f *fakeTasks) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls++

	var results []map[string]any
	for _, s := range strings.Split(r.URL.Query().Get("uids"), ",") {
		uid, _ := strconv.ParseInt(s, 10, 64)
		final, ok := f.final[uid]
		if !ok {
			continue
		}
		status := final
		if f.remaining[uid] > 0 {
			f.remaining[uid]--
			status = "processing"
		}
		task := map[string]any{"uid": uid, "indexUid": "idx", "status": status, "type": "documentAdditionOrUpdate"}
		if status == "failed" {
			task["error"] = map[string]any{"message": "bad document", "code": "invalid_document_id"}
		}
		results = append(results, task)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"results": results})
}

func testClient(t *testing.T, h http.Handler) *client {
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := config{Endpoint: srv.URL + "/"}.toClient()
	require.NoError(t, err)
	return c
}

func TestWaitForTasks(t *testing.T) {
	ctx := context.Background()

	t.Run("succeeded", func(t *testing.T) {
		f := &fakeTasks{remaining: map[int64]int{}, final: map[int64]string{}}
		var uids []int64
		// More tasks than are polled in a single request.
		for uid := int64(0); uid < 250; uid++ {
			uids = append(uids, uid)
			f.final[uid] = "succeeded"
			f.remaining[uid] = int(uid % 3)
		}
		require.NoError(t, testClient(t, f).waitForTasks(ctx, uids))
		// Only tasks which are still pending are polled again: 250, then 166,
		// then 83 tasks, in chunks of 100.
		require.Equal(t, 3+2+1, f.polls)
	})

	t.Run("failed", func(t *testing.T) {
		f := &fakeTasks{
			remaining: map[int64]int{1: 0, 2: 1},
			final:     map[int64]string{1: "succeeded", 2: "failed"},
		}
		err := testClient(t, f).waitForTasks(ctx, []int64{1, 2})
		require.EqualError(t, err, `documentAdditionOrUpdate task 2 for index "idx" failed: bad document (invalid_document_id)`)
	})

	t.Run("canceled", func(t *testing.T) {
		f := &fakeTasks{remaining: map[int64]int{}, final: map[int64]string{7: "canceled"}}
		err := testClient(t, f).waitForTasks(ctx, []int64{7})
		require.EqualError(t, err, `documentAdditionOrUpdate task 7 for index "idx" was canceled`)
	})

	t.Run("not found", func(t *testing.T) {
		f := &fakeTasks{remaining: map[int64]int{}, final: map[int64]string{}}
		err := testClient(t, f).waitForTasks(ctx, []int64{9})
		require.ErrorContains(t, err, "task 9 was not found")
	})

	t.Run("no tasks", func(t *testing.T) {
		require.NoError(t, (&client{}).waitForTasks(ctx, nil))
	})
}

func TestRetries(t *testing.T) {
	ctx := context.Background()

	t.Run("retryable status", func(t *testing.T) {
		var calls int
		f := &fakeTasks{remaining: map[int64]int{}, final: map[int64]string{1: "succeeded"}}
		c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls++; calls == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			f.ServeHTTP(w, r)
		}))
		require.NoError(t, c.waitForTasks(ctx, []int64{1}))
		require.Equal(t, 2, calls)
	})

	t.Run("dropped connection", func(t *testing.T) {
		var calls int
		f := &fakeTasks{remaining: map[int64]int{}, final: map[int64]string{1: "succeeded"}}
		c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls++; calls == 1 {
				conn, _, err := w.(http.Hijacker).Hijack()
				require.NoError(t, err)
				conn.Close()
				return
			}
			f.ServeHTTP(w, r)
		}))
		require.NoError(t, c.waitForTasks(ctx, []int64{1}))
		require.Equal(t, 2, calls)
	})

	t.Run("api error is not retried", func(t *testing.T) {
		var calls int
		c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"message":"The provided API key is invalid.","code":"invalid_api_key","type":"auth","link":""}`)
		}))
		err := c.waitForTasks(ctx, []int64{1})
		require.Error(t, err)
		require.Equal(t, "invalid_api_key", apiErrorCode(err))
		require.Equal(t, 1, calls)
	})
}

func TestDeltaDocumentID(t *testing.T) {
	key := tuple.Tuple{"a"}.Pack()
	id := deltaDocumentID(key, json.RawMessage(`{"_meta":{"uuid":"1"}}`))
	for _, r := range id {
		require.True(t, isIdentifierRune(r), "invalid character %q in %q", r, id)
	}
	require.LessOrEqual(t, len(id), maxDocumentIDBytes)
	// A replayed document has the same ID.
	require.Equal(t, id, deltaDocumentID(key, json.RawMessage(`{"_meta":{"uuid":"1"}}`)))
	require.NotEqual(t, id, deltaDocumentID(key, json.RawMessage(`{"_meta":{"uuid":"2"}}`)))
	require.NotEqual(t, id, deltaDocumentID(tuple.Tuple{"b"}.Pack(), json.RawMessage(`{"_meta":{"uuid":"1"}}`)))

	// Without the root document, documents with the same key get different
	// IDs rather than replacing each other.
	require.NotEqual(t, deltaDocumentID(key, nil), deltaDocumentID(key, nil))
}

func TestRedirectingTransport(t *testing.T) {
	var gotHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
	}))
	t.Cleanup(srv.Close)

	// Requests for the remote host are sent to the local address, keeping the
	// remote host in the request.
	httpClient := &http.Client{Transport: redirectingTransport("meili.example.com:7700", srv.Listener.Addr().String())}
	res, err := httpClient.Get("http://meili.example.com:7700/health")
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, "meili.example.com:7700", gotHost)
}

func TestIsTransient(t *testing.T) {
	require.True(t, isTransient(&meilisearch.Error{ErrCode: meilisearch.MeilisearchCommunicationError}))
	require.True(t, isTransient(&meilisearch.Error{ErrCode: meilisearch.MeilisearchMaxRetriesExceeded}))
	require.False(t, isTransient(&meilisearch.Error{ErrCode: meilisearch.MeilisearchApiError}))
	require.False(t, isTransient(fmt.Errorf("other")))

	require.True(t, isUnreachable(&meilisearch.Error{ErrCode: meilisearch.MeilisearchTimeoutError}))
	require.True(t, isUnreachable(fmt.Errorf("wrapped: %w", context.DeadlineExceeded)))
	require.False(t, isUnreachable(&meilisearch.Error{ErrCode: meilisearch.MeilisearchApiError}))
}
