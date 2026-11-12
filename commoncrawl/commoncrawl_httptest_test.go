package commoncrawl

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	common "github.com/karust/gogetcrawl/common"
)

const testNDJSON = `{"urlkey": "com,example)/", "timestamp": "20230320100841", "url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "ABC", "length": "100", "offset": "10", "filename": "crawl-data/CC-MAIN-2024-01/segments/x.warc.gz"}
{"urlkey": "com,example)/a", "timestamp": "20230320100842", "url": "http://example.com/a", "mime": "text/html", "status": "200", "digest": "ABD", "length": "101", "offset": "11", "filename": "crawl-data/CC-MAIN-2024-01/segments/x.warc.gz"}
`

func testSource() *CommonCrawl {
	return &CommonCrawl{
		MaxTimeout: 5,
		MaxRetries: 2,
		indexes:    []latestIndex{{Id: "CC-MAIN-2024-01"}},
	}
}

// withIndexServer points INDEX_SERVER at a test server for the duration of the test.
func withIndexServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	old := INDEX_SERVER
	INDEX_SERVER = srv.URL + "/"
	t.Cleanup(func() {
		INDEX_SERVER = old
		srv.Close()
	})
	return srv
}

func shrinkRetries(t *testing.T) {
	t.Helper()
	old := common.RetryInterval
	common.RetryInterval = time.Millisecond
	t.Cleanup(func() { common.RetryInterval = old })
}

type fetchOutcome struct {
	results []*common.CdxResponse
	errs    []error
}

// runFetch executes FetchPages and fails the test if it does not return in time.
func runFetch(t *testing.T, ctx context.Context, cc *CommonCrawl, config common.RequestConfig) fetchOutcome {
	t.Helper()

	resultsChan := make(chan []*common.CdxResponse)
	errorsChan := make(chan error)
	done := make(chan struct{})

	go func() {
		defer close(done)
		cc.FetchPages(ctx, config, resultsChan, errorsChan)
	}()

	var out fetchOutcome
	for {
		select {
		case batch := <-resultsChan:
			out.results = append(out.results, batch...)
		case err := <-errorsChan:
			out.errs = append(out.errs, err)
		case <-done:
			return out
		case <-time.After(15 * time.Second):
			t.Fatal("FetchPages did not return in time (goroutine stuck)")
		}
	}
}

func TestFetchPagesOK(t *testing.T) {
	withIndexServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("showNumPages") == "true" {
			w.Write([]byte(`{"pages": 1, "pageSize": 50, "blocks": 1}`))
			return
		}
		w.Write([]byte(testNDJSON))
	}))

	cc := testSource()
	config := common.RequestConfig{URL: "example.com/*"}

	out := runFetch(t, context.Background(), cc, config)
	if len(out.errs) != 0 {
		t.Fatalf("Unexpected errors: %v", out.errs)
	}
	if len(out.results) != 2 {
		t.Fatalf("Expected 2 results, got %v", len(out.results))
	}
	if out.results[0].Original != "http://example.com/" {
		t.Fatalf("Unexpected first result: %v", out.results[0].Original)
	}
	if out.results[0].Source.Name() != "CommonCrawl" {
		t.Fatalf("Source not set on result: %v", out.results[0].Source)
	}
}

func TestFetchPagesRateLimited(t *testing.T) {
	shrinkRetries(t)

	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	old := INDEX_SERVER
	INDEX_SERVER = srv.URL + "/"
	defer func() { INDEX_SERVER = old; srv.Close() }()

	cc := testSource()
	config := common.RequestConfig{URL: "example.com/*", SinglePage: true}

	out := runFetch(t, context.Background(), cc, config)
	if len(out.results) != 0 {
		t.Fatalf("Rate-limited body must not be parsed, got %v results", len(out.results))
	}
	if len(out.errs) != 1 {
		t.Fatalf("Expected exactly 1 error, got %v: %v", len(out.errs), out.errs)
	}
	if !errors.Is(out.errs[0], common.Status429Error) {
		t.Fatalf("Final error must keep the 429 category, got: %v", out.errs[0])
	}
	if attempts != 2 { // MaxRetries = 2
		t.Fatalf("Expected 2 attempts, got %v", attempts)
	}
}

func TestFetchPagesBadJSON(t *testing.T) {
	withIndexServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"urlkey": broken`))
	}))

	cc := testSource()
	config := common.RequestConfig{URL: "example.com/*", SinglePage: true}

	out := runFetch(t, context.Background(), cc, config)
	if len(out.results) != 0 {
		t.Fatalf("Bad JSON must not produce results, got %v", len(out.results))
	}
	if len(out.errs) != 1 {
		t.Fatalf("Expected exactly 1 parse error, got %v: %v", len(out.errs), out.errs)
	}
}

func TestFetchPagesEmptyResponse(t *testing.T) {
	withIndexServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // empty body: no results
	}))

	cc := testSource()
	config := common.RequestConfig{URL: "example.com/*", SinglePage: true}

	out := runFetch(t, context.Background(), cc, config)
	if len(out.results) != 0 {
		t.Fatalf("Expected no results, got %v", len(out.results))
	}
	if len(out.errs) != 0 {
		t.Fatalf("Empty response must not produce errors, got %v", out.errs)
	}
}

func TestFetchPagesLimit(t *testing.T) {
	// Emulate the CDX server honoring the limit parameter.
	withIndexServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var limit int
		fmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
		body := ""
		for i := 0; i < limit; i++ {
			body += fmt.Sprintf(`{"urlkey": "com,example)/%d", "timestamp": "20230320100841", "url": "http://example.com/%d", "mime": "text/html", "status": "200", "digest": "ABC", "length": "100", "offset": "10", "filename": "f.warc.gz"}`, i, i) + "\n"
		}
		w.Write([]byte(body))
	}))

	cc := testSource()
	config := common.RequestConfig{URL: "example.com/*", Limit: 3, SinglePage: true}

	out := runFetch(t, context.Background(), cc, config)
	if len(out.errs) != 0 {
		t.Fatalf("Unexpected errors: %v", out.errs)
	}
	if len(out.results) != 3 {
		t.Fatalf("Expected limit to be honored with 3 results, got %v", len(out.results))
	}
}

func TestFetchPagesNumPagesFailureSkipsIndex(t *testing.T) {
	withIndexServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("showNumPages") == "true" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(testNDJSON))
	}))

	shrinkRetries(t)
	cc := testSource()
	config := common.RequestConfig{URL: "example.com/*"} // not SinglePage: GetNumPagesIndex fails

	out := runFetch(t, context.Background(), cc, config)
	if len(out.results) != 0 {
		t.Fatalf("Expected no results, got %v", len(out.results))
	}
	if len(out.errs) != 1 {
		t.Fatalf("Expected exactly 1 error, got %v: %v", len(out.errs), out.errs)
	}
	if !errors.Is(out.errs[0], common.Status500Error) {
		t.Fatalf("Final error must keep the 500 category, got: %v", out.errs[0])
	}
}

func TestFetchPagesCancel(t *testing.T) {
	withIndexServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // block until the client goes away
	}))

	cc := testSource()
	cc.MaxTimeout = 30
	config := common.RequestConfig{URL: "example.com/*", SinglePage: true}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	out := runFetch(t, ctx, cc, config)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("FetchPages ignored cancellation for %v", elapsed)
	}
	if len(out.results) != 0 {
		t.Fatalf("Expected no results after cancel, got %v", len(out.results))
	}
}
