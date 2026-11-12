package wayback

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	common "github.com/karust/gogetcrawl/common"
)

const testResponse = `[["urlkey","timestamp","original","mimetype","statuscode","digest","length"],
["ru,kamaloff)/", "20130522121421", "http://kamaloff.ru/", "text/html", "200", "FXOQP7LM7FWUC7S5MTDHZS2WMKNLCW2E", "2558"],
["ru,kamaloff)/favicon.ico", "20180104074528", "http://kamaloff.ru/favicon.ico", "text/html", "301", "WHT3EXKF6XVIKYVG67BXESI75TWESKWU", "463"]]`

// withIndexServer points INDEX_SERVER at a test server for the duration of the test.
func withIndexServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	old := INDEX_SERVER
	INDEX_SERVER = srv.URL
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

// fetchOutcome collects everything a FetchPages run produces.
type fetchOutcome struct {
	results []*common.CdxResponse
	errs    []error
}

// runFetch executes FetchPages and fails the test if it does not return in time.
func runFetch(t *testing.T, ctx context.Context, wb *Wayback, config common.RequestConfig) fetchOutcome {
	t.Helper()

	resultsChan := make(chan []*common.CdxResponse)
	errorsChan := make(chan error)
	done := make(chan struct{})

	go func() {
		defer close(done)
		wb.FetchPages(ctx, config, resultsChan, errorsChan)
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
		w.Write([]byte(testResponse))
	}))

	wb := &Wayback{MaxTimeout: 5, MaxRetries: 1}
	config := common.RequestConfig{URL: "kamaloff.ru/*", SinglePage: true}

	out := runFetch(t, context.Background(), wb, config)
	if len(out.errs) != 0 {
		t.Fatalf("Unexpected errors: %v", out.errs)
	}
	if len(out.results) != 2 {
		t.Fatalf("Expected 2 results, got %v", len(out.results))
	}
	if out.results[0].Original != "http://kamaloff.ru/" {
		t.Fatalf("Unexpected first result: %v", out.results[0].Original)
	}
	if out.results[0].Source.Name() != "Wayback" {
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
	INDEX_SERVER = srv.URL
	defer func() { INDEX_SERVER = old; srv.Close() }()

	wb := &Wayback{MaxTimeout: 5, MaxRetries: 3}
	config := common.RequestConfig{URL: "kamaloff.ru/*", SinglePage: true}

	out := runFetch(t, context.Background(), wb, config)
	if len(out.results) != 0 {
		t.Fatalf("Rate-limited body must not be parsed, got %v results", len(out.results))
	}
	if len(out.errs) != 1 {
		t.Fatalf("Expected exactly 1 error, got %v: %v", len(out.errs), out.errs)
	}
	if !errors.Is(out.errs[0], common.Status429Error) {
		t.Fatalf("Final error must keep the 429 category, got: %v", out.errs[0])
	}
	if attempts != 3 {
		t.Fatalf("Expected 3 attempts, got %v", attempts)
	}
}

func TestFetchPagesBadJSON(t *testing.T) {
	withIndexServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[["urlkey","timestamp"],[broken`))
	}))

	wb := &Wayback{MaxTimeout: 5, MaxRetries: 1}
	config := common.RequestConfig{URL: "kamaloff.ru/*", SinglePage: true}

	out := runFetch(t, context.Background(), wb, config)
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

	wb := &Wayback{MaxTimeout: 5, MaxRetries: 1}
	config := common.RequestConfig{URL: "kamaloff.ru/*", SinglePage: true}

	out := runFetch(t, context.Background(), wb, config)
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
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		body := `[["urlkey","timestamp","original","mimetype","statuscode","digest","length"]`
		for i := 0; i < limit; i++ {
			body += fmt.Sprintf(`,["k%v","20200101000000","http://example.com/%v","text/html","200","D","1"]`, i, i)
		}
		body += `]`
		w.Write([]byte(body))
	}))
	wb := &Wayback{MaxTimeout: 5, MaxRetries: 1}
	config := common.RequestConfig{URL: "example.com/*", Limit: 3, SinglePage: true}

	out := runFetch(t, context.Background(), wb, config)
	if len(out.errs) != 0 {
		t.Fatalf("Unexpected errors: %v", out.errs)
	}
	if len(out.results) != 3 {
		t.Fatalf("Expected limit to be honored with 3 results, got %v", len(out.results))
	}
}

func TestFetchPagesPagination(t *testing.T) {
	withIndexServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("showNumPages") == "true" {
			w.Write([]byte("2\n"))
			return
		}
		page := r.URL.Query().Get("page")
		w.Write([]byte(`[["urlkey","timestamp","original","mimetype","statuscode","digest","length"],
["k", "20200101000000", "http://example.com/p` + page + `", "text/html", "200", "D", "1"]]`))
	}))

	wb := &Wayback{MaxTimeout: 5, MaxRetries: 1}
	config := common.RequestConfig{URL: "example.com/*"}

	out := runFetch(t, context.Background(), wb, config)
	if len(out.errs) != 0 {
		t.Fatalf("Unexpected errors: %v", out.errs)
	}
	if len(out.results) != 2 {
		t.Fatalf("Expected 2 paginated results, got %v", len(out.results))
	}
}

func TestFetchPagesCancel(t *testing.T) {
	withIndexServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // block until the client goes away
	}))

	wb := &Wayback{MaxTimeout: 30, MaxRetries: 5}
	config := common.RequestConfig{URL: "example.com/*", SinglePage: true}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	out := runFetch(t, ctx, wb, config)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("FetchPages ignored cancellation for %v", elapsed)
	}
	if len(out.results) != 0 {
		t.Fatalf("Expected no results after cancel, got %v", len(out.results))
	}
}

func TestFetchPagesCancelWhileBlockedOnSend(t *testing.T) {
	withIndexServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testResponse))
	}))

	wb := &Wayback{MaxTimeout: 5, MaxRetries: 1}
	config := common.RequestConfig{URL: "kamaloff.ru/*", SinglePage: true}

	ctx, cancel := context.WithCancel(context.Background())
	resultsChan := make(chan []*common.CdxResponse) // never read: send blocks
	errorsChan := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		// Cancel as soon as the fetch had time to produce a batch.
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		wb.FetchPages(ctx, config, resultsChan, errorsChan)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("FetchPages leaked a goroutine blocked on a results send after cancel")
	}
}
