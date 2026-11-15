package wayback

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	common "github.com/karust/gogetcrawl/common"
)

// Example request: https://web.archive.org/cdx/search/cdx?url=kamaloff.ru/*&output=json&limit=100&collapse=urlkey
const RESPONSE = `[["urlkey","timestamp","original","mimetype","statuscode","digest","length"],
["ru,kamaloff)/", "20130522121421", "http://kamaloff.ru/", "text/html", "200", "FXOQP7LM7FWUC7S5MTDHZS2WMKNLCW2E", "2558"],
["ru,kamaloff)/favicon.ico", "20180104074528", "http://kamaloff.ru/favicon.ico", "text/html", "301", "WHT3EXKF6XVIKYVG67BXESI75TWESKWU", "463"],
["ru,kamaloff)/login?next=/", "20180104100356", "https://kamaloff.ru/login/?next=/", "text/html", "200", "7AK62UMEB5LCDYN5JSLXZ7ZZG5Z7XNCQ", "1998"],
["ru,kamaloff)/robots.txt", "20130801111119", "http://kamaloff.ru/robots.txt", "text/html", "404", "6443ZIMC2V4HX7MZ4YUEY2VI3OEI36HM", "351"]]`

// Test interface compliance
var wbtest common.Source = &Wayback{}

// replayServer builds a local Wayback CDX replay:
//   - /cdx with showNumPages=true returns "2"
//   - page 0 has two rows, page 1 is truncated (broken JSON)
//   - /web/ serves archived file content
func replayServer(t *testing.T) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()

	mux.HandleFunc("/cdx", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("showNumPages") == "true" {
			fmt.Fprint(w, "2\n")
			return
		}
		switch q.Get("page") {
		case "0":
			fmt.Fprint(w, `[["urlkey","timestamp","original","mimetype","statuscode","digest","length"],
["com,example)/a", "20200105120000", "http://example.com/a", "text/html", "200", "DIGESTA", "100"],
["com,example)/b", "20200106120000", "http://example.com/b", "text/html", "200", "DIGESTB", "200"]]`)
		case "1":
			// Truncated mid-array: must not pollute results from other pages
			fmt.Fprint(w, `[["urlkey","timestamp","original","mimetype","statuscode","digest","length"],
["com,example)/c", "2020`)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	mux.HandleFunc("/web/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "archived-content")
	})

	return httptest.NewServer(mux)
}

func newReplaySource(t *testing.T, srv *httptest.Server) *Wayback {
	t.Helper()

	client := common.NewClient(5, 3)
	client.Backoff = func(int) time.Duration { return 0 }

	return &Wayback{
		MaxTimeout:    5,
		MaxRetries:    3,
		IndexServer:   srv.URL + "/cdx",
		StorageServer: srv.URL + "/web",
		Client:        client,
	}
}

func TestParseResponse(t *testing.T) {
	wb := &Wayback{}
	want := "http://kamaloff.ru/"

	parsedResp, err := wb.ParseResponse([]byte(RESPONSE))
	if err != nil {
		t.Fatalf("%v", err)
	}

	got := parsedResp[0].Original
	if got != want {
		t.Fatalf("parsed result doesn't contain wanted value: Want=%v, Got=%v", want, got)
	}
}

func TestParseResponseMalformedRow(t *testing.T) {
	wb := &Wayback{}
	// Row with only 3 columns must not panic with index out of range
	_, err := wb.ParseResponse([]byte(`[["urlkey","timestamp","original"],["com,x)/","20200101","http://x/"]]`))
	if err == nil {
		t.Fatal("Expected error on malformed row")
	}
}

func TestReplayPaginationBadPageDoesNotPollute(t *testing.T) {
	srv := replayServer(t)
	defer srv.Close()

	wb := newReplaySource(t, srv)
	config := common.RequestConfig{URL: "example.com/*"}

	results, err := wb.GetPages(context.Background(), config)
	if err == nil {
		t.Fatal("Expected aggregated error from truncated page")
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results from the good page, got %v", len(results))
	}
}

func TestReplayFetchPages(t *testing.T) {
	srv := replayServer(t)
	defer srv.Close()

	wb := newReplaySource(t, srv)
	config := common.RequestConfig{URL: "example.com/*"}

	results, err := common.FetchAll(context.Background(), []common.Source{wb}, config)
	if err == nil {
		t.Fatal("Expected aggregated error from truncated page")
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %v", len(results))
	}

	want := []string{"http://example.com/a", "http://example.com/b"}
	for i, w := range want {
		if results[i].Original != w {
			t.Fatalf("Result %d: want %v, got %v", i, w, results[i].Original)
		}
	}
}

func TestReplay429Retry(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, "1\n")
	}))
	defer srv.Close()

	wb := &Wayback{
		IndexServer: srv.URL,
		Client: func() *common.Client {
			c := common.NewClient(5, 3)
			c.Backoff = func(int) time.Duration { return 0 }
			return c
		}(),
	}

	pages, err := wb.GetNumPages(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("Expected success after 429 retries, got: %v", err)
	}
	if pages != 1 {
		t.Fatalf("Expected 1 page, got %v", pages)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("Expected 3 attempts, got %v", got)
	}
}

func TestReplayCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	wb := &Wayback{
		IndexServer: srv.URL,
		Client:      common.NewClient(30, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		common.FetchAll(ctx, []common.Source{wb}, common.RequestConfig{URL: "example.com/*"})
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("FetchAll did not return after cancellation; goroutines leaked")
	}
}

func TestReplayGetFile(t *testing.T) {
	srv := replayServer(t)
	defer srv.Close()

	wb := newReplaySource(t, srv)
	page := &common.CdxResponse{
		Timestamp: "20200105120000",
		Original:  "http://example.com/a",
	}

	data, err := wb.GetFile(context.Background(), page)
	if err != nil {
		t.Fatalf("Cannot get file from replay server: %v", err)
	}
	if string(data) != "archived-content" {
		t.Fatalf("Unexpected file content: %q", string(data))
	}
}
