package commoncrawl

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	common "github.com/karust/gogetcrawl/common"
)

// Use pre-saved response, hard to get live one
// Example request: http://index.commoncrawl.org/CC-MAIN-2023-14-index?url=tutorialspoint.com/*&output=json&limit=6&filter=statuscode:200&filter=mimetype:application/pdf
const RESPONSE = `{"urlkey": "com,tutorialspoint)/accounting_basics/accounting_basics_tutorial.pdf", "timestamp": "20230320100841", "url": "http://www.tutorialspoint.com/accounting_basics/accounting_basics_tutorial.pdf", "mime": "application/pdf", "mime-detected": "application/pdf", "status": "200", "digest": "2JQ2AQ3HQZIMXHB5CJGSADUGOHYBIRJJ", "length": "787172", "offset": "102849414", "filename": "crawl-data/CC-MAIN-2023-14/segments/1679296943471.24/warc/CC-MAIN-20230320083513-20230320113513-00267.warc.gz"}
{"urlkey": "com,tutorialspoint)/add_and_subtract_whole_numbers/pdf/subtracting_of_two_2digit_numbers_with_borrowing_worksheet10_1.pdf", "timestamp": "20230326185123", "url": "https://www.tutorialspoint.com/add_and_subtract_whole_numbers/pdf/subtracting_of_two_2digit_numbers_with_borrowing_worksheet10_1.pdf", "mime": "application/pdf", "mime-detected": "application/pdf", "status": "200", "digest": "T4OQARBGDQ2Z3ZMJ57MWZTUIBCFR65QG", "length": "120114", "offset": "1156945883", "filename": "crawl-data/CC-MAIN-2023-14/segments/1679296946445.46/warc/CC-MAIN-20230326173112-20230326203112-00412.warc.gz"}
`

// Test interface compliance
var cctest common.Source = &CommonCrawl{}

const warcRecord = "WARC/1.0\r\n" +
	"WARC-Type: response\r\n" +
	"WARC-Target-URI: http://example.com/\r\n" +
	"WARC-Date: 2020-01-15T00:00:00Z\r\n" +
	"Content-Type: text/plain\r\n" +
	"Content-Length: 5\r\n" +
	"\r\n" +
	"hello\r\n\r\n"

// replayServer builds a local Common Crawl replay:
//   - /collinfo.json serves two indexes
//   - CC-MAIN-2020-01 has 2 pages, page 1 is truncated (broken JSON)
//   - CC-MAIN-2020-05 has 1 page
//   - /crawl-data/ serves a WARC record and verifies the Range header
func replayServer(t *testing.T) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()

	mux.HandleFunc("/collinfo.json", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[
			{"id":"CC-MAIN-2020-05","name":"May 2020","from":"2020-04-01T00:00:00","to":"2020-05-31T23:59:59"},
			{"id":"CC-MAIN-2020-01","name":"January 2020","from":"2019-12-01T00:00:00","to":"2020-01-31T23:59:59"}
		]`)
	})

	mux.HandleFunc("/CC-MAIN-2020-01-index", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("showNumPages") == "true" {
			fmt.Fprint(w, `{"pages":2,"pageSize":1,"blocks":2}`)
			return
		}
		switch q.Get("page") {
		case "0":
			fmt.Fprint(w, "{\"urlkey\": \"com,example)/a\", \"timestamp\": \"20200105120000\", \"url\": \"http://example.com/a\", \"mime\": \"text/html\", \"status\": \"200\", \"length\": \"100\", \"offset\": \"0\", \"filename\": \"crawl-data/test.warc\"}\n")
			fmt.Fprint(w, "{\"urlkey\": \"com,example)/b\", \"timestamp\": \"20200106120000\", \"url\": \"http://example.com/b\", \"mime\": \"text/html\", \"status\": \"200\", \"length\": \"100\", \"offset\": \"200\", \"filename\": \"crawl-data/test.warc\"}\n")
		case "1":
			// Truncated mid-line: must not pollute results from other pages
			fmt.Fprint(w, "{\"urlkey\": \"com,example)/c\", \"timestamp\": \"2020")
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	mux.HandleFunc("/CC-MAIN-2020-05-index", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("showNumPages") == "true" {
			fmt.Fprint(w, `{"pages":1,"pageSize":1,"blocks":1}`)
			return
		}
		fmt.Fprint(w, "{\"urlkey\": \"com,example)/d\", \"timestamp\": \"20200510120000\", \"url\": \"http://example.com/d\", \"mime\": \"text/html\", \"status\": \"200\", \"length\": \"100\", \"offset\": \"400\", \"filename\": \"crawl-data/test.warc\"}\n")
	})

	mux.HandleFunc("/crawl-data/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, warcRecord)
	})

	return httptest.NewServer(mux)
}

func newReplaySource(t *testing.T, srv *httptest.Server) *CommonCrawl {
	t.Helper()

	client := common.NewClient(5, 2)
	client.Backoff = func(int) time.Duration { return 0 }

	cc := &CommonCrawl{
		MaxTimeout:    5,
		MaxRetries:    2,
		IndexServer:   srv.URL + "/",
		StorageServer: srv.URL + "/",
		Client:        client,
	}

	indexes, err := cc.GetIndexes(context.Background())
	if err != nil {
		t.Fatalf("Cannot get indexes from replay server: %v", err)
	}
	cc.indexes = indexes

	return cc
}

func TestParseResponse(t *testing.T) {
	cc := &CommonCrawl{}
	want := "http://www.tutorialspoint.com/accounting_basics/accounting_basics_tutorial.pdf"
	parsedResp, err := cc.ParseResponse([]byte(RESPONSE))
	if err != nil {
		t.Fatalf("%v", err)
	}

	got := parsedResp[0].Original
	if got != want {
		t.Fatalf("parsed result doesn't contain wanted value: Want=%v, Got=%v", want, got)
	}
}

func TestReplayGetIndexes(t *testing.T) {
	srv := replayServer(t)
	defer srv.Close()

	cc := newReplaySource(t, srv)
	if len(cc.indexes) != 2 {
		t.Fatalf("Expected 2 indexes, got %v", len(cc.indexes))
	}
	if cc.indexes[0].Id != "CC-MAIN-2020-05" {
		t.Fatalf("Unexpected first index: %v", cc.indexes[0].Id)
	}
}

func TestReplayPaginationBadPageDoesNotPollute(t *testing.T) {
	srv := replayServer(t)
	defer srv.Close()

	cc := newReplaySource(t, srv)
	config := common.RequestConfig{URL: "example.com/*"}

	// CC-MAIN-2020-01 has 2 pages; page 1 is truncated. Results from page 0
	// must survive and the error must be reported.
	results, err := cc.GetPagesIndex(context.Background(), config, "CC-MAIN-2020-01")
	if err == nil {
		t.Fatal("Expected aggregated error from truncated page")
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results from the good page, got %v", len(results))
	}
}

func TestReplayFetchPagesAcrossIndexes(t *testing.T) {
	srv := replayServer(t)
	defer srv.Close()

	cc := newReplaySource(t, srv)

	// Date filter matching both indexes
	config := common.RequestConfig{
		URL:      "example.com/*",
		FromDate: time.Date(2019, 6, 1, 0, 0, 0, 0, time.UTC),
		ToDate:   time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC),
	}

	results, err := common.FetchAll(context.Background(), []common.Source{cc}, config)

	// 2 good results from CC-MAIN-2020-01 (page 1 truncated) + 1 from CC-MAIN-2020-05
	if len(results) != 3 {
		t.Fatalf("Expected 3 results across indexes, got %v", len(results))
	}
	if err == nil {
		t.Fatal("Expected aggregated error from the truncated page")
	}

	// Deterministic order by timestamp
	want := []string{"http://example.com/a", "http://example.com/b", "http://example.com/d"}
	for i, w := range want {
		if results[i].Original != w {
			t.Fatalf("Result %d: want %v, got %v", i, w, results[i].Original)
		}
	}
}

func TestReplayFetchPagesGlobalLimit(t *testing.T) {
	srv := replayServer(t)
	defer srv.Close()

	cc := newReplaySource(t, srv)
	config := common.RequestConfig{
		URL:      "example.com/*",
		Limit:    2,
		FromDate: time.Date(2019, 6, 1, 0, 0, 0, 0, time.UTC),
		ToDate:   time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC),
	}

	results, _ := common.FetchAll(context.Background(), []common.Source{cc}, config)
	if len(results) != 2 {
		t.Fatalf("Expected global limit of 2 results across indexes, got %v", len(results))
	}
}

func TestReplayCancel(t *testing.T) {
	// Server that blocks until the request context is cancelled
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	cc := &CommonCrawl{
		IndexServer: srv.URL + "/",
		Client:      common.NewClient(30, 1),
		indexes:     []latestIndex{{Id: "CC-MAIN-2020-01"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		common.FetchAll(ctx, []common.Source{cc}, common.RequestConfig{URL: "example.com/*"})
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

	cc := newReplaySource(t, srv)
	page := &common.CdxResponse{
		Offset:   "0",
		Length:   "100",
		Filename: "crawl-data/test.warc",
	}

	data, err := cc.GetFile(context.Background(), page)
	if err != nil {
		t.Fatalf("Cannot get file from replay server: %v", err)
	}
	if strings.TrimSpace(string(data)) != "hello" {
		t.Fatalf("Unexpected WARC content: %q", string(data))
	}
}
