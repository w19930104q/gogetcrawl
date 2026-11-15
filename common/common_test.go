package common

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// --- Client tests (local replay) ---

func TestClientRetriesOn429(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	client := NewClient(5, 3)
	client.Backoff = func(int) time.Duration { return 0 }

	body, err := client.Get(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("Expected success after retries, got: %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("Unexpected body: %q", body)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("Expected 3 attempts, got %v", got)
	}
}

func TestClientGivesUpAfterRetries(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	client := NewClient(5, 2)
	client.Backoff = func(int) time.Duration { return 0 }

	_, err := client.Get(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("Expected error after retries exhausted")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Fatalf("Expected 429 error, got: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("Expected 2 attempts, got %v", got)
	}
}

func TestClientNoRetryOnClientError(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	client := NewClient(5, 3)
	client.Backoff = func(int) time.Duration { return 0 }

	_, err := client.Get(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("Expected error on 400")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("Expected no retries on 400, got %v attempts", got)
	}
}

func TestClientContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // block until the request is cancelled
	}))
	defer srv.Close()

	client := NewClient(30, 3)
	client.Backoff = func(int) time.Duration { return 0 }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.Get(ctx, srv.URL)
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Expected cancellation error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Client did not return after context cancellation")
	}
}

// --- FetchAll tests ---

type fakeSource struct {
	name    string
	batches [][]*CdxResponse
	errs    []error
	block   bool // wait for ctx cancellation inside FetchPages
}

func (f *fakeSource) Name() string { return f.name }
func (f *fakeSource) ParseResponse(resp []byte) ([]*CdxResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeSource) GetNumPages(ctx context.Context, url string) (int, error) { return 1, nil }
func (f *fakeSource) GetPages(ctx context.Context, config RequestConfig) ([]*CdxResponse, error) {
	return nil, nil
}
func (f *fakeSource) GetFile(ctx context.Context, page *CdxResponse) ([]byte, error) {
	return nil, nil
}
func (f *fakeSource) FetchPages(ctx context.Context, config RequestConfig, results chan []*CdxResponse, errs chan error) {
	for _, b := range f.batches {
		if !SendBatch(ctx, results, b) {
			return
		}
	}
	for _, e := range f.errs {
		if !SendError(ctx, errs, e) {
			return
		}
	}
	if f.block {
		<-ctx.Done()
	}
}

func TestFetchAllGlobalLimitAndDeterministicOrder(t *testing.T) {
	srcA := &fakeSource{name: "A", batches: [][]*CdxResponse{
		{
			{Timestamp: "20200103", Original: "http://c.example"},
			{Timestamp: "20200101", Original: "http://a.example"},
		},
	}}
	srcB := &fakeSource{name: "B", batches: [][]*CdxResponse{
		{
			{Timestamp: "20200102", Original: "http://b.example"},
			{Timestamp: "20200104", Original: "http://d.example"},
		},
	}}

	for _, src := range []*fakeSource{srcA, srcB} {
		for _, batch := range src.batches {
			for _, r := range batch {
				r.Source = src
			}
		}
	}

	config := RequestConfig{URL: "example.com", Limit: 3}
	results, err := FetchAll(context.Background(), []Source{srcA, srcB}, config)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected global limit of 3 results, got %v", len(results))
	}

	want := []string{"http://a.example", "http://b.example", "http://c.example"}
	for i, w := range want {
		if results[i].Original != w {
			t.Fatalf("Result %d: want %v, got %v", i, w, results[i].Original)
		}
	}
}

func TestFetchAllAggregatesErrors(t *testing.T) {
	errA := errors.New("source A failed")
	errB := errors.New("source B failed")
	srcA := &fakeSource{name: "A", errs: []error{errA}}
	srcB := &fakeSource{name: "B", errs: []error{errB}}

	_, err := FetchAll(context.Background(), []Source{srcA, srcB}, RequestConfig{URL: "example.com"})
	if err == nil {
		t.Fatal("Expected aggregated error")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("Expected both errors aggregated, got: %v", err)
	}
}

func TestFetchAllCancel(t *testing.T) {
	src := &fakeSource{name: "blocking", block: true}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		FetchAll(ctx, []Source{src}, RequestConfig{URL: "example.com"})
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

func TestFetchAllUnconsumedChannelsDontDeadlock(t *testing.T) {
	// A source that emits more than one batch must not block forever even
	// though FetchAll is the only reader: FetchAll drains concurrently.
	batches := [][]*CdxResponse{}
	for i := 0; i < 100; i++ {
		batches = append(batches, []*CdxResponse{{Timestamp: fmt.Sprintf("2020%06d", i), Original: "http://x.example"}})
	}
	src := &fakeSource{name: "chatty", batches: batches}

	done := make(chan int, 1)
	go func() {
		results, _ := FetchAll(context.Background(), []Source{src}, RequestConfig{URL: "example.com"})
		done <- len(results)
	}()

	select {
	case n := <-done:
		if n != 100 {
			t.Fatalf("Expected 100 results, got %v", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FetchAll deadlocked on multiple batches")
	}
}
