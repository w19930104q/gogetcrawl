package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/karust/gogetcrawl/common"
)

// fakeSource is a controllable common.Source for fan-in tests.
type fakeSource struct {
	name    string
	batches [][]*common.CdxResponse
	err     error
	block   bool // wait for ctx cancellation instead of returning
	onFetch func()
}

func (f *fakeSource) Name() string { return f.name }

func (f *fakeSource) ParseResponse(resp []byte) ([]*common.CdxResponse, error) { return nil, nil }

func (f *fakeSource) GetNumPages(url string) (int, error) { return 1, nil }

func (f *fakeSource) GetPages(config common.RequestConfig) ([]*common.CdxResponse, error) {
	return nil, nil
}

func (f *fakeSource) GetFile(*common.CdxResponse) ([]byte, error) { return []byte("data"), nil }

func (f *fakeSource) FetchPages(ctx context.Context, config common.RequestConfig, results chan<- []*common.CdxResponse, errs chan<- error) {
	if f.onFetch != nil {
		f.onFetch()
	}
	for _, b := range f.batches {
		if !common.SendResults(ctx, results, b) {
			return
		}
	}
	if f.err != nil {
		common.SendError(ctx, errs, f.err)
	}
	if f.block {
		<-ctx.Done()
	}
}

func batch(urls ...string) []*common.CdxResponse {
	res := make([]*common.CdxResponse, len(urls))
	for i, u := range urls {
		res[i] = &common.CdxResponse{Original: u}
	}
	return res
}

// setupRunEnv swaps the package-level fan-in state with fresh values and
// restores it afterwards, so tests can run the collectors directly.
func setupRunEnv(t *testing.T, srcs []common.Source, limit uint) {
	t.Helper()
	oldSources, oldResults, oldErrors := sources, results, errors
	oldMaxWorkers, oldMaxResults := maxWorkers, maxResults

	sources = srcs
	results = make(chan []*common.CdxResponse)
	errors = make(chan error)
	maxWorkers = 2
	maxResults = limit

	t.Cleanup(func() {
		sources, results, errors = oldSources, oldResults, oldErrors
		maxWorkers, maxResults = oldMaxWorkers, oldMaxResults
	})
}

func testConfigs(urls ...string) chan common.RequestConfig {
	configs := make(chan common.RequestConfig, len(urls))
	for _, u := range urls {
		configs <- common.RequestConfig{URL: u}
	}
	close(configs)
	return configs
}

func TestURLRunDualSourcePartialSuccess(t *testing.T) {
	good := &fakeSource{name: "good", batches: [][]*common.CdxResponse{
		batch("http://a.example.com/", "http://b.example.com/"),
	}}
	bad := &fakeSource{name: "bad", err: fmt.Errorf("boom: %w", common.Status500Error)}
	setupRunEnv(t, []common.Source{good, bad}, 0)

	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	us := &urlScenario{}
	got := us.run(ctx, cancel, testConfigs("example.com"), &out)

	if got != 2 {
		t.Fatalf("Expected 2 collected results, got %v", got)
	}
	for _, want := range []string{"http://a.example.com/", "http://b.example.com/"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("Output missing %q: %q", want, out.String())
		}
	}
}

func TestURLRunLimitTruncation(t *testing.T) {
	srcA := &fakeSource{name: "a", batches: [][]*common.CdxResponse{
		batch("http://a1/", "http://a2/", "http://a3/", "http://a4/"),
	}}
	srcB := &fakeSource{name: "b", batches: [][]*common.CdxResponse{
		batch("http://b1/", "http://b2/", "http://b3/", "http://b4/"),
	}}
	setupRunEnv(t, []common.Source{srcA, srcB}, 5)

	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	us := &urlScenario{}
	got := us.run(ctx, cancel, testConfigs("example.com"), &out)

	if got != 5 {
		t.Fatalf("--limit must apply exactly to final usable results: got %v, want 5", got)
	}
	if lines := strings.Count(out.String(), "\n"); lines != 5 {
		t.Fatalf("Expected exactly 5 output lines, got %v", lines)
	}
}

func TestURLRunNoResults(t *testing.T) {
	setupRunEnv(t, []common.Source{
		&fakeSource{name: "a"},
		&fakeSource{name: "b", err: fmt.Errorf("rate limited: %w", common.Status429Error)},
	}, 10)

	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	us := &urlScenario{}
	done := make(chan uint, 1)
	go func() { done <- us.run(ctx, cancel, testConfigs("example.com"), &out) }()

	select {
	case got := <-done:
		if got != 0 {
			t.Fatalf("Expected 0 results, got %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not exit normally with no results")
	}
}

func TestURLRunCancel(t *testing.T) {
	// One source blocks forever; cancellation must release every goroutine.
	blocking := &fakeSource{name: "blocking", block: true}
	setupRunEnv(t, []common.Source{blocking}, 0)

	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())

	us := &urlScenario{}
	done := make(chan uint, 1)
	go func() { done <- us.run(ctx, cancel, testConfigs("example.com"), &out) }()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancellation")
	}
}

func TestURLRunLimitCancelsBlockedSource(t *testing.T) {
	// Source A fills the limit; source B blocks. Reaching the limit must
	// cancel B and let the run finish.
	var fetchedA, fetchedB int32
	srcA := &fakeSource{
		name:    "a",
		batches: [][]*common.CdxResponse{batch("http://a1/", "http://a2/")},
		onFetch: func() { atomic.AddInt32(&fetchedA, 1) },
	}
	srcB := &fakeSource{
		name:    "b",
		block:   true,
		onFetch: func() { atomic.AddInt32(&fetchedB, 1) },
	}
	setupRunEnv(t, []common.Source{srcA, srcB}, 2)

	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	us := &urlScenario{}
	done := make(chan uint, 1)
	go func() { done <- us.run(ctx, cancel, testConfigs("example.com"), &out) }()

	select {
	case got := <-done:
		if got != 2 {
			t.Fatalf("Expected exactly 2 results, got %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run stuck after limit was reached")
	}
}

func TestFileRunSavesFiles(t *testing.T) {
	dir := t.TempDir()
	src := &fakeSource{name: "a", batches: [][]*common.CdxResponse{
		{
			{Original: "http://a.example.com/", Timestamp: "20200101000000", MimeType: "text/html"},
			{Original: "http://b.example.com/", Timestamp: "20200101000001", MimeType: "text/html"},
		},
	}}
	setupRunEnv(t, []common.Source{src}, 0)

	// GetFile needs a Source reference on each result.
	for _, b := range src.batches {
		for _, r := range b {
			r.Source = src
		}
	}

	fs := &fileScenario{outputDir: dir, downloadRate: 0}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fs.run(ctx, testConfigs("example.com"))
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("file run did not exit")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Cannot read output dir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Expected 2 saved files, got %v", len(entries))
	}
}

func TestFileRunLimit(t *testing.T) {
	dir := t.TempDir()
	srcA := &fakeSource{name: "a", batches: [][]*common.CdxResponse{
		{
			{Original: "http://a1/", Timestamp: "20200101000000", MimeType: "text/html"},
			{Original: "http://a2/", Timestamp: "20200101000001", MimeType: "text/html"},
		},
	}}
	srcB := &fakeSource{name: "b", batches: [][]*common.CdxResponse{
		{
			{Original: "http://b1/", Timestamp: "20200101000002", MimeType: "text/html"},
			{Original: "http://b2/", Timestamp: "20200101000003", MimeType: "text/html"},
		},
	}}
	for _, s := range []*fakeSource{srcA, srcB} {
		for _, b := range s.batches {
			for _, r := range b {
				r.Source = s
			}
		}
	}
	setupRunEnv(t, []common.Source{srcA, srcB}, 3)

	fs := &fileScenario{outputDir: dir, downloadRate: 0}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fs.run(ctx, testConfigs("example.com"))
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("file run did not exit")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Cannot read output dir: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("--limit must apply exactly to saved files: got %v, want 3", len(entries))
	}
}
