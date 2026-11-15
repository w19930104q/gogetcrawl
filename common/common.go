package common

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// WebArchive and Common Crawl (index.commoncrawl.org) CDX API Response structure from
type CdxResponse struct {
	Urlkey       string `json:"urlkey,omitempty"`
	Timestamp    string `json:"timestamp,omitempty"`
	Charset      string `json:"charset,omitempty"`
	MimeType     string `json:"mime,omitempty"`
	Languages    string `json:"languages,omitempty"`
	MimeDetected string `json:"mimedetected,omitempty"`
	Digest       string `json:"digest,omitempty"`
	Offset       string `json:"offset,omitempty"`
	Original     string `json:"url,omitempty"` // Original URL
	Length       string `json:"length,omitempty"`
	StatusCode   string `json:"status,omitempty"`
	Filename     string `json:"filename,omitempty"`
	Source       Source
}

// Source of web archive data
type Source interface {
	Name() string
	ParseResponse(resp []byte) ([]*CdxResponse, error)
	GetNumPages(ctx context.Context, url string) (int, error)
	GetPages(ctx context.Context, config RequestConfig) ([]*CdxResponse, error)
	FetchPages(ctx context.Context, config RequestConfig, results chan []*CdxResponse, errors chan error)
	GetFile(ctx context.Context, page *CdxResponse) ([]byte, error)
}

type RequestConfig struct {
	URL            string    // Url to parse
	Filters        []string  // Extenstion to search
	Limit          uint      // Max number of results per page
	CollapseColumn string    // Which column to use to collapse results
	SinglePage     bool      // Get results only from 1st page (mostly used for tests)
	FromDate       time.Time // Filter results from Date
	ToDate         time.Time // Filter results to Date
}

// GetUrlFromConfig ... Compose URL with CDX server request parameters
func (config RequestConfig) GetUrl(serverURL string, page int) string {
	reqURL := fmt.Sprintf("%v?url=%v&output=json", serverURL, config.URL)

	if config.Limit != 0 {
		reqURL = fmt.Sprintf("%v&limit=%v", reqURL, config.Limit)
	}

	if config.CollapseColumn != "" {
		reqURL = fmt.Sprintf("%v&collapse=%v", reqURL, config.CollapseColumn)
	}

	for _, filter := range config.Filters {
		if filter != "" {
			reqURL = fmt.Sprintf("%v&filter=%v", reqURL, filter)
		}
	}

	if !config.FromDate.IsZero() {
		reqURL = fmt.Sprintf("%v&from=%v", reqURL, config.FromDate.Format("20060102"))
	}

	if !config.ToDate.IsZero() {
		reqURL = fmt.Sprintf("%v&to=%v", reqURL, config.ToDate.Format("20060102"))
	}

	if !config.SinglePage {
		reqURL = fmt.Sprintf("%v&page=%v", reqURL, page)
	}
	return reqURL
}

// FetchAll queries all given sources concurrently and merges their results.
//
// The merged results are sorted deterministically (see SortCdxResponses) and
// truncated to config.Limit if it is set, so the limit applies globally across
// all sources and indexes rather than per index. Errors from individual
// sources or pages don't abort the others; they are aggregated and returned
// together with the results. All spawned goroutines exit before FetchAll
// returns, even when ctx is cancelled.
func FetchAll(ctx context.Context, sources []Source, config RequestConfig) ([]*CdxResponse, error) {
	results := make(chan []*CdxResponse)
	errs := make(chan error)

	var wg sync.WaitGroup
	for _, s := range sources {
		wg.Add(1)
		go func(s Source) {
			defer wg.Done()
			s.FetchPages(ctx, config, results, errs)
		}(s)
	}

	// Close both channels once every source is done, so the collector below
	// terminates and no goroutine is left behind.
	go func() {
		wg.Wait()
		close(results)
		close(errs)
	}()

	var all []*CdxResponse
	var errList []error
	for results != nil || errs != nil {
		select {
		case batch, ok := <-results:
			if !ok {
				results = nil
				continue
			}
			all = append(all, batch...)
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			errList = append(errList, err)
		}
	}

	SortCdxResponses(all)
	if config.Limit != 0 && uint(len(all)) > config.Limit {
		all = all[:config.Limit]
	}

	return all, errors.Join(errList...)
}

// SortCdxResponses sorts results deterministically by timestamp, then URL,
// then source name, so merged multi-source output is stable across runs.
func SortCdxResponses(results []*CdxResponse) {
	sort.SliceStable(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if a.Timestamp != b.Timestamp {
			return a.Timestamp < b.Timestamp
		}
		if a.Original != b.Original {
			return a.Original < b.Original
		}
		if a.Urlkey != b.Urlkey {
			return a.Urlkey < b.Urlkey
		}
		return sourceName(a.Source) < sourceName(b.Source)
	})
}

func sourceName(s Source) string {
	if s == nil {
		return ""
	}
	return s.Name()
}

// SendBatch delivers a batch of results unless the context is cancelled.
// Returns false when the context is done and the batch was not sent.
func SendBatch(ctx context.Context, results chan []*CdxResponse, batch []*CdxResponse) bool {
	select {
	case results <- batch:
		return true
	case <-ctx.Done():
		return false
	}
}

// SendError delivers err unless the context is cancelled.
// Returns false when the context is done and the error was not sent.
func SendError(ctx context.Context, errs chan error, err error) bool {
	select {
	case errs <- err:
		return true
	case <-ctx.Done():
		return false
	}
}

// Save data using file fullpath
func SaveFile(data []byte, path string) error {
	err := os.WriteFile(path, data, 0o644)
	if err != nil {
		return err
	}

	return nil
}

// SaveResults saves files described by CDX responses into output directory.
// Per-file errors are aggregated and returned; saving continues past them.
func SaveResults(ctx context.Context, results []*CdxResponse, outputDir string, downloadRate float32) error {
	var errList []error

	for _, res := range results {
		if err := ctx.Err(); err != nil {
			return err
		}

		data, err := res.Source.GetFile(ctx, res)
		if err != nil {
			errList = append(errList, err)
			continue
		}

		exts, err := mime.ExtensionsByType(res.MimeType)
		if err != nil || len(exts) == 0 {
			errList = append(errList, fmt.Errorf("Cannot get extension from file"))
			continue
		}

		filename := fmt.Sprintf("%v-%v-%v%v", res.Original, res.Timestamp, res.Source.Name(), exts[0])
		fullPath := filepath.Join(outputDir, url.QueryEscape(filename))

		if err := SaveFile(data, fullPath); err != nil {
			errList = append(errList, err)
		}

		select {
		case <-time.After(time.Duration(downloadRate * float32(time.Second))):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return errors.Join(errList...)
}
