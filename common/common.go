package common

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/corpix/uarand"
	"github.com/valyala/fasthttp"
)

var (
	Status429Error = errors.New("Server returned 429 status response. (Rate limited)")
	Status500Error = errors.New("Server returned 500 status response. (Slow down)")
	Status503Error = errors.New("Server returned 503 status response")
)

// RetryInterval is the base backoff between request retries (attempt N waits N*RetryInterval).
// It is a variable so tests can shrink it.
var RetryInterval = time.Second

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
	GetNumPages(url string) (int, error)
	GetPages(config RequestConfig) ([]*CdxResponse, error)
	FetchPages(ctx context.Context, config RequestConfig, results chan<- []*CdxResponse, errors chan<- error)
	GetFile(*CdxResponse) ([]byte, error)
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

// SendResults delivers a batch to the results channel unless the context is
// cancelled first. Returns false when the send was abandoned, meaning the
// caller must stop producing and return so no goroutine is leaked.
func SendResults(ctx context.Context, results chan<- []*CdxResponse, batch []*CdxResponse) bool {
	select {
	case results <- batch:
		return true
	case <-ctx.Done():
		return false
	}
}

// SendError delivers an error to the errors channel unless the context is
// cancelled first. Returns false when the send was abandoned.
func SendError(ctx context.Context, errors chan<- error, err error) bool {
	select {
	case errors <- err:
		return true
	case <-ctx.Done():
		return false
	}
}

func DoRequest(url string, timeout int, headers map[string]string) ([]byte, error) {
	timeoutDuration := time.Second * time.Duration(timeout)

	req := fasthttp.AcquireRequest()
	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.Header.Set(fasthttp.HeaderUserAgent, uarand.GetRandom())
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	client := &fasthttp.Client{}
	client.ReadTimeout = timeoutDuration
	err := client.DoTimeout(req, resp, timeoutDuration)
	if err != nil {
		return nil, fmt.Errorf("[DoRequest] Error making request: %v", err)
	}

	switch resp.StatusCode() {
	case 429:
		return nil, Status429Error
	case 500:
		return nil, Status500Error
	case 503:
		return nil, Status503Error
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("[DoRequest] Got %v status response", resp.StatusCode())
	}

	if len(resp.Body()) == 0 {
		return nil, nil
	}

	// Copy the body: it is only valid until ReleaseResponse runs.
	return append([]byte(nil), resp.Body()...), nil
}

// Get ... Performs HTTP GET request and returns response bytes
func Get(url string, timeout int, maxRetries int) ([]byte, error) {
	return GetContext(context.Background(), url, timeout, maxRetries)
}

// GetContext performs an HTTP GET request and returns the response body.
//
// Only 200 responses with a non-empty body are returned as success; a 200
// with an empty body yields (nil, nil) so callers can treat it as "no
// results". Transient failures (network errors, 429, 500, 503) are retried
// up to maxRetries times with a growing backoff; when retries are exhausted
// the returned error still wraps the final category (e.g. Status429Error).
// Any other status code fails immediately. The request and the backoff both
// honor ctx cancellation, and the response body is always closed.
func GetContext(ctx context.Context, url string, timeout int, maxRetries int) ([]byte, error) {
	if maxRetries < 1 {
		maxRetries = 1
	}

	client := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		log.Printf("GET [t=%v] [attempt=%v/%v]: %v", timeout, attempt+1, maxRetries, url)

		body, status, err := doGet(ctx, client, url)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
		} else {
			switch status {
			case http.StatusOK:
				if len(body) == 0 {
					return nil, nil
				}
				return body, nil
			case http.StatusTooManyRequests:
				lastErr = Status429Error
			case http.StatusInternalServerError:
				lastErr = Status500Error
			case http.StatusServiceUnavailable:
				lastErr = Status503Error
			default:
				return nil, fmt.Errorf("[Get] Got %v status response", status)
			}
		}

		if attempt+1 < maxRetries {
			timer := time.NewTimer(RetryInterval * time.Duration(attempt+1))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}

	return nil, fmt.Errorf("[Get] Request failed after %v attempt(s): %w", maxRetries, lastErr)
}

// doGet performs a single GET request, always closing the response body.
func doGet(ctx context.Context, client *http.Client, url string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("[Get] Cannot create request: %w", err)
	}
	req.Header.Set("User-Agent", uarand.GetRandom())

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("[Get] Request error: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("[Get] Cannot read response body: %w", err)
	}

	return body, resp.StatusCode, nil
}

// Save data using file fullpath
func SaveFile(data []byte, path string) error {
	err := os.WriteFile(path, data, 0o644)
	if err != nil {
		return err
	}

	return nil
}

// Save files from CDX Response channel into output directory.
// Returns when the results channel is closed and drained, or when ctx is
// cancelled. Errors are reported through the errors channel without blocking
// shutdown.
func SaveFiles(ctx context.Context, results <-chan []*CdxResponse, outputDir string, errors chan<- error, downloadRate float32) {
	for resBatch := range results {
		for _, res := range resBatch {
			if ctx.Err() != nil {
				return
			}

			data, err := res.Source.GetFile(res)
			if err != nil {
				SendError(ctx, errors, err)
				continue
			}

			exts, err := mime.ExtensionsByType(res.MimeType)
			if err != nil || len(exts) == 0 {
				SendError(ctx, errors, fmt.Errorf("Cannot get extension from file"))
				continue
			}

			filename := fmt.Sprintf("%v-%v-%v%v", res.Original, res.Timestamp, res.Source.Name(), exts[0])
			fullPath := filepath.Join(outputDir, url.QueryEscape(filename))

			if err := SaveFile(data, fullPath); err != nil {
				SendError(ctx, errors, err)
			}

			if downloadRate > 0 {
				timer := time.NewTimer(time.Duration(downloadRate * float32(time.Second)))
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
	}
}
