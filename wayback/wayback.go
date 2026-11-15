package wayback

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
	common "github.com/karust/gogetcrawl/common"
)

const INDEX_SERVER = "https://web.archive.org/cdx/search/cdx"
const CRAWL_STORAGE = "https://web.archive.org/web"

type Wayback struct {
	MaxTimeout int // Request timeout
	MaxRetries int // Max number of request retries if timeouted

	// IndexServer is the CDX API URL. Defaults to INDEX_SERVER.
	IndexServer string
	// StorageServer is the replay base URL. Defaults to CRAWL_STORAGE.
	StorageServer string
	// Client is the HTTP client used for all requests. Defaults to a client
	// built from MaxTimeout/MaxRetries.
	Client *common.Client
}

func New(timeout, retries int) (*Wayback, error) {
	source := &Wayback{
		MaxTimeout: timeout,
		MaxRetries: retries,
		Client:     common.NewClient(timeout, retries),
	}
	return source, nil
}

func (Wayback) Name() string {
	return "Wayback"
}

func (wb *Wayback) http() *common.Client {
	if wb.Client != nil {
		return wb.Client
	}
	return common.NewClient(wb.MaxTimeout, wb.MaxRetries)
}

func (wb *Wayback) indexServer() string {
	if wb.IndexServer != "" {
		return wb.IndexServer
	}
	return INDEX_SERVER
}

func (wb *Wayback) storageServer() string {
	if wb.StorageServer != "" {
		return wb.StorageServer
	}
	return CRAWL_STORAGE
}

// Return the number of pages located in WebArchive for given url
func (wb *Wayback) GetNumPages(ctx context.Context, url string) (int, error) {
	requestURI := fmt.Sprintf("%v?url=%v&showNumPages=true", wb.indexServer(), url)
	response, err := wb.http().Get(ctx, requestURI)
	if err != nil {
		return 0, fmt.Errorf("[GetNumPages] Request error: %v", err)
	}

	// Remove return and convert to integer
	strRes := strings.TrimSpace(string(response))
	res, err := strconv.Atoi(strRes)
	if err != nil {
		return 0, fmt.Errorf("[GetNumPages] Cannot convert response value: %v", response)
	}

	return res, nil
}

// Parse response from https://web.archive.org/cdx/search/cdx CDX server
func (wb *Wayback) ParseResponse(resp []byte) ([]*common.CdxResponse, error) {
	var results [][]string

	err := jsoniter.Unmarshal(resp, &results)
	if err != nil {
		return nil, fmt.Errorf("[ParseResponse] Failed to decode Wayback results '%v'", err)
	}

	parsedResults := []*common.CdxResponse{}
	for i, entry := range results {
		// Skip header
		if i == 0 {
			continue
		}

		if len(entry) < 7 {
			return nil, fmt.Errorf("[ParseResponse] Malformed row %d: expected 7 columns, got %d", i, len(entry))
		}

		parsed := common.CdxResponse{
			Urlkey:     entry[0],
			Timestamp:  entry[1],
			Original:   entry[2],
			MimeType:   entry[3],
			StatusCode: entry[4],
			Digest:     entry[5],
			Length:     entry[6],
		}

		parsed.Source = wb
		parsedResults = append(parsedResults, &parsed)
	}

	return parsedResults, nil
}

// fetchPage requests and parses a single result page.
func (wb *Wayback) fetchPage(ctx context.Context, config common.RequestConfig, page int) ([]*common.CdxResponse, error) {
	reqURL := config.GetUrl(wb.indexServer(), page)

	response, err := wb.http().Get(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("Request error: %w", err)
	}

	parsedResponse, err := wb.ParseResponse(response)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse response: %w", err)
	}

	return parsedResponse, nil
}

// GetPages ... Makes request to WebArchive CDX API to gather all url observations
//
// A broken page (request or parse failure) is skipped and its error
// aggregated, so it doesn't pollute results from other pages.
func (wb *Wayback) GetPages(ctx context.Context, config common.RequestConfig) ([]*common.CdxResponse, error) {
	pages := 1
	if !config.SinglePage {
		var err error
		pages, err = wb.GetNumPages(ctx, config.URL)
		if err != nil {
			return nil, err
		}
	}

	var results []*common.CdxResponse
	var errList []error
	numResults := 0

	for page := 0; page < pages; page++ {
		parsedResponse, err := wb.fetchPage(ctx, config, page)
		if err != nil {
			errList = append(errList, fmt.Errorf("[GetPages] page %d: %w", page, err))
			continue
		}
		results = append(results, parsedResponse...)
		numResults += len(parsedResponse)

		if config.Limit != 0 && uint(numResults) >= config.Limit {
			break
		}
	}

	return results, errors.Join(errList...)
}

// FetchPages ... Concurrent way to GetPages.
// Makes request to WebArchive CDX API and return observations in a channel.
//
// Broken pages are reported on the errors channel and skipped without
// polluting other results. The goroutine exits when done or when ctx is
// cancelled.
func (wb *Wayback) FetchPages(ctx context.Context, config common.RequestConfig, results chan []*common.CdxResponse, errs chan error) {
	pages := 1
	if !config.SinglePage {
		var err error
		pages, err = wb.GetNumPages(ctx, config.URL)
		if err != nil {
			common.SendError(ctx, errs, fmt.Errorf("[FetchPages] Cannot get number of pages: %w", err))
			return
		}
	}

	numResults := 0

	for page := 0; page < pages; page++ {
		parsedResponse, err := wb.fetchPage(ctx, config, page)
		if err != nil {
			if !common.SendError(ctx, errs, fmt.Errorf("[FetchPages] page %d: %w", page, err)) {
				return
			}
			continue
		}

		if len(parsedResponse) == 0 {
			continue
		}
		numResults += len(parsedResponse)

		if !common.SendBatch(ctx, results, parsedResponse) {
			return
		}

		if config.Limit != 0 && uint(numResults) >= config.Limit {
			return
		}
	}
}

// Download file from WebArchive using a link from CDX response
func (wb *Wayback) GetFile(ctx context.Context, page *common.CdxResponse) ([]byte, error) {
	requestURI := fmt.Sprintf("%v/%vid_/%v", wb.storageServer(), page.Timestamp, page.Original)
	response, err := wb.http().Get(ctx, requestURI)
	if err != nil {
		return nil, fmt.Errorf("[GetFile] Request error: %v", err)
	}
	return response, nil
}
