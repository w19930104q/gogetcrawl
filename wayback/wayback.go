package wayback

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
	common "github.com/karust/gogetcrawl/common"
)

// INDEX_SERVER is a variable (not a const) so tests can point it at a local server.
var INDEX_SERVER = "https://web.archive.org/cdx/search/cdx"

const CRAWL_STORAGE = "https://web.archive.org/web"

type Wayback struct {
	MaxTimeout int // Request timeout
	MaxRetries int // Max number of request retries if timeouted
}

func New(timeout, retries int) (*Wayback, error) {
	source := &Wayback{MaxTimeout: timeout, MaxRetries: retries}
	return source, nil
}

func (Wayback) Name() string {
	return "Wayback"
}

// Return the number of pages located in WebArchive for given url
func (wb *Wayback) GetNumPages(url string) (int, error) {
	return wb.getNumPages(context.Background(), url)
}

func (wb *Wayback) getNumPages(ctx context.Context, url string) (int, error) {
	requestURI := fmt.Sprintf("%v?url=%v&showNumPages=true", INDEX_SERVER, url)
	response, err := common.GetContext(ctx, requestURI, wb.MaxTimeout, wb.MaxRetries)
	if err != nil {
		return 0, fmt.Errorf("[GetNumPages] Request error: %w", err)
	}

	// Remove return and convert to integer
	strRes := strings.TrimSpace(string(response))
	if strRes == "" {
		return 0, fmt.Errorf("[GetNumPages] Empty response")
	}

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

// GetPages ... Makes request to WebArchive CDX API to gather all url observations
func (wb *Wayback) GetPages(config common.RequestConfig) ([]*common.CdxResponse, error) {
	var pages int
	var err error

	if config.SinglePage {
		pages = 1
	} else {
		pages, err = wb.GetNumPages(config.URL)
		if err != nil {
			return nil, err
		}
	}

	var results []*common.CdxResponse
	numResults := 0

	for page := 0; page < pages; page++ {
		reqURL := config.GetUrl(INDEX_SERVER, page)

		response, err := common.Get(reqURL, wb.MaxTimeout, wb.MaxRetries)
		if err != nil {
			return results, fmt.Errorf("[GetPages] Request error: %v", err)
		}

		// Only parse non-empty 200 bodies
		if len(response) == 0 {
			continue
		}

		parsedResponse, err := wb.ParseResponse(response)
		if err != nil {
			return results, fmt.Errorf("[GetPages] Cannot parse response: %v", err)
		}
		results = append(results, parsedResponse...)
		numResults += len(parsedResponse)

		if config.Limit != 0 && uint(numResults) >= config.Limit {
			break
		}
	}

	return results, nil
}

// FetchPages ... Concurrent way to GetPages.
// Makes request to WebArchive CDX API and return observations in a channel.
//
// Only 200 bodies with content are parsed. Request and parse failures are
// reported through the errors channel and the source moves on; the function
// returns early when ctx is cancelled or the configured limit is reached,
// and never blocks on a channel send after cancellation.
func (wb *Wayback) FetchPages(ctx context.Context, config common.RequestConfig, results chan<- []*common.CdxResponse, errors chan<- error) {
	pages := 1

	if !config.SinglePage {
		var err error
		pages, err = wb.getNumPages(ctx, config.URL)
		if err != nil {
			common.SendError(ctx, errors, fmt.Errorf("[FetchPages] Cannot get number of pages: %w", err))
			return
		}
	}

	numResults := 0

	for page := 0; page < pages; page++ {
		if ctx.Err() != nil {
			return
		}

		reqURL := config.GetUrl(INDEX_SERVER, page)

		response, err := common.GetContext(ctx, reqURL, wb.MaxTimeout, wb.MaxRetries)
		if err != nil {
			if !common.SendError(ctx, errors, fmt.Errorf("[FetchPages] Request error: %w", err)) {
				return
			}
			continue
		}

		// Only parse non-empty 200 bodies; an empty body means no results.
		if len(response) == 0 {
			continue
		}

		parsedResponse, err := wb.ParseResponse(response)
		if err != nil {
			if !common.SendError(ctx, errors, fmt.Errorf("[FetchPages] Cannot parse response: %w", err)) {
				return
			}
			continue
		}

		if len(parsedResponse) == 0 {
			continue
		}
		numResults += len(parsedResponse)

		if !common.SendResults(ctx, results, parsedResponse) {
			return
		}

		if config.Limit != 0 && uint(numResults) >= config.Limit {
			return
		}
	}
}

// Download file from WebArchive using a link from CDX response
func (wb *Wayback) GetFile(page *common.CdxResponse) ([]byte, error) {
	requestURI := fmt.Sprintf("%v/%vid_/%v", CRAWL_STORAGE, page.Timestamp, page.Original)
	response, err := common.Get(requestURI, wb.MaxTimeout, wb.MaxRetries)
	if err != nil {
		return nil, fmt.Errorf("[GetFile] Request error: %v", err)
	}
	return response, nil
}
