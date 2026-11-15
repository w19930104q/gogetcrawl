package commoncrawl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	common "github.com/karust/gogetcrawl/common"
	"github.com/slyrz/warc"
)

const (
	INDEX_SERVER  = "https://index.commoncrawl.org/"
	CRAWL_STORAGE = "https://data.commoncrawl.org/" // https://commoncrawl.s3.amazonaws.com/
)

// CustomTime is a wrapper for time.Time to implement custom JSON unmarshaling
type CustomTime time.Time

// UnmarshalJSON implements json.Unmarshaler interface
func (ct *CustomTime) UnmarshalJSON(b []byte) error {
	s := string(b)

	// Remove any surrounding quotes
	s = strings.Trim(s, "\"")

	t, err := time.Parse("2006-01-02T15:04:05", s)
	if err != nil {
		return err
	}
	*ct = CustomTime(t)
	return nil
}

// JSON response containing latest index name at http://index.commoncrawl.org/collinfo.json
type latestIndex struct {
	Id       string     `json:"id"`
	Name     string     `json:"name"`
	Timegate string     `json:"timegate"`
	CdxAPI   string     `json:"cdx-api"`
	From     CustomTime `json:"from"`
	To       CustomTime `json:"to"`
}

// ex: http://index.commoncrawl.org/CC-MAIN-2015-11-index?url=*.wikipedia.org/&showNumPages=true
type numPagesResponse struct {
	Pages    int `json:"pages"`
	PageSize int `json:"pageSize"`
	Blocks   int `json:"blocks"`
}

type CommonCrawl struct {
	MaxTimeout int // Request timeout
	MaxRetries int // Max number of request retries if timeouted

	// IndexServer is the CDX index API base URL. Defaults to INDEX_SERVER.
	IndexServer string
	// StorageServer is the WARC storage base URL. Defaults to CRAWL_STORAGE.
	StorageServer string
	// Client is the HTTP client used for all requests. Defaults to a client
	// built from MaxTimeout/MaxRetries.
	Client *common.Client

	indexes []latestIndex // CDX Indexes versions cache
}

func New(timeout, retries int) (*CommonCrawl, error) {
	source := &CommonCrawl{
		MaxTimeout: timeout,
		MaxRetries: retries,
		Client:     common.NewClient(timeout, retries),
	}

	var err error
	source.indexes, err = source.GetIndexes(context.Background())
	if err != nil {
		log.Printf("Error fetching indexes: %v", err)
		return nil, err
	}

	return source, nil
}

func (CommonCrawl) Name() string {
	return "CommonCrawl"
}

func (cc *CommonCrawl) http() *common.Client {
	if cc.Client != nil {
		return cc.Client
	}
	return common.NewClient(cc.MaxTimeout, cc.MaxRetries)
}

func (cc *CommonCrawl) indexServer() string {
	if cc.IndexServer != "" {
		return cc.IndexServer
	}
	return INDEX_SERVER
}

func (cc *CommonCrawl) storageServer() string {
	if cc.StorageServer != "" {
		return cc.StorageServer
	}
	return CRAWL_STORAGE
}

// Get latest CDX indexes from http://index.commoncrawl.org/collinfo.json
func (cc *CommonCrawl) GetIndexes(ctx context.Context) ([]latestIndex, error) {
	response, err := cc.http().Get(ctx, cc.indexServer()+"collinfo.json")
	if err != nil {
		return nil, fmt.Errorf("[GetIndexes] response read error: %v", err)
	}

	latestIndexes := []latestIndex{}
	err = jsoniter.Unmarshal(response, &latestIndexes)
	if err != nil {
		log.Printf("JSON Unmarshal error: %v", err)
		log.Printf("Response content: %s", string(response))
		return latestIndexes, fmt.Errorf("[GetIndexes] Cannot get latest index ID: %v", err)
	}

	return latestIndexes, nil
}

// Returns the number of pages located in CommonCrawl for given url
//
//	index: needs to be set manually here like "CC-MAIN-2023-14"
func (cc *CommonCrawl) GetNumPagesIndex(ctx context.Context, url, index string) (int, error) {
	requestURI := fmt.Sprintf("%v%v-index?url=%v&showNumPages=true", cc.indexServer(), index, url)

	response, err := cc.http().Get(ctx, requestURI)
	if err != nil {
		return 0, fmt.Errorf("[GetNumPagesIndex] Request error: %v", err)
	}

	numPagesResp := numPagesResponse{}
	err = jsoniter.Unmarshal(response, &numPagesResp)
	if err != nil {
		return 0, fmt.Errorf("[GetNumPagesIndex] JSON decode error: %v", err)
	}

	return numPagesResp.Pages, nil
}

// Returns the number of pages located in CommonCrawl for given url
// Use latest index from http://index.commoncrawl.org/collinfo.json
func (cc *CommonCrawl) GetNumPages(ctx context.Context, url string) (int, error) {
	return cc.GetNumPagesIndex(ctx, url, cc.indexes[0].Id)
}

// Parse response from http://index.commoncrawl.org/[Index Version]-index index server
func (cc *CommonCrawl) ParseResponse(resp []byte) ([]*common.CdxResponse, error) {
	pages := []*common.CdxResponse{}

	trimmed := bytes.TrimRight(resp, "\r\n\t ")
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("Empty response provided")
	}

	// Parse the response that contains JSON objects separated with new line
	for _, line := range bytes.Split(trimmed, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var indexVal common.CdxResponse
		if err := jsoniter.Unmarshal(line, &indexVal); err != nil {
			return nil, fmt.Errorf("[ParseResponse] Cannot decode JSON line: %w. Response: %v", err, string(line))
		}
		indexVal.Source = cc
		pages = append(pages, &indexVal)
	}

	return pages, nil
}

// fetchPage requests and parses a single result page of an index.
func (cc *CommonCrawl) fetchPage(ctx context.Context, config common.RequestConfig, indexURL string, page int) ([]*common.CdxResponse, error) {
	reqURL := config.GetUrl(indexURL, page)

	response, err := cc.http().Get(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("Request error: %w", err)
	}

	parsedResponse, err := cc.ParseResponse(response)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse response: %w", err)
	}

	return parsedResponse, nil
}

// GetPagesIndex ... Makes request to WebArchive index API to gather all url observations
//
//	index: needs to be set manually here like "CC-MAIN-2023-14"
//
// A broken page (request or parse failure) is skipped and its error
// aggregated, so it doesn't pollute results from other pages.
func (cc *CommonCrawl) GetPagesIndex(ctx context.Context, config common.RequestConfig, index string) ([]*common.CdxResponse, error) {
	pages := 1
	if !config.SinglePage {
		var err error
		pages, err = cc.GetNumPagesIndex(ctx, config.URL, index)
		if err != nil {
			return nil, err
		}
	}

	var results []*common.CdxResponse
	var errList []error
	numResults := 0

	indexURL := fmt.Sprintf("%v%v-index", cc.indexServer(), index)
	for page := 0; page < pages; page++ {
		parsedResponse, err := cc.fetchPage(ctx, config, indexURL, page)
		if err != nil {
			errList = append(errList, fmt.Errorf("[GetPagesIndex] page %d: %w", page, err))
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

// Makes request to the Commoncrawl index API to gather all offsets that contain chosen URL.
//
//	Uses the latest CommonCrawl index.
func (cc *CommonCrawl) GetPages(ctx context.Context, config common.RequestConfig) ([]*common.CdxResponse, error) {
	return cc.GetPagesIndex(ctx, config, cc.indexes[0].Id)
}

// FetchPages is a concurrent way to GetPages.
// Makes request to CommonCrawl index API and returns observations in a channel.
//
// Broken pages are reported on the errors channel and skipped without
// polluting other results. The limit applies across all filtered indexes.
// The goroutine exits when done or when ctx is cancelled.
func (cc *CommonCrawl) FetchPages(ctx context.Context, config common.RequestConfig, results chan []*common.CdxResponse, errs chan error) {
	numResults := 0

	for _, idx := range cc.filterIndices(config) {
		pages := 1
		if !config.SinglePage {
			var err error
			pages, err = cc.GetNumPagesIndex(ctx, config.URL, idx)
			if err != nil {
				if !common.SendError(ctx, errs, fmt.Errorf("[FetchPages] Cannot get number of pages: %w", err)) {
					return
				}
				continue
			}
		}

		indexURL := fmt.Sprintf("%v%v-index", cc.indexServer(), idx)
		for page := 0; page < pages; page++ {
			parsedResponse, err := cc.fetchPage(ctx, config, indexURL, page)
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
}

// Get indices that match the filter date criteria
func (cc *CommonCrawl) filterIndices(config common.RequestConfig) []string {
	// no date filter, just use the first index
	if config.FromDate.IsZero() && config.ToDate.IsZero() {
		return []string{cc.indexes[0].Id}
	}

	indices := []string{}
	for _, idx := range cc.indexes {
		if !config.FromDate.IsZero() && config.FromDate.After(time.Time(idx.From)) {
			continue
		}
		if !config.ToDate.IsZero() && config.ToDate.Before(time.Time(idx.To)) {
			continue
		}
		indices = append(indices, idx.Id)
	}
	log.Printf("Filtered indices: %v", indices)
	return indices
}

// Gets files from CommonCrawl storage using info from CdxResponse server
//
//	page: info about found web page in CdxResponse
func (cc *CommonCrawl) GetFile(ctx context.Context, page *common.CdxResponse) ([]byte, error) {
	offset, _ := strconv.Atoi(page.Offset)
	length, _ := strconv.Atoi(page.Length)
	offsetEnd := offset + length + 1

	headers := map[string]string{
		"Range": fmt.Sprintf("bytes=%v-%v", page.Offset, offsetEnd),
	}
	resp, err := cc.http().Do(ctx, cc.storageServer()+page.Filename, headers)
	if err != nil {
		return nil, fmt.Errorf("[GetFile] Request error: %v", err)
	}

	reader, err := warc.NewReader(bytes.NewReader(resp))
	if err != nil {
		return nil, fmt.Errorf("[GetFile] Cannot decode WARC: %v", err)
	}
	defer reader.Close()

	record, err := reader.ReadRecord()
	if err != nil {
		return nil, fmt.Errorf("[GetFile] Cannot decode WARC: %v", err)
	}

	return io.ReadAll(record.Content)
}
