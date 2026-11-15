package common

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/corpix/uarand"
)

// Doer performs HTTP requests. *http.Client implements it, and tests can
// substitute custom transports.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client is an injectable HTTP client with retries, backoff and context
// support. The zero-value-friendly defaults produced by NewClient match the
// previous hardcoded behavior of the package-level Get/DoRequest helpers.
type Client struct {
	Transport  Doer          // Underlying transport, defaults to a *http.Client with Timeout
	Timeout    time.Duration // Per-request timeout used by the default transport
	MaxRetries int           // Total attempts per request (minimum 1)
	// Backoff returns how long to wait before the next attempt (attempt starts at 1).
	// Defaults to attempt * time.Second.
	Backoff func(attempt int) time.Duration
	// UserAgent sets the User-Agent header. Defaults to a random browser UA.
	UserAgent string
}

// NewClient creates a Client with the production default transport.
func NewClient(timeout, retries int) *Client {
	t := time.Duration(timeout) * time.Second
	return &Client{
		Timeout:    t,
		MaxRetries: retries,
		Transport:  &http.Client{Timeout: t},
	}
}

// retryableStatuses are HTTP statuses worth retrying (e.g. 429 rate limiting).
var retryableStatuses = map[int]bool{
	http.StatusTooManyRequests:     true,
	http.StatusInternalServerError: true,
	http.StatusBadGateway:          true,
	http.StatusServiceUnavailable:  true,
	http.StatusGatewayTimeout:      true,
}

// Get performs a GET request and returns the response body.
func (c *Client) Get(ctx context.Context, url string) ([]byte, error) {
	return c.Do(ctx, url, nil)
}

// Do performs a GET request with additional headers and returns the response
// body. Retries are performed on network errors and retryable statuses
// (429/5xx); the request aborts early when ctx is cancelled.
func (c *Client) Do(ctx context.Context, url string, headers map[string]string) ([]byte, error) {
	attempts := c.MaxRetries
	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		log.Printf("GET [t=%v] [r=%v]: %v", c.Timeout, c.MaxRetries, url)
		body, status, err := c.doOnce(ctx, url, headers)
		if err == nil {
			return body, nil
		}
		lastErr = err
		log.Printf("Attempt %d failed: %v", attempt+1, err)

		// Don't retry client errors like 400/404
		if status != 0 && !retryableStatuses[status] {
			break
		}

		if attempt+1 < attempts {
			if err := c.sleep(ctx, attempt+1); err != nil {
				return nil, err
			}
		}
	}

	return nil, lastErr
}

func (c *Client) doOnce(ctx context.Context, url string, headers map[string]string) ([]byte, int, error) {
	transport := c.Transport
	if transport == nil {
		transport = &http.Client{Timeout: c.Timeout}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("[Client] Cannot build request: %w", err)
	}

	ua := c.UserAgent
	if ua == "" {
		ua = uarand.GetRandom()
	}
	req.Header.Set("User-Agent", ua)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := transport.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("[Client] Request error: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("[Client] Cannot read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("[Client] Got %v status response", resp.StatusCode)
	}

	if len(body) == 0 {
		return nil, resp.StatusCode, fmt.Errorf("[Client] Response body is empty")
	}

	return body, resp.StatusCode, nil
}

func (c *Client) sleep(ctx context.Context, attempt int) error {
	d := time.Duration(attempt) * time.Second
	if c.Backoff != nil {
		d = c.Backoff(attempt)
	}
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
