package common

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func shrinkRetryInterval(t *testing.T) {
	t.Helper()
	old := RetryInterval
	RetryInterval = time.Millisecond
	t.Cleanup(func() { RetryInterval = old })
}

func TestGetRetriesTransientThenSucceeds(t *testing.T) {
	shrinkRetryInterval(t)

	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	body, err := GetContext(context.Background(), srv.URL, 5, 5)
	if err != nil {
		t.Fatalf("GetContext failed: %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("Unexpected body: %q", body)
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("Expected 3 attempts, got %v", got)
	}
}

func TestGetPreservesErrorCategory(t *testing.T) {
	shrinkRetryInterval(t)

	cases := []struct {
		status int
		want   error
	}{
		{http.StatusTooManyRequests, Status429Error},
		{http.StatusInternalServerError, Status500Error},
		{http.StatusServiceUnavailable, Status503Error},
	}

	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			var attempts int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&attempts, 1)
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			_, err := GetContext(context.Background(), srv.URL, 5, 3)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Expected error category %v, got %v", tc.want, err)
			}
			if got := atomic.LoadInt32(&attempts); got != 3 {
				t.Fatalf("Expected 3 attempts, got %v", got)
			}
		})
	}
}

func TestGetEmptyBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	body, err := GetContext(context.Background(), srv.URL, 5, 2)
	if err != nil {
		t.Fatalf("Empty 200 body must not be an error, got: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("Expected empty body, got %q", body)
	}
}

func TestGetNonRetryableStatus(t *testing.T) {
	shrinkRetryInterval(t)

	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	_, err := GetContext(context.Background(), srv.URL, 5, 5)
	if err == nil {
		t.Fatal("Expected error for 400 status")
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("Non-retryable status must not be retried, got %v attempts", got)
	}
}

func TestGetContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // block until the client goes away
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := GetContext(ctx, srv.URL, 30, 5)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Cancellation took too long: %v", elapsed)
	}
}

func TestGetContextCancelDuringBackoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := GetContext(ctx, srv.URL, 30, 5) // backoff would take 1+2+3+4s without cancel
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Cancellation during backoff took too long: %v", elapsed)
	}
}
