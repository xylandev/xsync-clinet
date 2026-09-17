package syncer

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xylandev/xsync-clinet/internal/config"
)

func TestSafeDestination(t *testing.T) {
	root := t.TempDir()
	if _, err := safeDestination(root, "dir/file"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../file", "a/../../file", "/absolute"} {
		if _, err := safeDestination(root, name); err == nil {
			t.Errorf("unsafe path %q accepted", name)
		}
	}
}

func TestProcessResumeVerifyAndCommit(t *testing.T) {
	payload := []byte("hello resumable world")
	sum := sha256.Sum256(payload)
	var committed atomic.Bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/content"):
			start := 0
			if value := r.Header.Get("Range"); value != "" {
				_, _ = fmt.Sscanf(value, "bytes=%d-", &start)
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(payload)-1, len(payload)))
				w.WriteHeader(http.StatusPartialContent)
			}
			_, _ = w.Write(payload[start:])
		case strings.HasSuffix(r.URL.Path, "/commit"):
			committed.Store(true)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/renew"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"lease_id":"lease"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	cert := srv.Certificate()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := x509.ParseCertificate(cert.Raw); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	cfg := config.Default()
	cfg.Endpoint = srv.URL
	cfg.APIKey = "key"
	cfg.CAFile = caFile
	cfg.Destination = dest
	cfg.ReserveBytes = 0
	cfg.LeaseTTL = 30 * time.Second
	client, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	claim := Claim{LeaseID: "lease", ObjectID: "object", Path: "dir/file.txt", Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}
	partialDir := filepath.Join(dest, ".xsync", "partial")
	if err = os.MkdirAll(partialDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(partialDir, "object.partial"), payload[:6], 0o600); err != nil {
		t.Fatal(err)
	}
	if err = client.process(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dest, "dir", "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(payload) {
		t.Fatalf("got %q", raw)
	}
	if !committed.Load() {
		t.Fatal("server commit was not called")
	}
}

func TestProcessFinalizesCompletePartialWithoutRangeRequest(t *testing.T) {
	payload := []byte("complete partial")
	sum := sha256.Sum256(payload)
	var contentRequests atomic.Int32
	var committed atomic.Bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/content"):
			contentRequests.Add(1)
			http.Error(w, "unexpected content request", http.StatusRequestedRangeNotSatisfiable)
		case strings.HasSuffix(r.URL.Path, "/commit"):
			committed.Store(true)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/renew"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"lease_id":"lease"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	cfg := config.Default()
	cfg.Endpoint = srv.URL
	cfg.APIKey = "key"
	cfg.CAFile = caFile
	cfg.Destination = dest
	cfg.ReserveBytes = 0
	cfg.LeaseTTL = 30 * time.Second
	client, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	claim := Claim{LeaseID: "lease", ObjectID: "object", Path: "file.txt", Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}
	partialDir := filepath.Join(dest, ".xsync", "partial")
	if err = os.MkdirAll(partialDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(partialDir, "object.partial"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = client.process(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if contentRequests.Load() != 0 {
		t.Fatalf("made %d content requests for a complete partial", contentRequests.Load())
	}
	if !committed.Load() {
		t.Fatal("server commit was not called")
	}
	got, err := os.ReadFile(filepath.Join(dest, "file.txt"))
	if err != nil || string(got) != string(payload) {
		t.Fatalf("final file = %q, err=%v", got, err)
	}
}
