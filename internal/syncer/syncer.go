package syncer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/xylandev/xsync-clinet/internal/config"
)

var ErrConflict = errors.New("destination path already contains different content")

type Claim struct {
	LeaseID    string    `json:"lease_id"`
	ObjectID   string    `json:"object_id"`
	Tenant     string    `json:"tenant"`
	ClientID   string    `json:"client_id"`
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	SHA256     string    `json:"sha256"`
	Version    uint64    `json:"version"`
	LeaseUntil time.Time `json:"lease_until"`
}
type Client struct {
	cfg     config.Config
	http    *http.Client
	log     *slog.Logger
	buffers sync.Pool
}

func New(cfg config.Config, log *slog.Logger) (*Client, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAFile != "" || cfg.CACertificate != "" {
		roots := x509.NewCertPool()
		if cfg.CACertificate != "" && !roots.AppendCertsFromPEM([]byte(cfg.CACertificate)) {
			return nil, errors.New("embedded CA contains no certificates")
		}
		if cfg.CAFile != "" {
			raw, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, err
			}
			if !roots.AppendCertsFromPEM(raw) {
				return nil, errors.New("CA file contains no certificates")
			}
		}
		tlsCfg.RootCAs = roots
	} else {
		want := strings.ToLower(strings.ReplaceAll(cfg.TLSFingerprint, ":", ""))
		tlsCfg.InsecureSkipVerify = true
		tlsCfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("server sent no certificate")
			}
			sum := sha256.Sum256(cs.PeerCertificates[0].Raw)
			if !strings.EqualFold(hex.EncodeToString(sum[:]), want) {
				return errors.New("TLS certificate fingerprint mismatch")
			}
			return nil
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg
	transport.ForceAttemptHTTP2 = true
	transport.MaxIdleConns = cfg.Concurrency * 2
	transport.MaxIdleConnsPerHost = cfg.Concurrency
	transport.MaxConnsPerHost = cfg.Concurrency
	transport.IdleConnTimeout = 90 * time.Second
	transport.DisableCompression = true
	client := &Client{cfg: cfg, http: &http.Client{Transport: transport, Timeout: 0}, log: log}
	client.buffers.New = func() any { return make([]byte, cfg.BufferSize) }
	return client, nil
}

func (c *Client) Run(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Join(c.cfg.Destination, ".xsync", "partial"), 0o750); err != nil {
		return err
	}
	var wg sync.WaitGroup
	errCh := make(chan error, c.cfg.Concurrency)
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			if err := c.worker(ctx, worker); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		<-done
		return nil
	case err := <-errCh:
		return err
	case <-done:
		return nil
	}
}
func (c *Client) worker(ctx context.Context, worker int) error {
	for {
		claim, found, err := c.claim(ctx)
		if err != nil {
			c.log.Warn("claim failed", "worker", worker, "error", err)
			if !wait(ctx, c.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		}
		if !found {
			if !wait(ctx, c.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		}
		c.log.Info("claimed object", "worker", worker, "object_id", claim.ObjectID, "path", claim.Path, "size", claim.Size)
		if err = c.process(ctx, claim); err != nil {
			c.log.Error("sync failed", "object_id", claim.ObjectID, "path", claim.Path, "error", err)
			releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = c.release(releaseCtx, claim.LeaseID)
			cancel()
			backoff := c.cfg.PollInterval
			if errors.Is(err, ErrConflict) && backoff < time.Minute {
				backoff = time.Minute
			}
			if !wait(ctx, backoff) {
				return ctx.Err()
			}
		}
	}
}

func (c *Client) claim(ctx context.Context) (Claim, bool, error) {
	body, _ := json.Marshal(map[string]any{"client_id": c.cfg.ClientID, "prefix": c.cfg.Prefix, "lease_seconds": int(c.cfg.LeaseTTL.Seconds())})
	req, err := c.request(ctx, http.MethodPost, "/v1/claims", bytes.NewReader(body))
	if err != nil {
		return Claim{}, false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Claim{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return Claim{}, false, nil
	}
	if resp.StatusCode != http.StatusCreated {
		return Claim{}, false, responseError(resp)
	}
	var claim Claim
	if err = json.NewDecoder(resp.Body).Decode(&claim); err != nil {
		return Claim{}, false, err
	}
	return claim, true, nil
}
func (c *Client) process(ctx context.Context, claim Claim) error {
	stopRenew := make(chan struct{})
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(c.cfg.LeaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenew:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if e := c.renew(ctx, claim.LeaseID); e != nil {
					c.log.Warn("lease renewal failed", "object_id", claim.ObjectID, "error", e)
				}
			}
		}
	}()
	defer func() { close(stopRenew); <-renewDone }()
	final, err := safeDestination(c.cfg.Destination, claim.Path)
	if err != nil {
		return err
	}
	if existing, err := os.Stat(final); err == nil && existing.IsDir() {
		return ErrConflict
	} else if err == nil {
		digest, size, e := hashFile(final)
		if e != nil {
			return e
		}
		if size == claim.Size && strings.EqualFold(digest, claim.SHA256) {
			return c.commit(ctx, claim)
		}
		return ErrConflict
	}
	partial := filepath.Join(c.cfg.Destination, ".xsync", "partial", claim.ObjectID+".partial")
	sidecar := partial + ".json"
	offset := int64(0)
	partialExists := false
	if st, e := os.Stat(partial); e == nil {
		partialExists = true
		offset = st.Size()
		if offset > claim.Size {
			_ = os.Remove(partial)
			offset = 0
			partialExists = false
		}
	}
	remaining := claim.Size - offset
	if remaining < 0 {
		remaining = claim.Size
	}
	if err = ensureSpace(c.cfg.Destination, uint64(remaining), c.cfg.ReserveBytes); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(final), 0o750); err != nil {
		return err
	}
	var digest string
	var size int64
	if !partialExists || offset < claim.Size {
		if digest, size, err = c.download(ctx, claim, partial, sidecar, offset); err != nil {
			return err
		}
	} else {
		digest, size, err = hashFile(partial)
		if err != nil {
			return err
		}
	}
	if size != claim.Size || !strings.EqualFold(digest, claim.SHA256) {
		_ = os.Remove(partial)
		_ = os.Remove(sidecar)
		return fmt.Errorf("checksum mismatch: got size=%d sha256=%s", size, digest)
	}
	f, err := os.OpenFile(partial, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	_ = f.Close()
	if err != nil {
		return err
	}
	if _, err = os.Stat(final); err == nil {
		return ErrConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = os.Link(partial, final); err != nil {
		return err
	}
	if err = syncDir(filepath.Dir(final)); err != nil {
		return err
	}
	if err = c.commit(ctx, claim); err != nil {
		return err
	}
	_ = os.Remove(partial)
	_ = os.Remove(sidecar)
	c.log.Info("object synchronized", "object_id", claim.ObjectID, "path", claim.Path, "size", claim.Size)
	return nil
}

func (c *Client) download(ctx context.Context, claim Claim, partial, sidecar string, offset int64) (string, int64, error) {
	hasher := sha256.New()
	if offset > 0 {
		prefix, err := os.Open(partial)
		if err != nil {
			return "", 0, err
		}
		n, hashErr := io.CopyN(hasher, prefix, offset)
		closeErr := prefix.Close()
		if hashErr != nil {
			return "", 0, hashErr
		}
		if closeErr != nil {
			return "", 0, closeErr
		}
		if n != offset {
			return "", 0, io.ErrUnexpectedEOF
		}
	}
	req, err := c.request(ctx, http.MethodGet, "/v1/objects/"+claim.ObjectID+"/content", nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("X-Xsync-Lease-ID", claim.LeaseID)
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	if offset > 0 && resp.StatusCode == http.StatusOK {
		offset = 0
		hasher.Reset()
		_ = os.Remove(partial)
	} else if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return "", 0, responseError(resp)
	}
	flags := os.O_CREATE | os.O_WRONLY
	if offset == 0 {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(partial, flags, 0o600)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return "", 0, err
	}
	buf := c.buffers.Get().([]byte)
	defer c.buffers.Put(buf)
	written := offset
	lastSync := time.Now()
	for {
		n, re := resp.Body.Read(buf)
		if n > 0 {
			wn, we := f.Write(buf[:n])
			written += int64(wn)
			if wn > 0 {
				_, _ = hasher.Write(buf[:wn])
			}
			if we != nil {
				return "", 0, we
			}
			if wn != n {
				return "", 0, io.ErrShortWrite
			}
			if written%(256<<20) < int64(n) || time.Since(lastSync) >= 5*time.Second {
				if err = f.Sync(); err != nil {
					return "", 0, err
				}
				_ = writeSidecar(sidecar, claim, written)
				lastSync = time.Now()
			}
		}
		if re == io.EOF {
			break
		}
		if re != nil {
			return "", 0, re
		}
	}
	if err = f.Sync(); err != nil {
		return "", 0, err
	}
	if err = writeSidecar(sidecar, claim, written); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hasher.Sum(nil)), written, nil
}

func (c *Client) renew(ctx context.Context, lease string) error {
	body, _ := json.Marshal(map[string]int{"lease_seconds": int(c.cfg.LeaseTTL.Seconds())})
	req, err := c.request(ctx, http.MethodPost, "/v1/claims/"+lease+"/renew", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	return nil
}
func (c *Client) release(ctx context.Context, lease string) error {
	req, err := c.request(ctx, http.MethodDelete, "/v1/claims/"+lease, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return responseError(resp)
	}
	return nil
}
func (c *Client) commit(ctx context.Context, claim Claim) error {
	body, _ := json.Marshal(map[string]any{"lease_id": claim.LeaseID, "sha256": claim.SHA256, "size": claim.Size})
	req, err := c.request(ctx, http.MethodPost, "/v1/objects/"+claim.ObjectID+"/commit", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return responseError(resp)
	}
	return nil
}
func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.cfg.Endpoint, "/")+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func safeDestination(root, name string) (string, error) {
	name = strings.ReplaceAll(name, "\\", "/")
	clean := filepath.Clean(filepath.FromSlash(name))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("unsafe remote path")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	target := filepath.Join(rootAbs, clean)
	if target != rootAbs && !strings.HasPrefix(target, rootAbs+string(filepath.Separator)) {
		return "", errors.New("remote path escapes destination")
	}
	return target, nil
}
func hashFile(name string) (string, int64, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(h, f, make([]byte, 1<<20))
	return hex.EncodeToString(h.Sum(nil)), n, err
}
func ensureSpace(path string, need, reserve uint64) error {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return err
	}
	available := uint64(st.Bavail) * uint64(st.Bsize)
	if available < need+reserve {
		return fmt.Errorf("insufficient destination space: available=%d need=%d reserve=%d", available, need, reserve)
	}
	return nil
}
func syncDir(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func writeSidecar(name string, claim Claim, offset int64) error {
	raw, _ := json.Marshal(map[string]any{"object_id": claim.ObjectID, "path": claim.Path, "size": claim.Size, "sha256": claim.SHA256, "offset": offset})
	tmp := name + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, name)
}
func responseError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(raw)))
}
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
