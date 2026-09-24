// Package syncer downloads objects from an xsync server: it claims them,
// fetches them under a lease with resumable writes, verifies the SHA-256,
// places them atomically and commits the delivery.
package syncer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xylandev/xsync-clinet/internal/config"
)

var (
	// ErrConflict means the destination holds different content and the
	// conflict policy does not allow replacing it.
	ErrConflict = errors.New("destination path already contains different content")
	// errLeaseLost means the server no longer considers this client the
	// holder of the object; the work must stop without committing.
	errLeaseLost = errors.New("lease lost")
	// errSymlink means a destination path would pass through a symbolic link.
	errSymlink = errors.New("destination path goes through a symbolic link")
)

// permanentError marks failures that retrying cannot fix, such as an unsafe
// path or a conflict the policy refuses to resolve. The server parks such
// objects for an operator instead of redelivering them forever.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func permanent(err error) error { return permanentError{err} }

type Claim struct {
	LeaseID      string    `json:"lease_id"`
	ObjectID     string    `json:"object_id"`
	Tenant       string    `json:"tenant"`
	ClientID     string    `json:"client_id"`
	Path         string    `json:"path"`
	Size         int64     `json:"size"`
	SHA256       string    `json:"sha256"`
	Version      uint64    `json:"version"`
	Attempts     int       `json:"attempts"`
	LeaseUntil   time.Time `json:"lease_until"`
	LeaseSeconds int       `json:"lease_seconds"`
}

type Client struct {
	cfg     config.Config
	http    *http.Client
	log     *slog.Logger
	buffers sync.Pool

	inflight sync.Map
	spaceMu  sync.Mutex
	reserved uint64
	// longPoll is cleared when the server answers an empty claim at once,
	// which means it predates long polling.
	longPoll atomic.Bool
}

func New(cfg config.Config, log *slog.Logger) (*Client, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	// Every destination check compares absolute paths.
	abs, err := filepath.Abs(cfg.Destination)
	if err != nil {
		return nil, err
	}
	cfg.Destination = abs
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
	// One TCP connection per transfer: HTTP/2 would multiplex every worker
	// over a single connection and its congestion window.
	transport.ForceAttemptHTTP2 = false
	transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	transport.MaxIdleConns = cfg.Concurrency*2 + 4
	transport.MaxIdleConnsPerHost = cfg.Concurrency*2 + 4
	transport.MaxConnsPerHost = 0
	transport.IdleConnTimeout = 90 * time.Second
	transport.ResponseHeaderTimeout = cfg.LongPoll + cfg.RequestTimeout
	transport.DisableCompression = true
	c := &Client{cfg: cfg, http: &http.Client{Transport: transport}, log: log}
	c.buffers.New = func() any { return make([]byte, cfg.BufferSize) }
	c.longPoll.Store(cfg.LongPoll > 0)
	return c, nil
}

func (c *Client) stateDir(parts ...string) string {
	return filepath.Join(append([]string{c.cfg.Destination, ".xsync"}, parts...)...)
}

// Run claims and delivers objects until ctx ends. On shutdown every lease
// still held is handed back without counting as a failed delivery.
func (c *Client) Run(ctx context.Context) error {
	for _, d := range []string{"partial", "meta", "conflicts"} {
		if err := os.MkdirAll(c.stateDir(d), 0o750); err != nil {
			return err
		}
	}
	c.collectPartials()
	slots := make(chan struct{}, c.cfg.Concurrency)
	var workers sync.WaitGroup
	defer workers.Wait()
	gc := time.NewTicker(time.Hour)
	defer gc.Stop()
	backoff := c.cfg.PollInterval
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-gc.C:
			c.collectPartials()
		case slots <- struct{}{}:
		}
		// Claim as many objects as there are free workers.
		free := 1
		for free < c.cfg.MaxBatch {
			select {
			case slots <- struct{}{}:
				free++
				continue
			default:
			}
			break
		}
		start := time.Now()
		claims, err := c.claim(ctx, free)
		for i := len(claims); i < free; i++ {
			<-slots
		}
		if ctx.Err() != nil {
			for _, cl := range claims {
				c.release(context.Background(), cl, releaseRequest{Reason: "client shutting down", CountAttempt: ptr(false)})
			}
			return nil
		}
		if err != nil {
			c.log.Warn("claim failed", "error", err, "retry_in", backoff)
			if !wait(ctx, backoff) {
				return nil
			}
			backoff = min(backoff*2, time.Minute)
			continue
		}
		backoff = c.cfg.PollInterval
		if len(claims) == 0 {
			// A long poll already waited on the server; an older server
			// answers at once and needs a client-side pause.
			if !c.longPoll.Load() || time.Since(start) < c.cfg.LongPoll/2 {
				if !wait(ctx, c.cfg.PollInterval) {
					return nil
				}
			}
			continue
		}
		for _, cl := range claims {
			workers.Add(1)
			go func(cl Claim) {
				defer workers.Done()
				defer func() { <-slots }()
				c.deliver(ctx, cl)
			}(cl)
		}
	}
}

func ptr[T any](v T) *T { return &v }

// deliver runs one claim to its end and reports the outcome to the server.
func (c *Client) deliver(ctx context.Context, claim Claim) {
	if _, busy := c.inflight.LoadOrStore(claim.ObjectID, struct{}{}); busy {
		// Another worker of this process holds the same object under an
		// older lease; let it finish.
		c.release(context.Background(), claim, releaseRequest{Reason: "object already in progress on this client", RetryAfterSeconds: 60, CountAttempt: ptr(false)})
		return
	}
	defer c.inflight.Delete(claim.ObjectID)
	c.log.Info("claimed object", "object_id", claim.ObjectID, "path", claim.Path, "size", claim.Size, "attempt", claim.Attempts)
	leaseCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := c.keepLease(leaseCtx, claim, cancel)
	err := c.process(leaseCtx, claim)
	stop()
	switch {
	case err == nil:
		return
	case errors.Is(context.Cause(leaseCtx), errLeaseLost), errors.Is(err, errLeaseLost):
		c.log.Warn("lease lost; object will be redelivered", "object_id", claim.ObjectID, "path", claim.Path)
	case ctx.Err() != nil:
		c.release(context.Background(), claim, releaseRequest{Reason: "client shutting down", CountAttempt: ptr(false)})
	default:
		var perm permanentError
		req := releaseRequest{Reason: err.Error()}
		if errors.As(err, &perm) {
			req.Permanent = true
			c.log.Error("delivery failed permanently; the server parks the object", "object_id", claim.ObjectID, "path", claim.Path, "error", err)
		} else {
			req.RetryAfterSeconds = int(retryDelay(claim.Attempts) / time.Second)
			c.log.Error("delivery failed; will retry", "object_id", claim.ObjectID, "path", claim.Path, "error", err, "retry_after", retryDelay(claim.Attempts))
		}
		c.release(context.Background(), claim, req)
	}
}

// retryDelay backs off exponentially with the number of attempts.
func retryDelay(attempts int) time.Duration {
	d := 10 * time.Second
	for i := 1; i < attempts && d < 10*time.Minute; i++ {
		d *= 2
	}
	return min(d, 10*time.Minute)
}

// keepLease renews the lease at a third of its remaining time. If the server
// says the lease is gone, the download is cancelled at once: continuing would
// race whoever holds the object now.
func (c *Client) keepLease(ctx context.Context, claim Claim, cancel context.CancelCauseFunc) func() {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		until := claim.LeaseUntil
		if until.IsZero() {
			until = time.Now().Add(c.cfg.LeaseTTL)
		}
		for {
			next := time.Until(until) / 3
			if next < time.Second {
				next = time.Second
			}
			t := time.NewTimer(next)
			select {
			case <-done:
				t.Stop()
				return
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			renewed, err := c.renew(ctx, claim.LeaseID)
			switch {
			case err == nil:
				until = renewed
			case errors.Is(err, errLeaseLost):
				cancel(errLeaseLost)
				return
			default:
				c.log.Warn("lease renewal failed", "object_id", claim.ObjectID, "error", err)
				if time.Now().After(until) {
					cancel(errLeaseLost)
					return
				}
			}
		}
	}()
	return func() { close(done); <-finished }
}

// deliveredMeta records what this client placed at a path, so a later
// delivery of the same path can tell a newer version from an older one.
type deliveredMeta struct {
	Path     string `json:"path"`
	ObjectID string `json:"object_id"`
	Version  uint64 `json:"version"`
	SHA256   string `json:"sha256"`
	Size     int64  `json:"size"`
}

func (c *Client) metaPath(p string) string {
	sum := sha256.Sum256([]byte(p))
	return c.stateDir("meta", hex.EncodeToString(sum[:16])+".json")
}

func (c *Client) readMeta(p string) *deliveredMeta {
	raw, err := os.ReadFile(c.metaPath(p))
	if err != nil {
		return nil
	}
	var m deliveredMeta
	if json.Unmarshal(raw, &m) != nil || m.Path != p {
		return nil
	}
	return &m
}

func (c *Client) writeMeta(claim Claim) error {
	raw, _ := json.Marshal(deliveredMeta{Path: claim.Path, ObjectID: claim.ObjectID, Version: claim.Version, SHA256: claim.SHA256, Size: claim.Size})
	return writeFileAtomic(c.metaPath(claim.Path), raw)
}

func (c *Client) process(ctx context.Context, claim Claim) error {
	final, err := safeDestination(c.cfg.Destination, claim.Path)
	if err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return err // an I/O failure while checking, not a bad path
		}
		return permanent(err)
	}
	partial := c.stateDir("partial", claim.ObjectID+".partial")
	sidecar := partial + ".json"
	if done, err := c.alreadyDelivered(ctx, claim, final, partial, sidecar); done || err != nil {
		return err
	}
	remaining := claim.Size
	if st, err := os.Stat(partial); err == nil && st.Size() <= claim.Size {
		remaining -= st.Size()
	}
	release, err := c.reserveSpace(uint64(remaining))
	if err != nil {
		return err
	}
	defer release()
	digest, size, err := c.download(ctx, claim, partial, sidecar)
	if err != nil {
		return err
	}
	if size != claim.Size || !strings.EqualFold(digest, claim.SHA256) {
		_ = os.Remove(partial)
		_ = os.Remove(sidecar)
		return fmt.Errorf("checksum mismatch: got size=%d sha256=%s", size, digest)
	}
	if err := os.Chmod(partial, c.cfg.FileMode); err != nil {
		return err
	}
	placed, err := c.place(claim, partial, final)
	if err != nil {
		return err
	}
	if placed {
		if err := c.writeMeta(claim); err != nil {
			return err
		}
	}
	// The final path is a hard link to the partial, so dropping the partial
	// now frees nothing twice and leaves no second name behind.
	_ = os.Remove(partial)
	_ = os.Remove(sidecar)
	if err := c.commit(ctx, claim); err != nil {
		return err
	}
	c.log.Info("object synchronized", "object_id", claim.ObjectID, "path", claim.Path, "size", claim.Size)
	return nil
}

// alreadyDelivered handles a destination that already exists before the
// download: the same content (a redelivery after a crash), a newer version
// this client delivered earlier (this claim is obsolete), or a conflict.
func (c *Client) alreadyDelivered(ctx context.Context, claim Claim, final, partial, sidecar string) (bool, error) {
	st, err := os.Lstat(final)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !st.Mode().IsRegular() {
		return false, permanent(fmt.Errorf("%w: destination %s is not a regular file", ErrConflict, claim.Path))
	}
	finish := func(reason string) (bool, error) {
		_ = os.Remove(partial)
		_ = os.Remove(sidecar)
		if err := c.commit(ctx, claim); err != nil {
			return true, err
		}
		c.log.Info(reason, "object_id", claim.ObjectID, "path", claim.Path)
		return true, nil
	}
	if meta := c.readMeta(claim.Path); meta != nil && meta.ObjectID != claim.ObjectID && meta.Version > claim.Version {
		return finish("skipped an older version; a newer one was already delivered")
	}
	if st.Size() == claim.Size {
		digest, _, err := hashFile(final)
		if err != nil {
			return false, err
		}
		if strings.EqualFold(digest, claim.SHA256) {
			if err := c.writeMeta(claim); err != nil {
				return false, err
			}
			return finish("destination already holds this object")
		}
	}
	if c.cfg.Conflict == config.ConflictSkip {
		return false, permanent(fmt.Errorf("%w: %s", ErrConflict, claim.Path))
	}
	// backup and overwrite resolve the conflict when the file is placed.
	return false, nil
}

// place links the verified partial into its final path. An existing file is
// replaced atomically, after being moved aside when the policy asks for a
// backup. It reports whether this client's content is now at the path.
func (c *Client) place(claim Claim, partial, final string) (bool, error) {
	if err := mkdirAllNoSymlinks(c.cfg.Destination, filepath.Dir(final), c.cfg.DirMode); err != nil {
		if errors.Is(err, errSymlink) {
			return false, permanent(err)
		}
		return false, err
	}
	dir := filepath.Dir(final)
	for attempt := 0; attempt < 3; attempt++ {
		err := os.Link(partial, final)
		if err == nil {
			return true, syncDir(dir)
		}
		if !errors.Is(err, os.ErrExist) {
			return false, err
		}
		existing, err := os.Lstat(final)
		if err != nil {
			continue
		}
		if !existing.Mode().IsRegular() {
			return false, permanent(fmt.Errorf("%w: destination %s is not a regular file", ErrConflict, claim.Path))
		}
		if meta := c.readMeta(claim.Path); meta != nil && meta.ObjectID != claim.ObjectID && meta.Version > claim.Version {
			return false, nil
		}
		switch c.cfg.Conflict {
		case config.ConflictSkip:
			return false, permanent(fmt.Errorf("%w: %s", ErrConflict, claim.Path))
		case config.ConflictBackup:
			if c.readMeta(claim.Path) == nil {
				// Not a file this client delivered: keep a copy.
				backup, err := c.backupPath(claim.Path)
				if err != nil {
					return false, err
				}
				if err := os.Rename(final, backup); err != nil && !errors.Is(err, os.ErrNotExist) {
					return false, err
				}
				c.log.Warn("moved conflicting file aside", "path", claim.Path, "backup", backup)
				continue
			}
		}
		// Replace atomically through a temporary name in the same directory.
		tmp := final + ".xsync-" + claim.ObjectID
		_ = os.Remove(tmp)
		if err := os.Link(partial, tmp); err != nil {
			return false, err
		}
		if err := os.Rename(tmp, final); err != nil {
			_ = os.Remove(tmp)
			return false, err
		}
		return true, syncDir(dir)
	}
	return false, fmt.Errorf("%w: %s keeps changing", ErrConflict, claim.Path)
}

func (c *Client) backupPath(p string) (string, error) {
	name := c.stateDir("conflicts", filepath.FromSlash(p)+"."+time.Now().UTC().Format("20060102T150405.000000000Z"))
	return name, os.MkdirAll(filepath.Dir(name), 0o750)
}

type checkpoint struct {
	ObjectID string `json:"object_id"`
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
	// Offset is the number of bytes known to be on disk (fsynced) and
	// HashState the SHA-256 state after them, so a resume neither trusts
	// unsynced bytes nor rereads the prefix.
	Offset    int64  `json:"offset"`
	HashState string `json:"hash_state,omitempty"`
}

func readCheckpoint(name string, claim Claim) (checkpoint, bool) {
	raw, err := os.ReadFile(name)
	if err != nil {
		return checkpoint{}, false
	}
	var cp checkpoint
	if json.Unmarshal(raw, &cp) != nil || cp.ObjectID != claim.ObjectID || cp.SHA256 != claim.SHA256 || cp.Size != claim.Size || cp.Offset < 0 || cp.Offset > claim.Size {
		return checkpoint{}, false
	}
	return cp, true
}

func (c *Client) download(ctx context.Context, claim Claim, partial, sidecar string) (string, int64, error) {
	f, err := os.OpenFile(partial, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	// Exclusive ownership of the partial: a second writer, in this or
	// another process, would interleave with ours.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return "", 0, fmt.Errorf("partial file is in use by another downloader: %w", err)
	}
	hasher := sha256.New()
	offset := int64(0)
	if cp, ok := readCheckpoint(sidecar, claim); ok {
		if st, err := f.Stat(); err == nil && st.Size() >= cp.Offset {
			offset = cp.Offset
			if state, err := base64.StdEncoding.DecodeString(cp.HashState); err == nil && cp.HashState != "" {
				if err := hasher.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
					offset = 0
					hasher.Reset()
				}
			} else if offset > 0 {
				if _, err := io.CopyN(hasher, io.NewSectionReader(f, 0, offset), offset); err != nil {
					offset = 0
					hasher.Reset()
				}
			}
		}
	}
	// Bytes after the last checkpoint may never have reached the disk.
	if err := f.Truncate(offset); err != nil {
		return "", 0, err
	}
	if offset == claim.Size {
		return hex.EncodeToString(hasher.Sum(nil)), offset, nil
	}
	req, err := c.request(ctx, http.MethodGet, "/v1/objects/"+claim.ObjectID+"/content", nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("X-Xsync-Lease-ID", claim.LeaseID)
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		req.Header.Set("If-Range", `"sha256:`+claim.SHA256+`"`)
	}
	stallCtx, stall := context.WithCancelCause(ctx)
	defer stall(nil)
	resp, err := c.http.Do(req.WithContext(stallCtx))
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusOK:
		if offset > 0 {
			offset = 0
			hasher.Reset()
			if err := f.Truncate(0); err != nil {
				return "", 0, err
			}
		}
	case http.StatusNotFound, http.StatusConflict:
		return "", 0, errLeaseLost
	default:
		return "", 0, responseError(resp)
	}
	body := newStallReader(resp.Body, 2*time.Minute, func() { stall(errors.New("download stalled")) })
	defer body.stop()
	buf := c.buffers.Get().([]byte)
	defer c.buffers.Put(buf)
	written := offset
	lastSync := time.Now()
	checkpointNow := func() error {
		if err := f.Sync(); err != nil {
			return err
		}
		return writeCheckpoint(sidecar, claim, written, hasher)
	}
	for {
		n, re := body.Read(buf)
		if n > 0 {
			if written+int64(n) > claim.Size {
				return "", 0, fmt.Errorf("server sent more than %d bytes", claim.Size)
			}
			wn, we := f.WriteAt(buf[:n], written)
			if wn > 0 {
				_, _ = hasher.Write(buf[:wn])
				written += int64(wn)
			}
			if we != nil {
				return "", 0, we
			}
			if time.Since(lastSync) >= 5*time.Second {
				if err := checkpointNow(); err != nil {
					return "", 0, err
				}
				lastSync = time.Now()
			}
		}
		if re == io.EOF {
			break
		}
		if re != nil {
			_ = checkpointNow()
			if cause := context.Cause(stallCtx); cause != nil && ctx.Err() == nil {
				return "", 0, cause
			}
			return "", 0, re
		}
	}
	if err := checkpointNow(); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hasher.Sum(nil)), written, nil
}

func writeCheckpoint(name string, claim Claim, offset int64, h hash.Hash) error {
	cp := checkpoint{ObjectID: claim.ObjectID, Path: claim.Path, Size: claim.Size, SHA256: claim.SHA256, Offset: offset}
	if m, ok := h.(encoding.BinaryMarshaler); ok {
		if state, err := m.MarshalBinary(); err == nil {
			cp.HashState = base64.StdEncoding.EncodeToString(state)
		}
	}
	raw, _ := json.Marshal(cp)
	return writeFileAtomic(name, raw)
}

// stallReader cancels a download that makes no progress for idle, without
// limiting how long a healthy transfer may take.
type stallReader struct {
	r     io.Reader
	idle  time.Duration
	timer *time.Timer
}

func newStallReader(r io.Reader, idle time.Duration, onStall func()) *stallReader {
	return &stallReader{r: r, idle: idle, timer: time.AfterFunc(idle, onStall)}
}

func (s *stallReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if n > 0 {
		s.timer.Reset(s.idle)
	}
	return n, err
}

func (s *stallReader) stop() { s.timer.Stop() }

// reserveSpace checks free space for n more bytes, counting bytes promised to
// concurrent downloads that have not been written yet.
func (c *Client) reserveSpace(n uint64) (func(), error) {
	c.spaceMu.Lock()
	defer c.spaceMu.Unlock()
	var st syscall.Statfs_t
	if err := syscall.Statfs(c.cfg.Destination, &st); err != nil {
		return nil, err
	}
	available := uint64(st.Bavail) * uint64(st.Bsize)
	if available < n+c.reserved+c.cfg.ReserveBytes {
		return nil, fmt.Errorf("insufficient destination space: available=%d need=%d reserved=%d reserve=%d", available, n, c.reserved, c.cfg.ReserveBytes)
	}
	c.reserved += n
	var once sync.Once
	return func() {
		once.Do(func() {
			c.spaceMu.Lock()
			c.reserved -= n
			c.spaceMu.Unlock()
		})
	}, nil
}

// collectPartials removes partial downloads abandoned for longer than
// partial_ttl, for example of objects that another client delivered.
func (c *Client) collectPartials() {
	entries, err := os.ReadDir(c.stateDir("partial"))
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".partial") {
			continue
		}
		if _, busy := c.inflight.Load(strings.TrimSuffix(name, ".partial")); busy {
			continue
		}
		info, err := e.Info()
		if err != nil || time.Since(info.ModTime()) < c.cfg.PartialTTL {
			continue
		}
		p := c.stateDir("partial", name)
		f, err := os.OpenFile(p, os.O_RDWR, 0)
		if err != nil {
			continue
		}
		if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
			_ = os.Remove(p)
			_ = os.Remove(p + ".json")
			c.log.Info("removed abandoned partial download", "file", name)
		}
		f.Close()
	}
}

type claimRequest struct {
	ClientID     string `json:"client_id"`
	Prefix       string `json:"prefix"`
	LeaseSeconds int    `json:"lease_seconds"`
	Max          int    `json:"max"`
	WaitSeconds  int    `json:"wait_seconds,omitempty"`
}

// claim asks for up to max objects. Servers that predate batch claims answer
// with a single claim, which is handled the same way.
func (c *Client) claim(ctx context.Context, max int) ([]Claim, error) {
	wait := 0
	if c.longPoll.Load() {
		wait = int(c.cfg.LongPoll / time.Second)
	}
	body, _ := json.Marshal(claimRequest{ClientID: c.cfg.ClientID, Prefix: c.cfg.Prefix, LeaseSeconds: int(c.cfg.LeaseTTL.Seconds()), Max: max, WaitSeconds: wait})
	ctx, cancel := context.WithTimeout(ctx, c.cfg.LongPoll+c.cfg.RequestTimeout)
	defer cancel()
	req, err := c.request(ctx, http.MethodPost, "/v1/claims", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if v, _ := strconv.Atoi(resp.Header.Get("X-Xsync-API-Version")); v < 2 {
		c.longPoll.Store(false)
	}
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil, nil
	case http.StatusOK:
		var batch struct {
			Claims []Claim `json:"claims"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&batch); err != nil {
			return nil, err
		}
		return batch.Claims, nil
	case http.StatusCreated:
		var one Claim
		if err := json.NewDecoder(resp.Body).Decode(&one); err != nil {
			return nil, err
		}
		return []Claim{one}, nil
	}
	return nil, responseError(resp)
}

func (c *Client) control(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	req, err := c.request(ctx, method, path, rd)
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	resp.Body = cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// renew extends the lease by its original length and returns the new expiry.
func (c *Client) renew(ctx context.Context, lease string) (time.Time, error) {
	resp, err := c.control(ctx, http.MethodPost, "/v1/claims/"+lease+"/renew", nil)
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var claim Claim
		if err := json.NewDecoder(resp.Body).Decode(&claim); err != nil || claim.LeaseUntil.IsZero() {
			return time.Now().Add(c.cfg.LeaseTTL), nil
		}
		return claim.LeaseUntil, nil
	case http.StatusNotFound, http.StatusConflict:
		return time.Time{}, errLeaseLost
	}
	return time.Time{}, responseError(resp)
}

type releaseRequest struct {
	Reason            string `json:"reason,omitempty"`
	RetryAfterSeconds int    `json:"retry_after_seconds,omitempty"`
	Permanent         bool   `json:"permanent,omitempty"`
	CountAttempt      *bool  `json:"count_attempt,omitempty"`
}

func (c *Client) release(ctx context.Context, claim Claim, body releaseRequest) {
	if len(body.Reason) > 500 {
		body.Reason = body.Reason[:500]
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := c.control(ctx, http.MethodPost, "/v1/claims/"+claim.LeaseID+"/release", body)
	if err == nil && resp.StatusCode == http.StatusNotFound {
		// A server that predates POST .../release.
		resp.Body.Close()
		resp, err = c.control(ctx, http.MethodDelete, "/v1/claims/"+claim.LeaseID, nil)
	}
	if err != nil {
		c.log.Warn("release failed; the lease will expire on its own", "object_id", claim.ObjectID, "error", err)
		return
	}
	resp.Body.Close()
}

func (c *Client) commit(ctx context.Context, claim Claim) error {
	resp, err := c.control(ctx, http.MethodPost, "/v1/objects/"+claim.ObjectID+"/commit", map[string]any{"lease_id": claim.LeaseID, "sha256": claim.SHA256, "size": claim.Size})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusConflict:
		return errLeaseLost
	}
	return responseError(resp)
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

// safeDestination maps a server-provided path into root. It rejects absolute
// paths, traversal, control characters and the client's own state directory.
func safeDestination(root, name string) (string, error) {
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return "", errors.New("remote path contains control characters")
		}
	}
	name = strings.ReplaceAll(name, "\\", "/")
	clean := filepath.Clean(filepath.FromSlash(name))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("unsafe remote path")
	}
	if first, _, _ := strings.Cut(filepath.ToSlash(clean), "/"); first == ".xsync" {
		return "", errors.New("remote path targets the client state directory")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	target := filepath.Join(rootAbs, clean)
	if target != rootAbs && !strings.HasPrefix(target, rootAbs+string(filepath.Separator)) {
		return "", errors.New("remote path escapes destination")
	}
	if err := noSymlinks(rootAbs, filepath.Dir(target)); err != nil {
		return "", err
	}
	return target, nil
}

// noSymlinks fails if any existing directory between root and dir is a
// symbolic link, which could redirect a write outside the destination.
func noSymlinks(root, dir string) error {
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == "." {
		return err
	}
	cur := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		cur = filepath.Join(cur, part)
		st, err := os.Lstat(cur)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %s", errSymlink, cur)
		}
	}
	return nil
}

func mkdirAllNoSymlinks(root, dir string, mode os.FileMode) error {
	if err := noSymlinks(root, dir); err != nil {
		return err
	}
	return os.MkdirAll(dir, mode)
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

func syncDir(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func writeFileAtomic(name string, data []byte) error {
	tmp := name + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
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

// ListParked returns the server's list of parked objects as indented JSON.
func (c *Client) ListParked(ctx context.Context) (string, error) {
	resp, err := c.control(ctx, http.MethodGet, "/v1/parked", nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", responseError(resp)
	}
	var v any
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return "", err
	}
	out, _ := json.MarshalIndent(v, "", "  ")
	return string(out), nil
}

// RequeueParked gives a parked object a fresh set of delivery attempts.
func (c *Client) RequeueParked(ctx context.Context, id string) error {
	return c.simple(ctx, http.MethodPost, "/v1/objects/"+id+"/requeue")
}

// DropParked deletes a parked object on the server.
func (c *Client) DropParked(ctx context.Context, id string) error {
	return c.simple(ctx, http.MethodDelete, "/v1/objects/"+id)
}

func (c *Client) simple(ctx context.Context, method, path string) error {
	resp, err := c.control(ctx, method, path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return responseError(resp)
	}
	return nil
}
