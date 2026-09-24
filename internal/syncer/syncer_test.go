package syncer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xylandev/xsync-clinet/internal/config"
)

// fakeServer implements the download API well enough to exercise the client.
type fakeServer struct {
	t   *testing.T
	srv *httptest.Server
	v1  bool

	mu        sync.Mutex
	objects   map[string]*fakeObject
	order     []string
	leases    map[string]string // lease -> object
	commits   map[string]int
	releases  []releaseRequest
	content   int
	onContent func(id string)
}

type fakeObject struct {
	id, path string
	data     []byte
	version  uint64
	attempts int
	leased   string
	done     bool
	parked   bool
}

func newFakeServer(t *testing.T) *fakeServer {
	f := &fakeServer{t: t, objects: map[string]*fakeObject{}, leases: map[string]string{}, commits: map[string]int{}}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeServer) add(id, path, data string, version uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[id] = &fakeObject{id: id, path: path, data: []byte(data), version: version}
	f.order = append(f.order, id)
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (f *fakeServer) claimFor(o *fakeObject) Claim {
	lease := fmt.Sprintf("lease-%s-%d", o.id, o.attempts)
	f.leases[lease] = o.id
	o.leased = lease
	return Claim{LeaseID: lease, ObjectID: o.id, Path: o.path, Size: int64(len(o.data)), SHA256: digest(o.data), Version: o.version, Attempts: o.attempts, LeaseUntil: time.Now().Add(time.Minute)}
}

func (f *fakeServer) handle(w http.ResponseWriter, r *http.Request) {
	if !f.v1 {
		w.Header().Set("X-Xsync-API-Version", "2")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/claims":
		var req claimRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		max := req.Max
		if f.v1 || max < 1 {
			max = 1
		}
		var out []Claim
		for _, id := range f.order {
			o := f.objects[id]
			if o.done || o.parked || o.leased != "" || len(out) >= max {
				continue
			}
			o.attempts++
			out = append(out, f.claimFor(o))
		}
		if len(out) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if f.v1 || req.Max == 0 {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(out[0])
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"claims": out})
	case len(parts) == 4 && parts[1] == "claims" && parts[3] == "renew":
		id, ok := f.leases[parts[2]]
		if !ok || f.objects[id].leased != parts[2] {
			w.WriteHeader(http.StatusConflict)
			return
		}
		_ = json.NewEncoder(w).Encode(Claim{LeaseID: parts[2], LeaseUntil: time.Now().Add(time.Minute)})
	case len(parts) == 4 && parts[1] == "claims" && parts[3] == "release" && !f.v1, r.Method == http.MethodDelete && len(parts) == 3 && parts[1] == "claims":
		var req releaseRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.releases = append(f.releases, req)
		if id, ok := f.leases[parts[2]]; ok && f.objects[id].leased == parts[2] {
			f.objects[id].leased = ""
			f.objects[id].parked = req.Permanent
		}
		w.WriteHeader(http.StatusNoContent)
	case len(parts) == 4 && parts[3] == "content":
		o := f.objects[parts[2]]
		if o == nil || o.leased != r.Header.Get("X-Xsync-Lease-ID") {
			w.WriteHeader(http.StatusConflict)
			return
		}
		f.content++
		if f.onContent != nil {
			f.mu.Unlock()
			f.onContent(o.id)
			f.mu.Lock()
		}
		start := 0
		if v := r.Header.Get("Range"); v != "" {
			_, _ = fmt.Sscanf(v, "bytes=%d-", &start)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(o.data)-1, len(o.data)))
			w.WriteHeader(http.StatusPartialContent)
		}
		_, _ = w.Write(o.data[start:])
	case len(parts) == 4 && parts[3] == "commit":
		o := f.objects[parts[2]]
		var req struct {
			LeaseID string `json:"lease_id"`
			SHA256  string `json:"sha256"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if o == nil || (o.leased != req.LeaseID && !o.done) {
			w.WriteHeader(http.StatusConflict)
			return
		}
		if req.SHA256 != digest(o.data) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}
		o.done, o.leased = true, ""
		f.commits[o.id]++
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeServer) client(t *testing.T, mutate ...func(*config.Config)) (*Client, string) {
	t.Helper()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	cfg := config.Default()
	cfg.Endpoint, cfg.APIKey, cfg.CAFile, cfg.Destination = f.srv.URL, "key", caFile, dest
	cfg.ReserveBytes = 0
	cfg.LeaseTTL = 30 * time.Second
	cfg.PollInterval = 100 * time.Millisecond
	cfg.LongPoll = 0
	for _, m := range mutate {
		m(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	c, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"partial", "meta", "conflicts"} {
		_ = os.MkdirAll(c.stateDir(d), 0o750)
	}
	return c, dest
}

func (f *fakeServer) take(t *testing.T, id string) Claim {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	o := f.objects[id]
	o.attempts++
	return f.claimFor(o)
}

func read(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestSafeDestination(t *testing.T) {
	root := t.TempDir()
	if _, err := safeDestination(root, "dir/file"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../file", "a/../../file", "/absolute", ".xsync/partial/x", "bad\nname", "tab\tname"} {
		if _, err := safeDestination(root, name); err == nil {
			t.Errorf("unsafe path %q accepted", name)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := safeDestination(root, "link/file"); err == nil {
		t.Error("path through a symlink accepted")
	}
}

func TestResumeFromCheckpointWithoutRereadingPrefix(t *testing.T) {
	f := newFakeServer(t)
	payload := "hello resumable world"
	f.add("obj", "dir/file.txt", payload, 1)
	c, dest := f.client(t)
	claim := f.take(t, "obj")
	partial := c.stateDir("partial", "obj.partial")
	if err := os.WriteFile(partial, []byte(payload[:6]+"garbage-not-synced"), 0o600); err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	h.Write([]byte(payload[:6]))
	if err := writeCheckpoint(partial+".json", claim, 6, h); err != nil {
		t.Fatal(err)
	}
	if err := c.process(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if got := read(t, filepath.Join(dest, "dir", "file.txt")); got != payload {
		t.Fatalf("got %q", got)
	}
	if f.commits["obj"] != 1 {
		t.Fatal("not committed")
	}
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial left behind after delivery")
	}
	info, _ := os.Stat(filepath.Join(dest, "dir", "file.txt"))
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("file mode %v", info.Mode().Perm())
	}
}

func TestCompleteCheckpointNeedsNoContentRequest(t *testing.T) {
	f := newFakeServer(t)
	f.add("obj", "file.txt", "complete partial", 1)
	c, dest := f.client(t)
	claim := f.take(t, "obj")
	partial := c.stateDir("partial", "obj.partial")
	_ = os.WriteFile(partial, []byte("complete partial"), 0o600)
	h := sha256.New()
	h.Write([]byte("complete partial"))
	_ = writeCheckpoint(partial+".json", claim, int64(len("complete partial")), h)
	if err := c.process(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if f.content != 0 {
		t.Fatalf("%d content requests for a complete partial", f.content)
	}
	if read(t, filepath.Join(dest, "file.txt")) != "complete partial" {
		t.Fatal("wrong content")
	}
}

// A renewal that finds the lease gone must stop the download instead of
// racing the new holder.
func TestLeaseLossStopsTheDownload(t *testing.T) {
	f := newFakeServer(t)
	f.add("obj", "file.txt", strings.Repeat("x", 1<<20), 1)
	c, dest := f.client(t)
	claim := f.take(t, "obj")
	claim.LeaseUntil = time.Now().Add(3 * time.Second) // renew after ~1s
	block := make(chan struct{})
	f.mu.Lock()
	f.onContent = func(string) {
		// Someone else takes the object over while we are downloading.
		f.mu.Lock()
		f.objects["obj"].leased = "someone-else"
		f.mu.Unlock()
		<-block
	}
	f.mu.Unlock()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	stop := c.keepLease(ctx, claim, cancel)
	done := make(chan error, 1)
	go func() { done <- c.process(ctx, claim) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("download kept running after the lease was lost")
	}
	close(block)
	stop()
	if !errors.Is(context.Cause(ctx), errLeaseLost) {
		t.Fatalf("cause = %v", context.Cause(ctx))
	}
	if _, err := os.Stat(filepath.Join(dest, "file.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("file placed without holding the lease")
	}
}

func TestConflictBackupKeepsForeignFile(t *testing.T) {
	f := newFakeServer(t)
	f.add("obj", "report.csv", "new content", 1)
	c, dest := f.client(t)
	_ = os.WriteFile(filepath.Join(dest, "report.csv"), []byte("somebody else's file"), 0o644)
	if err := c.process(context.Background(), f.take(t, "obj")); err != nil {
		t.Fatal(err)
	}
	if read(t, filepath.Join(dest, "report.csv")) != "new content" {
		t.Fatal("new version not placed")
	}
	backups, _ := filepath.Glob(c.stateDir("conflicts", "report.csv.*"))
	if len(backups) != 1 || read(t, backups[0]) != "somebody else's file" {
		t.Fatalf("foreign file not backed up: %v", backups)
	}
}

// Re-uploads of the same path used to conflict forever and block the queue.
func TestNewerVersionReplacesAndOlderVersionIsSkipped(t *testing.T) {
	f := newFakeServer(t)
	f.add("v2", "daily.csv", "tuesday", 2)
	f.add("v1", "daily.csv", "monday", 1)
	f.add("v3", "daily.csv", "wednesday", 3)
	c, dest := f.client(t)
	if err := c.process(context.Background(), f.take(t, "v2")); err != nil {
		t.Fatal(err)
	}
	if err := c.process(context.Background(), f.take(t, "v1")); err != nil {
		t.Fatal(err)
	}
	if read(t, filepath.Join(dest, "daily.csv")) != "tuesday" {
		t.Fatal("an older version overwrote a newer one")
	}
	if f.commits["v1"] != 1 {
		t.Fatal("obsolete version was not committed away")
	}
	if err := c.process(context.Background(), f.take(t, "v3")); err != nil {
		t.Fatal(err)
	}
	if read(t, filepath.Join(dest, "daily.csv")) != "wednesday" {
		t.Fatal("newer version not placed")
	}
	if backups, _ := filepath.Glob(c.stateDir("conflicts", "*")); len(backups) != 0 {
		t.Fatalf("own deliveries should be replaced, not backed up: %v", backups)
	}
}

func TestSkipPolicyParksConflicts(t *testing.T) {
	f := newFakeServer(t)
	f.add("obj", "file", "new", 1)
	c, dest := f.client(t, func(cfg *config.Config) { cfg.Conflict = config.ConflictSkip })
	_ = os.WriteFile(filepath.Join(dest, "file"), []byte("old"), 0o644)
	claim := f.take(t, "obj")
	c.deliver(context.Background(), claim)
	if len(f.releases) != 1 || !f.releases[0].Permanent {
		t.Fatalf("releases = %+v", f.releases)
	}
	if read(t, filepath.Join(dest, "file")) != "old" {
		t.Fatal("skip policy overwrote the file")
	}
}

func TestRunDeliversBatchesAndReleasesOnShutdown(t *testing.T) {
	f := newFakeServer(t)
	for i := 0; i < 20; i++ {
		f.add(fmt.Sprintf("o%d", i), fmt.Sprintf("d/%d.txt", i), fmt.Sprintf("payload %d", i), uint64(i+1))
	}
	c, dest := f.client(t, func(cfg *config.Config) { cfg.Concurrency, cfg.MaxBatch = 4, 4 })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		f.mu.Lock()
		n := len(f.commits)
		f.mu.Unlock()
		if n == 20 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of 20 delivered", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i := 0; i < 20; i++ {
		if got := read(t, filepath.Join(dest, "d", fmt.Sprintf("%d.txt", i))); got != fmt.Sprintf("payload %d", i) {
			t.Fatalf("file %d = %q", i, got)
		}
	}
	// A slow object is in flight when the client stops.
	f.add("slow", "slow.bin", strings.Repeat("s", 1<<20), 99)
	release := make(chan struct{})
	f.mu.Lock()
	f.onContent = func(string) { <-release }
	f.mu.Unlock()
	time.Sleep(300 * time.Millisecond)
	cancel()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.objects["slow"].done {
		return
	}
	found := false
	for _, r := range f.releases {
		if r.CountAttempt != nil && !*r.CountAttempt {
			found = true
		}
	}
	if !found {
		t.Fatalf("shutdown did not hand the lease back uncounted: %+v", f.releases)
	}
}

func TestWorksWithVersionOneServer(t *testing.T) {
	f := newFakeServer(t)
	f.v1 = true
	f.add("a", "a.txt", "A", 1)
	f.add("b", "b.txt", "B", 2)
	c, dest := f.client(t, func(cfg *config.Config) { cfg.LongPoll = 20 * time.Second })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.mu.Lock()
		n := len(f.commits)
		f.mu.Unlock()
		if n == 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	if read(t, filepath.Join(dest, "a.txt")) != "A" || read(t, filepath.Join(dest, "b.txt")) != "B" {
		t.Fatal("v1 server not served")
	}
}

func TestCollectPartialsRemovesOnlyAbandoned(t *testing.T) {
	f := newFakeServer(t)
	c, _ := f.client(t, func(cfg *config.Config) { cfg.PartialTTL = time.Hour })
	old := c.stateDir("partial", "old.partial")
	fresh := c.stateDir("partial", "fresh.partial")
	_ = os.WriteFile(old, []byte("x"), 0o600)
	_ = os.WriteFile(fresh, []byte("x"), 0o600)
	past := time.Now().Add(-2 * time.Hour)
	_ = os.Chtimes(old, past, past)
	c.collectPartials()
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("abandoned partial kept")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatal("recent partial removed")
	}
}

func TestSpaceReservationsAccumulate(t *testing.T) {
	f := newFakeServer(t)
	c, _ := f.client(t)
	r1, err := c.reserveSpace(1)
	if err != nil {
		t.Fatal(err)
	}
	if c.reserved != 1 {
		t.Fatal("reservation not counted")
	}
	r1()
	r1()
	if c.reserved != 0 {
		t.Fatal("double release")
	}
	if _, err := c.reserveSpace(1 << 62); err == nil {
		t.Fatal("impossible reservation granted")
	}
	_ = io.Discard
}

func TestRetryDelayBacksOff(t *testing.T) {
	if retryDelay(1) != 10*time.Second || retryDelay(3) != 40*time.Second || retryDelay(20) != 10*time.Minute {
		t.Fatalf("delays %v %v %v", retryDelay(1), retryDelay(3), retryDelay(20))
	}
}

// A relative --dist used to make every placement fail, and the failure was
// reported as permanent, parking good objects.
func TestRelativeDestinationWorks(t *testing.T) {
	f := newFakeServer(t)
	f.add("obj", "a/b.txt", "data", 1)
	c, dest := f.client(t)
	t.Chdir(filepath.Dir(dest))
	cfg := c.cfg
	cfg.Destination = filepath.Base(dest)
	rel, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rel.process(context.Background(), f.take(t, "obj")); err != nil {
		t.Fatal(err)
	}
	if read(t, filepath.Join(dest, "a", "b.txt")) != "data" {
		t.Fatal("not delivered")
	}
}
