package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Endpoint       string        `yaml:"endpoint"`
	APIKey         string        `yaml:"api_key"`
	APIKeyFile     string        `yaml:"api_key_file"`
	CAFile         string        `yaml:"ca_file"`
	CACertificate  string        `yaml:"ca_certificate"`
	TLSFingerprint string        `yaml:"tls_fingerprint"`
	Destination    string        `yaml:"destination"`
	Prefix         string        `yaml:"prefix"`
	ClientID       string        `yaml:"client_id"`
	Concurrency    int           `yaml:"concurrency"`
	BufferSize     int           `yaml:"buffer_size"`
	LeaseTTL       time.Duration `yaml:"lease_ttl"`
	PollInterval   time.Duration `yaml:"poll_interval"`
	ReserveBytes   uint64        `yaml:"reserve_bytes"`
	// Conflict decides what happens when the destination already holds a
	// different file that this client did not deliver: "backup" moves it to
	// .xsync/conflicts/ first (default), "overwrite" replaces it, "skip" hands
	// the object back to the server, which parks it for an operator.
	Conflict string `yaml:"conflict"`
	// FileMode and DirMode apply to delivered files and created directories.
	FileMode os.FileMode `yaml:"file_mode"`
	DirMode  os.FileMode `yaml:"dir_mode"`
	// MaxBatch is how many objects one claim request may return.
	MaxBatch int `yaml:"max_batch"`
	// LongPoll is how long an empty claim waits on the server for new work.
	LongPoll time.Duration `yaml:"long_poll"`
	// RequestTimeout bounds control requests (claim, renew, commit, release).
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// PartialTTL is how long an abandoned partial download is kept.
	PartialTTL time.Duration `yaml:"partial_ttl"`
}

const (
	ConflictBackup    = "backup"
	ConflictOverwrite = "overwrite"
	ConflictSkip      = "skip"
)

type fileConfig struct {
	Endpoint       string        `yaml:"endpoint"`
	APIKey         string        `yaml:"api_key,omitempty"`
	APIKeyFile     string        `yaml:"api_key_file,omitempty"`
	CAFile         string        `yaml:"ca_file,omitempty"`
	CACertificate  string        `yaml:"ca_certificate,omitempty"`
	TLSFingerprint string        `yaml:"tls_fingerprint,omitempty"`
	Destination    string        `yaml:"destination"`
	Prefix         string        `yaml:"prefix,omitempty"`
	ClientID       string        `yaml:"client_id,omitempty"`
	Concurrency    int           `yaml:"concurrency,omitempty"`
	BufferSize     int           `yaml:"buffer_size,omitempty"`
	LeaseTTL       time.Duration `yaml:"lease_ttl,omitempty"`
	PollInterval   time.Duration `yaml:"poll_interval,omitempty"`
	ReserveBytes   uint64        `yaml:"reserve_bytes,omitempty"`
	Conflict       string        `yaml:"conflict,omitempty"`
}

type ConnectionBundle struct {
	Version  int            `yaml:"version"`
	Account  string         `yaml:"account"`
	Download BundleDownload `yaml:"download"`
	SFTP     BundleSFTP     `yaml:"sftp"`
	FTPS     BundleFTPS     `yaml:"ftps"`
	S3       BundleS3       `yaml:"s3"`
	Security BundleSecurity `yaml:"security"`
}

type BundleDownload struct {
	Endpoint string `yaml:"endpoint"`
	APIKey   string `yaml:"api_key"`
}

type BundleSFTP struct {
	Host               string `yaml:"host"`
	Port               int    `yaml:"port"`
	Username           string `yaml:"username"`
	Password           string `yaml:"password"`
	HostKeyFingerprint string `yaml:"host_key_sha256"`
}

type BundleFTPS struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	TLSMode  string `yaml:"tls_mode"`
}

type BundleS3 struct {
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Region    string `yaml:"region"`
}

type BundleSecurity struct {
	TLSCAPEM                  string `yaml:"tls_ca_pem"`
	TLSCertificateFingerprint string `yaml:"tls_certificate_sha256"`
}

func Default() Config {
	return Config{
		Endpoint: "https://127.0.0.1:9443", Destination: "./downloads", ClientID: "xsync-client",
		Concurrency: 8, BufferSize: 1 << 20, LeaseTTL: 2 * time.Minute, PollInterval: 3 * time.Second,
		ReserveBytes: 1 << 30, Conflict: ConflictBackup, FileMode: 0o640, DirMode: 0o750,
		MaxBatch: 8, LongPoll: 20 * time.Second, RequestTimeout: 30 * time.Second, PartialTTL: 7 * 24 * time.Hour,
	}
}
func Load(name string) (Config, error) {
	c := Default()
	raw, err := os.ReadFile(name)
	if err != nil {
		return c, err
	}
	if err = yaml.Unmarshal(raw, &c); err != nil {
		return c, fmt.Errorf("decode config: %w", err)
	}
	if c.APIKey == "" && c.APIKeyFile != "" {
		b, e := os.ReadFile(c.APIKeyFile)
		if e != nil {
			return c, e
		}
		c.APIKey = string(bytesTrimSpace(b))
	}
	if err = c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}
func Write(name string, c Config) error {
	defaults := Default()
	out := fileConfig{Endpoint: c.Endpoint, APIKey: c.APIKey, APIKeyFile: c.APIKeyFile, CAFile: c.CAFile, CACertificate: c.CACertificate, TLSFingerprint: c.TLSFingerprint, Destination: c.Destination, Prefix: c.Prefix}
	if c.ClientID != defaults.ClientID {
		out.ClientID = c.ClientID
	}
	if c.Concurrency != defaults.Concurrency {
		out.Concurrency = c.Concurrency
	}
	if c.BufferSize != defaults.BufferSize {
		out.BufferSize = c.BufferSize
	}
	if c.LeaseTTL != defaults.LeaseTTL {
		out.LeaseTTL = c.LeaseTTL
	}
	if c.PollInterval != defaults.PollInterval {
		out.PollInterval = c.PollInterval
	}
	if c.ReserveBytes != defaults.ReserveBytes {
		out.ReserveBytes = c.ReserveBytes
	}
	if c.Conflict != defaults.Conflict {
		out.Conflict = c.Conflict
	}
	raw, err := yaml.Marshal(out)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(name), 0o750); err != nil {
		return err
	}
	return os.WriteFile(name, raw, 0o600)
}
func (c Config) Validate() error {
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return errors.New("endpoint must be an https URL")
	}
	if c.APIKey == "" {
		return errors.New("api_key or api_key_file is required")
	}
	if c.CAFile == "" && c.CACertificate == "" && c.TLSFingerprint == "" {
		return errors.New("CA certificate or tls_fingerprint is required")
	}
	if c.Destination == "" {
		return errors.New("destination is required")
	}
	if c.ClientID == "" {
		return errors.New("client_id is required")
	}
	if c.Concurrency < 1 || c.Concurrency > 128 {
		return errors.New("concurrency must be between 1 and 128")
	}
	if c.BufferSize < 64<<10 || c.BufferSize > 16<<20 {
		return errors.New("buffer_size must be between 64 KiB and 16 MiB")
	}
	if c.LeaseTTL < 30*time.Second || c.LeaseTTL > time.Hour {
		return errors.New("lease_ttl must be between 30s and 1h (the server caps leases at its max_lease)")
	}
	if c.PollInterval < 100*time.Millisecond {
		return errors.New("poll_interval must be at least 100ms")
	}
	switch c.Conflict {
	case ConflictBackup, ConflictOverwrite, ConflictSkip:
	default:
		return fmt.Errorf("conflict must be %q, %q or %q", ConflictBackup, ConflictOverwrite, ConflictSkip)
	}
	if c.FileMode&^0o777 != 0 || c.DirMode&^0o777 != 0 || c.FileMode&0o600 != 0o600 || c.DirMode&0o700 != 0o700 {
		return errors.New("file_mode and dir_mode must be permission bits that keep owner read/write access")
	}
	if c.MaxBatch < 1 || c.MaxBatch > 256 {
		return errors.New("max_batch must be between 1 and 256")
	}
	if c.LongPoll < 0 || c.LongPoll > 5*time.Minute || c.RequestTimeout < time.Second {
		return errors.New("long_poll must be 0-5m and request_timeout at least 1s")
	}
	return nil
}

// LoadBundle turns a server-generated account connection bundle into the
// downloader's runtime configuration. Upload credentials remain available in
// the bundle for SFTP/FTPS/S3 tools but are not used by the downloader.
func LoadBundle(name, destination string) (Config, error) {
	c := Default()
	raw, err := os.ReadFile(name)
	if err != nil {
		return c, err
	}
	var bundle ConnectionBundle
	if err = yaml.Unmarshal(raw, &bundle); err != nil {
		return c, fmt.Errorf("decode client connection bundle: %w", err)
	}
	if bundle.Version != 1 {
		return c, fmt.Errorf("unsupported client connection bundle version %d", bundle.Version)
	}
	if bundle.Account == "" {
		return c, errors.New("client connection bundle has no account")
	}
	c.Endpoint = bundle.Download.Endpoint
	c.APIKey = bundle.Download.APIKey
	c.CACertificate = bundle.Security.TLSCAPEM
	c.TLSFingerprint = bundle.Security.TLSCertificateFingerprint
	c.Destination = destination
	// Several machines may use the same bundle; the host name keeps their
	// leases apart in server logs.
	c.ClientID = "xsync-client-" + bundle.Account
	if host, err := os.Hostname(); err == nil && host != "" {
		c.ClientID += "@" + host
	}
	if err = c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}
func bytesTrimSpace(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && (b[start] == ' ' || b[start] == '\n' || b[start] == '\r' || b[start] == '\t') {
		start++
	}
	for end > start && (b[end-1] == ' ' || b[end-1] == '\n' || b[end-1] == '\r' || b[end-1] == '\t') {
		end--
	}
	return b[start:end]
}
