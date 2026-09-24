package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteUsesMinimalConfigAndLoadAppliesPerformanceDefaults(t *testing.T) {
	cfg := Default()
	cfg.Endpoint = "https://127.0.0.1:9443"
	cfg.APIKey = "secret"
	cfg.CAFile = "/etc/xsync-client/server-ca.crt"
	cfg.Destination = "/srv/incoming"
	name := filepath.Join(t.TempDir(), "client.yaml")
	if err := Write(name, cfg); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, hiddenDefault := range []string{"concurrency:", "buffer_size:", "lease_ttl:", "poll_interval:", "reserve_bytes:"} {
		if strings.Contains(string(raw), hiddenDefault) {
			t.Fatalf("generated config exposes default %q:\n%s", hiddenDefault, raw)
		}
	}
	loaded, err := Load(name)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Concurrency != 8 || loaded.BufferSize != 1<<20 {
		t.Fatalf("performance defaults not restored: %+v", loaded)
	}
}

func TestLoadServerConnectionBundle(t *testing.T) {
	name := filepath.Join(t.TempDir(), "account.yaml")
	raw := `version: 1
account: customer-a
download:
  endpoint: https://203.0.113.10:9443
  api_key: download-secret
sftp:
  host: 203.0.113.10
  port: 2022
  username: customer-a
  password: sftp-secret
  host_key_sha256: SHA256:test
ftps:
  host: 203.0.113.10
  port: 2121
  username: customer-a
  password: ftp-secret
  tls_mode: explicit
s3:
  endpoint: https://203.0.113.10:9000
  bucket: customer-a
  access_key: access
  secret_key: secret
  region: us-east-1
security:
  tls_ca_pem: |
    -----BEGIN CERTIFICATE-----
    test
    -----END CERTIFICATE-----
  tls_certificate_sha256: abcdef
`
	if err := os.WriteFile(name, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadBundle(name, "/srv/incoming")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Endpoint != "https://203.0.113.10:9443" || cfg.APIKey != "download-secret" || cfg.Destination != "/srv/incoming" || !strings.HasPrefix(cfg.ClientID, "xsync-client-customer-a") || !strings.Contains(cfg.CACertificate, "BEGIN CERTIFICATE") {
		t.Fatalf("unexpected runtime config: %+v", cfg)
	}
}
