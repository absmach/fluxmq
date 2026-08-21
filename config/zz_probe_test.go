package config

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestProbeStorageLegacyKey(t *testing.T) {
	_, err := Load(write(t, "storage:\n  type: badger\n  sync_writes: true\n"))
	t.Logf("storage.sync_writes -> %v", err)
}

func TestProbeTrailingSeparator(t *testing.T) {
	_, err := Load(write(t, "server:\n  mqtt:\n    tcp:\n      v3:\n        addr: \":1883\"\n---\n"))
	t.Logf("trailing --- -> %v", err)
}

func TestProbeTrailingComment(t *testing.T) {
	_, err := Load(write(t, "log:\n  level: info\n---\n# just a comment\n"))
	t.Logf("trailing --- + comment -> %v", err)
}

func TestProbeEmpty(t *testing.T) {
	c, err := Load(write(t, ""))
	t.Logf("empty -> cfg=%v err=%v", c != nil, err)
}

func TestProbeIdentityCacheZero(t *testing.T) {
	_, err := Load(write(t, "auth:\n  external:\n    url: \"https://a:1\"\n    identity_cache_ttl: 0\n"))
	t.Logf("identity_cache_ttl: 0 -> %v", err)
}

func TestProbeIPv6WildcardConflict(t *testing.T) {
	cfg := Default()
	cfg.Server.MQTT.TCP.V3.Addr = "0.0.0.0:1883"
	cfg.Server.MQTT.TCP.V5.Addr = "[::1]:1883"
	t.Logf("0.0.0.0:1883 vs [::1]:1883 -> %v", cfg.validateNoDuplicateBinds())
}

func TestProbeUnknownNested(t *testing.T) {
	_, err := Load(write(t, "server:\n  mqtt:\n    tcp:\n      v3:\n        addr: \":1883\"\n        maxconnections: 5\n"))
	t.Logf("unknown nested -> %v", err)
}

func TestProbeAliasDup(t *testing.T) {
	cfg := Default()
	cfg.Server.AMQP091.Local.Addr = "127.0.0.1:5680"
	cfg.Server.AMQP091.Internal.Addr = "127.0.0.1:5680"
	t.Logf("local+internal same addr -> %v", cfg.validateNoDuplicateBinds())
	t.Logf("LocalListeners=%d deprecated=%v", len(cfg.Server.AMQP091.LocalListeners()), cfg.Server.AMQP091.DeprecatedLocalListenerNames())
}
