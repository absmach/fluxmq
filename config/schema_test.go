// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	testWSSAddr              = ":8084"
	testAuthCalloutKey       = "auth.external.url"
	testIdentityCacheSizeKey = "auth.external." + authIdentityCacheSizeField
	testIdentityCacheTTLKey  = "auth.external." + authIdentityCacheTTLField
)

// schemaKeys walks the Config struct and returns every accepted YAML key as a
// dotted path. Inline structs contribute their fields to the parent path,
// matching how yaml.v3 flattens `yaml:",inline"`.
func schemaKeys(t *testing.T, typ reflect.Type, prefix string, seen map[reflect.Type]bool) []string {
	t.Helper()

	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil
	}
	// Guard against recursive types; a repeat visit adds no new key names.
	if seen[typ] {
		return nil
	}
	seen[typ] = true
	defer delete(seen, typ)

	var keys []string
	for i := range typ.NumField() {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("yaml")
		name, opts, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}

		if strings.Contains(opts, "inline") {
			keys = append(keys, schemaKeys(t, field.Type, prefix, seen)...)
			continue
		}

		if name == "" {
			name = strings.ToLower(field.Name)
		}

		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		keys = append(keys, path)

		ft := field.Type
		for ft.Kind() == reflect.Pointer || ft.Kind() == reflect.Slice || ft.Kind() == reflect.Map {
			ft = ft.Elem()
		}
		keys = append(keys, schemaKeys(t, ft, path, seen)...)
	}
	return keys
}

func configSchemaKeys(t *testing.T) []string {
	t.Helper()
	keys := schemaKeys(t, reflect.TypeOf(Config{}), "", map[reflect.Type]bool{})
	sort.Strings(keys)
	return slicesCompact(keys)
}

func slicesCompact(in []string) []string {
	out := in[:0]
	var prev string
	for i, s := range in {
		if i == 0 || s != prev {
			out = append(out, s)
		}
		prev = s
	}
	return out
}

// TestSchemaTopLevelKeys pins the top-level key set. Adding a key here is a
// compatible change and needs a line in this list. Renaming or removing one
// breaks every deployed configuration, so this test must fail first.
func TestSchemaTopLevelKeys(t *testing.T) {
	want := []string{
		"auth",
		"broker",
		"cluster",
		"hooks",
		"log",
		"queue_manager",
		"queues",
		"ratelimit",
		"server",
		"session",
		"storage",
		"webhook",
	}

	var got []string
	typ := reflect.TypeOf(Config{})
	for i := range typ.NumField() {
		name, _, _ := strings.Cut(typ.Field(i).Tag.Get("yaml"), ",")
		if name != "" && name != "-" {
			got = append(got, name)
		}
	}
	sort.Strings(got)

	assert.Equal(t, want, got,
		"top-level config keys changed; renaming or removing one breaks deployed configurations")
}

// TestSchemaListenerKeys pins the listener slot names. These are the keys that
// silently changed from `plain` to `v3`/`v5` and left four shipped examples
// declaring listeners the broker never opened.
func TestSchemaListenerKeys(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		want []string
	}{
		{"server.mqtt.tcp", reflect.TypeOf(MQTTTCPConfig{}), []string{listenerNameMTLS, listenerNameTLS, "v3", "v5"}},
		{"server.mqtt.websocket", reflect.TypeOf(MQTTWebSocketConfig{}), []string{listenerNameMTLS, listenerNameTLS, "v3", "v5"}},
		{"server.http", reflect.TypeOf(HTTPConfig{}), []string{listenerNameMTLS, listenerNamePlain, listenerNameTLS}},
		{"server.coap", reflect.TypeOf(CoAPConfig{}), []string{"dtls", "mdtls", listenerNamePlain}},
		{"server.amqp", reflect.TypeOf(AMQPConfig{}), []string{listenerNameMTLS, listenerNamePlain, listenerNameTLS}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for i := range tc.typ.NumField() {
				name, _, _ := strings.Cut(tc.typ.Field(i).Tag.Get("yaml"), ",")
				if name != "" && name != "-" {
					got = append(got, name)
				}
			}
			sort.Strings(got)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestShippedConfigsDecodeStrictly is the regression guard for the discarded
// `plain` listener blocks. Every config file this repository ships must
// survive strict decoding; a file that fails here would, before strict
// decoding, have started a broker that silently ignored the failing key.
func TestShippedConfigsDecodeStrictly(t *testing.T) {
	roots := []string{"../examples", "../deployments"}

	var files []string
	for _, root := range roots {
		require.NoError(t, filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || filepath.Ext(path) != ".yaml" {
				return nil
			}
			// Compose files are docker's schema, not the broker's.
			if strings.Contains(filepath.Base(path), "compose") {
				return nil
			}
			files = append(files, path)
			return nil
		}))
	}
	require.NotEmpty(t, files, "no shipped config files found")

	for _, file := range files {
		t.Run(filepath.ToSlash(file), func(t *testing.T) {
			data, err := os.ReadFile(file)
			require.NoError(t, err)

			// Validation names secret files but never opens them, so this
			// holds on a workstation with no /run/secrets.
			if _, err = parse(data); err != nil {
				t.Fatalf("shipped config must decode strictly: %v", err)
			}
		})
	}
}

// TestLoadRejectsUnknownKey is the core promise of strict decoding: a
// misspelled key fails the load instead of silently dropping the setting it
// was meant to configure.
func TestLoadRejectsUnknownKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
server:
  mqtt:
    tcp:
      v3:
        addr: ":1883"
      plain:
        addr: ":1999"
`), 0o600))

	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plain")
}

// TestLoadMissingFileIsError covers the other half: a config path that does
// not exist must not quietly produce a default — and therefore unauthenticated
// — broker.
func TestLoadMissingFileIsError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")

	_, err := Load(missing)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConfigNotFound)

	cfg, err := LoadOptional(missing)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, Default(), cfg)
}

// TestValidateRejectsDuplicateBinds covers the collision that shipped in
// examples/tls-server.yaml, where an undeclared default listener shadowed a
// declared TLS one on the same port.
func TestValidateRejectsDuplicateBinds(t *testing.T) {
	t.Run("wildcard/same-port", func(t *testing.T) {
		cfg := Default()
		cfg.Server.MQTT.WebSocket.V5.Addr = testWSSAddr
		cfg.Server.MQTT.WebSocket.TLS.Addr = testWSSAddr
		cfg.Server.MQTT.WebSocket.TLS.TLS.CertFile = "cert.pem"
		cfg.Server.MQTT.WebSocket.TLS.TLS.KeyFile = "key.pem"

		err := cfg.validateNoDuplicateBinds()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "server.mqtt.websocket.v5.addr")
		assert.Contains(t, err.Error(), "server.mqtt.websocket.tls.addr")
	})

	t.Run("explicit-host/shadowed-by-wildcard", func(t *testing.T) {
		cfg := Default()
		cfg.Server.MQTT.TCP.V3.Addr = "127.0.0.1" + defaultTCPV3Addr
		cfg.Server.MQTT.TCP.V5.Addr = defaultTCPV3Addr

		require.Error(t, cfg.validateNoDuplicateBinds())
	})

	t.Run("distinct-hosts/same-port", func(t *testing.T) {
		cfg := Default()
		cfg.Server.MQTT.TCP.V3.Addr = "127.0.0.1" + defaultTCPV3Addr
		cfg.Server.MQTT.TCP.V5.Addr = "10.0.0.1" + defaultTCPV3Addr

		assert.NoError(t, cfg.validateNoDuplicateBinds())
	})

	t.Run("disabled-listener-never-conflicts", func(t *testing.T) {
		cfg := Default()
		cfg.Server.MQTT.TCP.V3.Addr = defaultTCPV3Addr
		cfg.Server.MQTT.TCP.V5.Addr = ""

		assert.NoError(t, cfg.validateNoDuplicateBinds())
	})

	t.Run("udp-may-reuse-a-tcp-port", func(t *testing.T) {
		cfg := Default()
		cfg.Server.MQTT.TCP.V3.Addr = testInternalAddr
		cfg.Server.CoAP.Plain.Addr = testInternalAddr

		assert.NoError(t, cfg.validateNoDuplicateBinds())
	})
}

// TestZeroMeansOneThing pins the rule that a value the operator writes is
// never replaced by a different one. Where zero is coherent — listener limits
// and timeouts — it is honoured. Where it is not, it is refused with a message
// saying to omit the key, which is what actually selects the default.
func TestZeroMeansOneThing(t *testing.T) {
	t.Run("zero is honoured where it is coherent", func(t *testing.T) {
		cfg := Default()
		cfg.Server.MQTT.TCP.V3.MaxConnections = 0 // unlimited
		cfg.Server.MQTT.TCP.V3.ReadTimeout = 0    // no deadline

		assert.NoError(t, cfg.Validate())
	})

	for _, tc := range []struct {
		name    string
		set     func(*Config)
		wantKey string
	}{
		{
			name:    "capture_workers",
			set:     func(c *Config) { c.QueueManager.CaptureWorkers = ptr(0) },
			wantKey: "queue_manager.capture_workers",
		},
		{
			name:    "capture_queue_depth",
			set:     func(c *Config) { c.QueueManager.CaptureQueueDepth = ptr(0) },
			wantKey: "queue_manager.capture_queue_depth",
		},
		{
			name:    "capture_drain_timeout",
			set:     func(c *Config) { c.QueueManager.CaptureDrainTimeout = ptr(time.Duration(0)) },
			wantKey: "queue_manager.capture_drain_timeout",
		},
		{
			name:    "identity_cache_size",
			set:     func(c *Config) { c.Auth.External.IdentityCacheSize = ptr(0) },
			wantKey: testIdentityCacheSizeKey,
		},
		{
			name:    "identity_cache_ttl",
			set:     func(c *Config) { c.Auth.External.IdentityCacheTTL = ptr(time.Duration(0)) },
			wantKey: testIdentityCacheTTLKey,
		},
		{
			name:    "hooks.timeout",
			set:     func(c *Config) { c.Hooks.Timeout = ptr(time.Duration(0)) },
			wantKey: "hooks.timeout",
		},
	} {
		t.Run("written zero rejected/"+tc.name, func(t *testing.T) {
			cfg := Default()
			tc.set(cfg)

			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantKey)
			assert.Contains(t, err.Error(), "omit the key to take the default")
		})
	}
}

// TestValidateListenAddress covers the shapes a listen address can take. The
// host is deliberately not resolved, so a name that only exists in the
// deployment's DNS still validates on a workstation.
func TestValidateListenAddress(t *testing.T) {
	for _, tc := range []struct {
		name    string
		addr    string
		wantErr string
	}{
		{name: "every interface", addr: defaultTCPV3Addr},
		{name: "loopback", addr: "127.0.0.1" + defaultTCPV3Addr},
		{name: "ipv6 loopback", addr: "[::1]:1883"},
		{name: "unresolved hostname", addr: "broker.internal" + defaultTCPV3Addr},
		{name: "highest port", addr: ":65535"},

		{name: "no colon", addr: "1883", wantErr: "is not a host:port address"},
		{name: "prose", addr: "the mqtt port", wantErr: "is not a host:port address"},
		{name: "non-numeric port", addr: ":mqtt", wantErr: "is not a number"},
		{name: "port zero", addr: ":0", wantErr: "choose a fixed port"},
		{name: "port above range", addr: ":65536", wantErr: "out of range"},
		{name: "negative port", addr: ":-1", wantErr: "out of range"},
		{name: "host with whitespace", addr: "my host" + defaultTCPV3Addr, wantErr: "contains whitespace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateListenAddress("server.mqtt.tcp.v3.addr", tc.addr)
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Contains(t, err.Error(), "server.mqtt.tcp.v3.addr",
				"every failure must name the key it came from")
		})
	}
}

// TestValidateRejectsMalformedListenerAddress checks that the shape rules are
// reached through Validate, not only by calling the helper directly.
func TestValidateRejectsMalformedListenerAddress(t *testing.T) {
	cfg := Default()
	cfg.Server.MQTT.TCP.V3.Addr = "1883"

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server.mqtt.tcp.v3.addr")
}

// TestSchemaKeysAreStable guards the full dotted key set against silent
// churn. The count is deliberately coarse: it catches a bulk rename or a
// dropped section without turning every additive change into a merge conflict.
func TestSchemaKeysAreStable(t *testing.T) {
	keys := configSchemaKeys(t)
	require.NotEmpty(t, keys)

	for _, must := range []string{
		"server.mqtt.tcp.v3.addr",
		"server.mqtt.tcp.v5.addr",
		"server.mqtt.websocket.v3.addr",
		"server.http.plain.addr",
		testAuthCalloutKey,
		"cluster.raft.enabled",
		"storage.type",
		"session.max_sessions",
		"ratelimit.enabled",
	} {
		assert.Contains(t, keys, must, "documented config key disappeared from the schema")
	}

	assert.NotContains(t, keys, "server.mqtt.tcp.plain",
		"server.tcp.plain was replaced by v3/v5; reintroducing it would resurrect the silent-listener bug")
	assert.NotContains(t, keys, "server.mqtt.websocket.plain")

	// The Badger fsync key names its engine: it does not reach the queue
	// append-only log, and the old name read as if it fsynced all storage.
	assert.Contains(t, keys, "storage.badger_sync_writes")
	assert.NotContains(t, keys, "storage.sync_writes")

	// MQTT transports moved under server.mqtt so `tcp` cannot be read as a
	// generic listener sitting beside server.amqp.
	assert.NotContains(t, keys, "server.tcp",
		"MQTT transports live under server.mqtt")
	assert.NotContains(t, keys, "server.websocket")
}

// docsConfigSkipMarker lets one documentation block opt out of
// TestDocumentedConfigsLoad. It is for blocks that must not load: a removed
// key shown as a negative example, or a reference section quoted in isolation
// whose siblings live in neighbouring blocks.
const docsConfigSkipMarker = "fluxmq:config-skip"

var docsYAMLBlock = regexp.MustCompile("(?s)```ya?ml\n(.*?)```")

// TestDocumentedConfigsLoad extends TestShippedConfigsDecodeStrictly to the
// documentation. Strict decoding turned every stale example into a broker that
// refuses to start, so a documented key that no longer exists is now a
// production incident waiting for the first operator who copies it — which is
// how `server.mqtt.websocket.plain` survived the listener rename on the very
// page that documented it.
//
// A fenced yaml block counts as broker configuration when at least one of its
// top-level keys is a top-level config key. That leaves Compose files,
// manifests, and payload samples alone without needing a marker on each.
func TestDocumentedConfigsLoad(t *testing.T) {
	topLevel := map[string]bool{}
	typ := reflect.TypeOf(Config{})
	for i := range typ.NumField() {
		name, _, _ := strings.Cut(typ.Field(i).Tag.Get("yaml"), ",")
		if name != "" && name != "-" {
			topLevel[name] = true
		}
	}

	files := []string{"../README.md"}
	require.NoError(t, filepath.WalkDir("../docs/content/docs", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".md" {
			files = append(files, path)
		}
		return nil
	}))

	checked := 0
	for _, file := range files {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		page := string(data)

		for _, match := range docsYAMLBlock.FindAllStringSubmatchIndex(page, -1) {
			preamble, block := page[:match[0]], page[match[2]:match[3]]
			if skippedByMarker(preamble) || !isBrokerConfigBlock(block, topLevel) {
				continue
			}

			checked++
			line := strings.Count(preamble, "\n") + 1
			t.Run(fmt.Sprintf("%s:%d", filepath.ToSlash(file), line), func(t *testing.T) {
				if _, err := parse([]byte(block)); err != nil {
					t.Fatalf("documented configuration must load: %v", err)
				}
			})
		}
	}
	require.NotZero(t, checked, "no documented configuration blocks found")
}

// skippedByMarker reports whether a skip marker sits immediately above a
// fenced block. The window is deliberately short: a marker further up the page
// must not silence an unrelated example below it.
func skippedByMarker(preamble string) bool {
	lines := strings.Split(strings.TrimRight(preamble, "\n"), "\n")
	for i := len(lines) - 1; i >= 0 && i > len(lines)-5; i-- {
		if strings.Contains(lines[i], docsConfigSkipMarker) {
			return true
		}
	}
	return false
}

func isBrokerConfigBlock(block string, topLevel map[string]bool) bool {
	var document map[string]any
	if err := yaml.Unmarshal([]byte(block), &document); err != nil {
		return false
	}
	for key := range document {
		if topLevel[key] {
			return true
		}
	}
	return false
}
