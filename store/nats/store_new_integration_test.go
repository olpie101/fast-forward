package nats

import (
	"strconv"
	"strings"
	"testing"

	"github.com/modernice/goes/codec"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func TestStoreNewIntegration(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		s, _, nc := newStore(t, "ok")
		if s == nil {
			t.Fatal("Store is nil")
		}
		// P1-4: subFn cannot be compared (unexported, method values are not
		// equality-comparable). Pin the non-legacy branch by checking that the
		// embedded server reports a version that routes through s.subscribe at
		// store.go:80 (major>=2 && minor>=10). End-to-end coverage is provided
		// by TestQueryIntegration's insert+query round-trip — that path only
		// succeeds when subFn == s.subscribe.
		v := nc.ConnectedServerVersion()
		major, minor := parseMajorMinor(t, v)
		if !(major > 2 || (major == 2 && minor >= 10)) {
			t.Fatalf("connected server version %q does not select the non-legacy branch", v)
		}
	})

	t.Run("missing_write_lease_kv", func(t *testing.T) {
		nc, err := nats.Connect(testServer.ClientURL())
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		t.Cleanup(func() { nc.Close() })

		_, err = New(nc, codec.New(),
			WithLogger(zap.NewNop().Sugar()),
			WithVersionKV(nilVersionKV(t, nc, "version_missing_lease_"+shortID())),
		)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		want := "store init: write lease kv cannot be nil"
		if err.Error() != want {
			t.Fatalf("err = %q, want %q", err.Error(), want)
		}
	})

	t.Run("missing_version_kv", func(t *testing.T) {
		nc, err := nats.Connect(testServer.ClientURL())
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		t.Cleanup(func() { nc.Close() })

		_, err = New(nc, codec.New(),
			WithLogger(zap.NewNop().Sugar()),
			WithWriteLeaseKV(nilLeaseKV(t, nc, "lease_missing_version_"+shortID())),
		)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		want := "store init: aggregate version kv cannot be nil"
		if err.Error() != want {
			t.Fatalf("err = %q, want %q", err.Error(), want)
		}
	})

	t.Run("missing_codec", func(t *testing.T) {
		nc, err := nats.Connect(testServer.ClientURL())
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		t.Cleanup(func() { nc.Close() })

		_, err = New(nc, nil,
			WithLogger(zap.NewNop().Sugar()),
			WithWriteLeaseKV(nilLeaseKV(t, nc, "lease_missing_codec_"+shortID())),
			WithVersionKV(nilVersionKV(t, nc, "version_missing_codec_"+shortID())),
		)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		want := "store init: encoding cannot be nil"
		if err.Error() != want {
			t.Fatalf("err = %q, want %q", err.Error(), want)
		}
	})
}

func TestStoreNewIntegrationIsLegacyMalformed(t *testing.T) {
	t.Skip("store/nats/store.go:202-219 isLegacy panics on malformed server version: parts[0]/parts[1] indexed without len check; desired: return error if len(parts) < 2. The embedded nats-server reports a real version so this path is not naturally reachable from the integration harness; pure-unit characterizes the panic via TestIsLegacyShortVersionPanics.")
}

func parseMajorMinor(t *testing.T, version string) (int, int) {
	t.Helper()
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		t.Fatalf("unparseable version %q", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		t.Fatalf("major: %v", err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatalf("minor: %v", err)
	}
	return major, minor
}
