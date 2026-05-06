package nats

import (
	"testing"

	"github.com/modernice/goes/codec"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func TestStoreNewIntegration(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		s, _, _ := newStore(t, "ok")
		if s == nil {
			t.Fatal("Store is nil")
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
	t.Skip("checkServerVersion now returns ErrUnsupportedServerVersion for malformed/<2.10 versions; covered by TestCheckServerVersion. Reaching this branch in integration would require a faked nats.Conn, which is out of scope for the embedded-server harness.")
}
