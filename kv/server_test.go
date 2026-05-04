package kv

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

var testServer *server.Server

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "kv-test-jetstream-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "MkdirTemp: %v\n", err)
		os.Exit(1)
	}

	opts := natsservertest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = tmpDir

	testServer = natsservertest.RunServer(&opts)

	code := m.Run()

	testServer.Shutdown()
	testServer.WaitForShutdown()
	_ = os.RemoveAll(tmpDir)
	os.Exit(code)
}

// sanitizeBucketName makes a t.Name() safe for JetStream bucket naming
// (which only allows ^[a-zA-Z0-9_-]+$).
func sanitizeBucketName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// newKV creates a per-test bucket on the embedded server, returns a typed
// KeyValuer wrapper, the underlying nats.KeyValue (legacy API), and the
// connection. Cleanup drains the connection and deletes the bucket.
func newKV[T MarshalerUnmarshaler](t *testing.T) (KeyValuer[T], nats.KeyValue, *nats.Conn) {
	t.Helper()

	nc, err := nats.Connect(testServer.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatalf("nc.JetStream: %v", err)
	}

	bucketName := fmt.Sprintf("kv_test_%s_%s",
		sanitizeBucketName(t.Name()),
		strings.ReplaceAll(uuid.NewString(), "-", "")[:8],
	)

	bucket, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  bucketName,
		Storage: nats.MemoryStorage,
	})
	if err != nil {
		nc.Close()
		t.Fatalf("CreateKeyValue %q: %v", bucketName, err)
	}

	t.Cleanup(func() {
		_ = js.DeleteKeyValue(bucketName)
		_ = nc.Drain()
		// Give Drain a brief moment to flush; Drain returns immediately and
		// completes asynchronously.
		deadline := time.After(2 * time.Second)
		for nc.Status() != nats.CLOSED {
			select {
			case <-deadline:
				nc.Close()
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	})

	return New[T](bucket), bucket, nc
}
