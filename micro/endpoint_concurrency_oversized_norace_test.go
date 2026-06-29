//go:build !race

package micro_test

import (
	"context"
	"errors"
	"testing"
	"time"

	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	natsmicro "github.com/nats-io/nats.go/micro"
	ffmicro "github.com/olpie101/fast-forward/micro"
)

// TestAsyncOversizedResponseDropsReply characterizes the accepted residual
// publish-failure behavior: in async mode, a response larger than the server's
// max_payload fails to publish and the reply is silently dropped, so the
// requester times out. This is the user-visible symptom of the documented
// residual race.
//
// It runs only in non-race builds (//go:build !race): the forced Respond publish
// failure writes nats.go's internal request.respondError after Handle returns,
// which is exactly the accepted race the default -race gate must not trip. The
// dedicated race reproduction lives behind the racerepro build tag.
func TestAsyncOversizedResponseDropsReply(t *testing.T) {
	opts := natsservertest.DefaultTestOptions
	opts.Port = -1
	opts.MaxPayload = 1024
	srv := natsservertest.RunServer(&opts)
	defer func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	}()

	svcConn, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	defer svcConn.Close()

	svc, err := natsmicro.AddService(svcConn, natsmicro.Config{Name: "oversized", Version: "0.0.1"})
	if err != nil {
		t.Fatalf("AddService: %v", err)
	}
	defer func() { _ = svc.Stop() }()
	group := svc.AddGroup("oversized")

	big := make([]byte, 64*1024) // far exceeds the 1KB max_payload
	handler := func(ctx context.Context, req any) (any, error) { return big, nil }

	err = ffmicro.AddEndpoint("echo", group, handler,
		ffmicro.WithDecoderFn(func(b []byte) (any, error) { return b, nil }),
		ffmicro.WithEncoderFn(func(v any) ([]byte, error) { return v.([]byte), nil }),
		ffmicro.WithConcurrency(2),
	)
	if err != nil {
		t.Fatalf("AddEndpoint: %v", err)
	}
	if err := svcConn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reqConn, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("request connect: %v", err)
	}
	defer reqConn.Close()

	_, err = reqConn.Request("oversized.echo", nil, 500*time.Millisecond)
	if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("want timeout (oversized async response dropped), got %v", err)
	}
}
