//go:build racerepro

package micro_test

import (
	"context"
	"sync"
	"testing"
	"time"

	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	natsmicro "github.com/nats-io/nats.go/micro"
	ffmicro "github.com/olpie101/fast-forward/micro"
)

// TestAsyncRespondErrorRace deliberately reproduces the accepted residual race
// on nats.go's internal request.respondError. It is QUARANTINED behind the
// racerepro build tag and excluded from default and CI gates.
//
// Run on demand with:
//
//	go test -race -tags=racerepro ./micro -run TestAsyncRespondErrorRace -count=1
//
// In async mode a Respond publish failure (here: a response larger than the
// server max_payload) writes request.respondError (request.go:113) from the
// worker goroutine after Handle has returned, racing reqHandler's read
// (service.go:700). The race detector should report the data race.
func TestAsyncRespondErrorRace(t *testing.T) {
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

	svc, err := natsmicro.AddService(svcConn, natsmicro.Config{Name: "racerepro", Version: "0.0.1"})
	if err != nil {
		t.Fatalf("AddService: %v", err)
	}
	defer func() { _ = svc.Stop() }()
	group := svc.AddGroup("racerepro")

	big := make([]byte, 64*1024)
	handler := func(ctx context.Context, req any) (any, error) { return big, nil }

	err = ffmicro.AddEndpoint("echo", group, handler,
		ffmicro.WithDecoderFn(func(b []byte) (any, error) { return b, nil }),
		ffmicro.WithEncoderFn(func(v any) ([]byte, error) { return v.([]byte), nil }),
		ffmicro.WithConcurrency(8),
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

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = reqConn.Request("racerepro.echo", nil, 200*time.Millisecond)
		}()
	}
	wg.Wait()
}
