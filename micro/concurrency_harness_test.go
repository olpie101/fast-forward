package micro_test

import (
	"os"
	"sync/atomic"
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	natsmicro "github.com/nats-io/nats.go/micro"
)

// testServer is the embedded core-NATS server shared by the package's
// integration-style tests (and the untracked concurrency benchmark).
var testServer *natsserver.Server

func TestMain(m *testing.M) {
	opts := natsservertest.DefaultTestOptions
	opts.Port = -1
	testServer = natsservertest.RunServer(&opts)

	code := m.Run()

	testServer.Shutdown()
	testServer.WaitForShutdown()
	os.Exit(code)
}

// concurrencyProbe tracks the peak number of handler invocations that were ever
// in flight simultaneously. enter()/leave() bracket each invocation.
type concurrencyProbe struct {
	inFlight int64
	peak     int64
}

func (p *concurrencyProbe) enter() {
	cur := atomic.AddInt64(&p.inFlight, 1)
	for {
		old := atomic.LoadInt64(&p.peak)
		if cur <= old || atomic.CompareAndSwapInt64(&p.peak, old, cur) {
			break
		}
	}
}

func (p *concurrencyProbe) leave() { atomic.AddInt64(&p.inFlight, -1) }

func (p *concurrencyProbe) peakValue() int64 { return atomic.LoadInt64(&p.peak) }

// newServiceT spins up a micro service on the embedded server and returns its
// root group plus the service and request connections. All are cleaned up when
// the test ends. After registering endpoints on the returned group, call
// svcConn.Flush() to guarantee the subscriptions are active server-side before
// issuing requests.
func newServiceT(t *testing.T, name string) (natsmicro.Group, *nats.Conn, *nats.Conn) {
	t.Helper()

	svcConn, err := nats.Connect(testServer.ClientURL())
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}

	svc, err := natsmicro.AddService(svcConn, natsmicro.Config{
		Name:    name,
		Version: "0.0.1",
	})
	if err != nil {
		svcConn.Close()
		t.Fatalf("AddService: %v", err)
	}

	reqConn, err := nats.Connect(testServer.ClientURL())
	if err != nil {
		_ = svc.Stop()
		svcConn.Close()
		t.Fatalf("request connect: %v", err)
	}

	t.Cleanup(func() {
		_ = svc.Stop()
		svcConn.Close()
		reqConn.Close()
	})

	return svc.AddGroup(name), svcConn, reqConn
}
