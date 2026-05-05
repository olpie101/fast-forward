package nats

import (
	"fmt"
	"os"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
)

var testServer *server.Server

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "store-nats-test-jetstream-*")
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
