package logs

import (
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

const TEST_PORT = 8369

func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	return RunServerWithOptions(&opts)
}

func RunServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}

func TestSomething(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	sUrl := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	nc, err := nats.Connect(sUrl)
	require.NoError(t, err)
	defer nc.Close()
}
