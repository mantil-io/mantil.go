package logs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// to test using ngs
// prepare logs-listener.creds and logs-publisher.creds files in some folder
// and then start test with:
//
// NGS_CREDS_DIR='/Users/ianic/mantil-io/mantil/cli' go test -v
//
//
func TestLambdaInvoke(t *testing.T) {
	var conf ListenerConfig

	if dir, ok := os.LookupEnv("NGS_CREDS_DIR"); ok {
		t.Logf("using NGS")
		lc, err := ioutil.ReadFile(filepath.Join(dir, "logs-listener.creds"))
		require.NoError(t, err)
		pc, err := ioutil.ReadFile(filepath.Join(dir, "logs-publisher.creds"))
		require.NoError(t, err)

		conf.ServerURL = defaultServerURL
		conf.ListenerJWT = string(lc)
		conf.PublisherJWT = string(pc)
	} else {
		t.Logf("using local nats server")
		// start test server
		s := RunServerOnPort(TEST_PORT)
		defer s.Shutdown()
		sUrl := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
		conf.ServerURL = sUrl
	}

	// use it for all connections
	lambdaListener := func(logSink func(chan []byte), rsp interface{}) *LambdaListener {
		conf.LogSink = logSink
		conf.Rsp = rsp
		ll, err := NewLambdaListener(conf)
		require.NoError(t, err)
		return ll
	}

	t.Run("one response", func(t *testing.T) {
		rsp := bytes.NewBuffer(nil)
		ll := lambdaListener(nil, rsp)

		go func() {
			cb, err := LambdaResponse(ll.Headers())
			require.NoError(t, err)
			cb("pero", nil)
		}()
		require.NoError(t, ll.Done(context.Background()))

		srsp := rsp.String()
		require.Equal(t, "pero", srsp)
		t.Logf("response: %s", srsp)
	})

	t.Run("nil response", func(t *testing.T) {
		rsp := bytes.NewBuffer(nil)
		ll := lambdaListener(nil, rsp)

		go func() {
			cb, err := LambdaResponse(ll.Headers())
			require.NoError(t, err)
			cb(nil, nil)
		}()
		require.NoError(t, ll.Done(context.Background()))
		require.Len(t, rsp.Bytes(), 0)
	})

	t.Run("no response", func(t *testing.T) {
		ll := lambdaListener(nil, nil)

		go func() {
			cb, err := LambdaResponse(ll.Headers())
			require.NoError(t, err)
			cb("something", nil)
		}()
		require.NoError(t, ll.Done(context.Background()))
	})

	t.Run("error", func(t *testing.T) {
		ll := lambdaListener(nil, nil)

		go func() {
			cb, err := LambdaResponse(ll.Headers())
			require.NoError(t, err)
			cb(nil, fmt.Errorf("bum"))
		}()

		err := ll.Done(context.Background())
		require.Error(t, err)
		t.Logf("error response: %s", err)
		re := &ErrRemoteError{}
		require.ErrorAs(t, err, &re)
		require.Equal(t, "bum", re.Error())
	})

	t.Run("invoke logs", func(t *testing.T) {
		log.SetFlags(0)
		log.SetOutput(io.Discard)

		rsp := bytes.NewBuffer(nil)

		var logs []string
		logSink := func(ch chan []byte) {
			for line := range ch {
				logs = append(logs, string(line))
			}
		}

		ll := lambdaListener(logSink, rsp)
		expectedRsp := "Jozo"
		expectedLogs := []string{"Samir", "Amir", "Mir", "Ir", "i mali R jos ne ide u skolu"}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			cb, err := LambdaResponse(ll.Headers())
			require.NoError(t, err)
			for _, l := range expectedLogs {
				log.Printf(l)
			}
			cb(expectedRsp, nil)
		}()

		require.NoError(t, ll.Done(context.Background()))
		require.Equal(t, logs, expectedLogs)
		require.Equal(t, rsp.String(), expectedRsp)
		t.Logf("logs: %#v", logs)
		t.Logf("rsp: %#v", rsp.String())
	})
}
