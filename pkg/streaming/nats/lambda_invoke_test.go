package nats

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestLambdaInvoke(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	sUrl := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	nc, cerr := nats.Connect(sUrl)
	require.NoError(t, cerr)
	nc.Close()

	lambdaListener := func(logSink func(chan []byte), rsp interface{}) *LambdaListener {
		c := Config{
			ServerURL: sUrl,
			LogSink:   logSink,
			Rsp:       rsp,
		}
		ll, err := NewLambdaListenerFromConfig(c)
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
		require.NoError(t, ll.Done())

		//rsp, err := ll.RawResponse(context.Background())
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
		require.NoError(t, ll.Done())
		require.Len(t, rsp.Bytes(), 0)
	})

	t.Run("no response", func(t *testing.T) {
		ll := lambdaListener(nil, nil)

		go func() {
			cb, err := LambdaResponse(ll.Headers())
			require.NoError(t, err)
			cb(nil, nil)
		}()
		require.NoError(t, ll.Done())
	})

	t.Run("error", func(t *testing.T) {
		ll := lambdaListener(nil, nil)

		go func() {
			cb, err := LambdaResponse(ll.Headers())
			require.NoError(t, err)
			cb(nil, fmt.Errorf("bum"))
		}()

		cerr = ll.Done()
		require.Error(t, cerr)
		t.Logf("error response: %s", cerr)
		re := &ErrRemoteError{}
		require.ErrorAs(t, cerr, &re)
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

		require.NoError(t, ll.Done())
		require.Equal(t, logs, expectedLogs)
		require.Equal(t, rsp.String(), expectedRsp)
		t.Logf("logs: %#v", logs)
		t.Logf("rsp: %#v", rsp.String())
	})
}

// func TestLambdaInvokeMultipleResponses(t *testing.T) {
// 	ll, err := NewLambdaListener(nil, )
// 	require.NoError(t, err)
// 	rsps := []string{"jozo", "bozo", "misteriozo"}

// 	go func() {
// 		cb, err := LambdaResponse(ll.Headers())
// 		require.NoError(t, err)
// 		cb(rsps, nil)
// 	}()

// 	ch, err := ll.Responses(context.Background())
// 	require.NoError(t, err)
// 	for buf := range ch {
// 		require.Equal(t, rsps[0], string(buf))
// 		rsps = rsps[1:]
// 		t.Logf("response: %s", buf)
// 	}
// }
