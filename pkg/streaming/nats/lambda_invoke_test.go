package nats

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLambdaInvokeOneResponse(t *testing.T) {
	ll, err := NewLambdaListener()
	require.NoError(t, err)

	go func() {
		cb, err := LambdaResponse(ll.Headers())
		require.NoError(t, err)
		cb("pero", nil)
	}()

	rsp, err := ll.RawResponse(context.Background())
	require.Equal(t, "pero", string(rsp))
	t.Logf("response: %s", rsp)
}

func TestLambdaInvokeNilResponse(t *testing.T) {
	ll, err := NewLambdaListener()
	require.NoError(t, err)

	go func() {
		cb, err := LambdaResponse(ll.Headers())
		require.NoError(t, err)
		cb(nil, nil)
	}()

	rsp, err := ll.RawResponse(context.Background())
	require.Nil(t, rsp)
}

func TestLambdaInvokeError(t *testing.T) {
	ll, err := NewLambdaListener()
	require.NoError(t, err)

	go func() {
		cb, err := LambdaResponse(ll.Headers())
		require.NoError(t, err)
		cb(nil, fmt.Errorf("bum"))
	}()

	rsp, err := ll.RawResponse(context.Background())
	require.Nil(t, rsp)
	t.Logf("error response: %s", err)
	re := &ErrRemoteError{}
	require.ErrorAs(t, err, &re)
	require.Equal(t, "bum", re.Error())
}

func TestLambdaInvokeMultipleResponses(t *testing.T) {
	ll, err := NewLambdaListener()
	require.NoError(t, err)
	rsps := []string{"jozo", "bozo", "misteriozo"}

	go func() {
		cb, err := LambdaResponse(ll.Headers())
		require.NoError(t, err)
		cb(rsps, nil)
	}()

	ch, err := ll.Responses(context.Background())
	require.NoError(t, err)
	for buf := range ch {
		require.Equal(t, rsps[0], string(buf))
		rsps = rsps[1:]
		t.Logf("response: %s", buf)
	}
}

func TestLambdaInvokeLogs(t *testing.T) {
	log.SetFlags(0)
	log.SetOutput(io.Discard)

	ll, err := NewLambdaListener()
	require.NoError(t, err)
	rsps := []string{"jozo", "bozo", "misteriozo"}
	logs := []string{"Samir", "Amir", "Mir", "Ir", "i mali R jos ne ide u skolu"}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		cb, err := LambdaResponse(ll.Headers())
		require.NoError(t, err)
		for _, l := range logs {
			log.Printf(l)
		}
		cb(rsps, nil)
	}()

	go func() {
		defer wg.Done()
		ch, err := ll.Logs(context.Background())
		require.NoError(t, err)
		for buf := range ch {
			require.Equal(t, logs[0], string(buf))
			logs = logs[1:]
			t.Logf("log: %s", buf)
		}
		require.Len(t, logs, 0)
	}()

	ch, err := ll.Responses(context.Background())
	require.NoError(t, err)
	for buf := range ch {
		require.Equal(t, rsps[0], string(buf))
		rsps = rsps[1:]
		t.Logf("response: %s", buf)
	}
	require.Len(t, rsps, 0)

	wg.Wait()
}
