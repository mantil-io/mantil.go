package mantil

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRequest(t *testing.T) {
	data := []struct {
		filename string
		typ      RequestType
		methods  []string
	}{
		{"api-gateway.json", APIGateway, []string{"MethodName"}},
		{"ws-connect.json", WSConnect, []string{"Connect", ""}},
		{"ws-disconnect.json", WSDisconnect, []string{"Disconnect", ""}},
		{"ws-message.json", WSMessage, []string{"Message", ""}},
		{"streaming.json", Streaming, []string{"methodName"}},
		{"console_test.json", RequestTypeUnknown, []string{"myMethod"}},
		{"bad_attribute_type.json", RequestTypeUnknown, []string{"method"}},
	}

	for _, d := range data {
		buf, err := ioutil.ReadFile("testdata/" + d.filename)
		require.NoError(t, err)
		req := parseRequest(buf)
		require.Equal(t, d.typ, req.Type, d.filename)
		require.Equal(t, buf, req.Raw)
		require.Equal(t, d.methods, req.Methods, d.filename)
		if req.Type == WSConnect || req.Type == WSDisconnect {
			require.Nil(t, req.Body)
			continue
		}
		var kv struct {
			Key string
		}
		err = json.Unmarshal(req.Body, &kv)
		require.NoError(t, err, d.filename)
		require.Equal(t, "value", kv.Key, d.filename)

		fmt.Printf("remoteIP: %s", req.RemoteIP())
	}
}

func TestRemoteIP(t *testing.T) {
	buf, err := ioutil.ReadFile("testdata/api-gateway.json")
	require.NoError(t, err)
	req := parseRequest(buf)
	require.Equal(t, "93.136.54.42", req.RemoteIP())
}
