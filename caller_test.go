package mantil

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mantil-io/mantil.go/er"
	"github.com/mantil-io/mantil.go/proto"
	"github.com/stretchr/testify/require"
)

// demo api entrypoint structure
type Hello struct{}

type WorldRequest struct {
	Name string
}
type WorldResponse struct {
	Response string
}

func (h *Hello) Invoke() {}

func (h *Hello) World(ctx context.Context, req *WorldRequest) (*WorldResponse, error) {
	if req == nil {
		return nil, nil
	}
	rsp := WorldResponse{Response: "Hello, " + req.Name}
	return &rsp, nil
}

func (h *Hello) ValueInRequest(ctx context.Context, req WorldRequest) (*WorldResponse, error) {
	rsp := WorldResponse{Response: "Hello, " + req.Name}
	return &rsp, nil
}

func (h *Hello) ValueInReqAndRsp(ctx context.Context, req WorldRequest) (WorldResponse, error) {
	rsp := WorldResponse{Response: "Hello, " + req.Name}
	return rsp, nil
}

func (h *Hello) ArrayResponse(ctx context.Context, req WorldRequest) ([]WorldResponse, error) {
	var rsps []WorldResponse
	for i := 0; i < 10; i++ {
		rsp := WorldResponse{Response: fmt.Sprintf("Hello, %d", i)}
		rsps = append(rsps, rsp)
	}
	return rsps, nil
}

func (h *Hello) NoCtx(req WorldRequest) (*WorldResponse, error) {
	rsp := WorldResponse{Response: "Hello, " + req.Name}
	return &rsp, nil
}

func (h *Hello) Error(ctx context.Context) (*WorldResponse, error) {
	return nil, fmt.Errorf("method call failed")
}

// this will panic if req is nil
func (h *Hello) Panic(ctx context.Context, req *WorldRequest) (*WorldResponse, error) {
	rsp := WorldResponse{Response: "Hello, " + req.Name}
	return &rsp, nil
}

func (h *Hello) Ping() string {
	return "pong"
}

func TestCaller(t *testing.T) {
	SetLogger(nil) // remove log lines from test output
	cases := []struct {
		method     string
		params     string
		body       string
		rsp        string
		statusCode int
		error      string
	}{
		{
			method:     "ValueInRequest",
			params:     `{}`,
			body:       "",
			rsp:        `{"Response":"Hello, "}`,
			statusCode: 200,
		},
		{
			method:     "ValueInReqAndRsp",
			params:     `{}`,
			body:       "",
			rsp:        `{"Response":"Hello, "}`,
			statusCode: 200,
		},
		{
			method:     "world",
			params:     `{}`,
			body:       "",
			rsp:        "",
			statusCode: 204,
		},
		{
			method:     "ValueInReqAndRsp",
			params:     `{}`,
			body:       `{"name": "Pero"}`,
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "ValueInRequest",
			params:     `{"name": "Pero"}`,
			body:       "",
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "world",
			params:     `{}`,
			body:       `{"name": "Pero"}`,
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "world",
			params:     `{"name": "Pero"}`,
			body:       "",
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "noctx", // method without ctx as first parameter
			params:     `{}`,
			body:       `{"name": "Pero"}`,
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "noctx", // method without ctx as first parameter
			params:     `{"name": "Pero"}`,
			body:       "",
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "no-Ctx", // case and '-' insensitive
			params:     `{}`,
			body:       `{"name": "Pero"}`,
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "no-Ctx", // case and '-' insensitive
			params:     `{"name": "Pero"}`,
			body:       "",
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "", // call Root method
			params:     `{}`,
			body:       "",
			rsp:        "",
			statusCode: 204,
		},
		{
			method:     "panic",
			params:     `{}`,
			body:       "",
			rsp:        "",
			statusCode: 500,
			error:      "PANIC runtime error: invalid memory address or nil pointer dereference",
		},
		{
			method:     "arrayresponse",
			params:     `{}`,
			body:       "",
			rsp:        "[{\"Response\":\"Hello, 0\"},{\"Response\":\"Hello, 1\"},{\"Response\":\"Hello, 2\"},{\"Response\":\"Hello, 3\"},{\"Response\":\"Hello, 4\"},{\"Response\":\"Hello, 5\"},{\"Response\":\"Hello, 6\"},{\"Response\":\"Hello, 7\"},{\"Response\":\"Hello, 8\"},{\"Response\":\"Hello, 9\"}]",
			statusCode: 200,
		},
		{
			method:     "ping",
			params:     `{}`,
			body:       "",
			rsp:        "pong",
			statusCode: 200,
		},
	}

	api := &Hello{}

	t.Run("caller", func(t *testing.T) {
		for i, c := range cases {
			ctx := context.Background()
			var reqPayload []byte
			if len(c.body) > 0 {
				reqPayload = []byte(c.body)
			}
			reqParams := make(map[string]string)
			json.Unmarshal([]byte(c.params), &reqParams)
			caller := newCaller(api)

			rsp := caller.call(ctx, reqPayload, reqParams, c.method)
			if c.error == "" {
				require.NoError(t, rsp.err)
			} else {
				require.Error(t, rsp.err)
				require.Equal(t, c.error, rsp.err.Error())
			}
			require.Equal(t, c.statusCode, rsp.StatusCode(), "case %d", i)
			require.Equal(t, c.rsp, string(rsp.payload))
		}
	})

	t.Run("api gateway", func(t *testing.T) {
		handler := newHandler(api)
		for i, c := range cases {
			ctx := context.Background()
			params := make(map[string]string)
			json.Unmarshal([]byte(c.params), &params)

			aReq := events.APIGatewayProxyRequest{
				Path:                  "path",
				HTTPMethod:            "method",
				PathParameters:        map[string]string{"proxy": c.method},
				QueryStringParameters: params,
				Body:                  c.body,
			}
			reqPayload, _ := json.Marshal(aReq)
			_, rspp := handler.invoke(ctx, reqPayload)
			rspPayload, err := rspp.AsAPIGateway()
			require.NoError(t, err)

			var aRsp events.APIGatewayProxyResponse
			err = json.Unmarshal(rspPayload, &aRsp)
			require.NoError(t, err)
			require.Equal(t, c.rsp, aRsp.Body, "case %d", i)
			require.Equal(t, c.statusCode, aRsp.StatusCode)
			require.Equal(t, c.error, aRsp.Headers["x-api-error"])
		}
	})

	t.Run("streaming message", func(t *testing.T) {
		handler := newHandler(api)
		for i, c := range cases {
			if i == 4 || i == 6 || i == 8 || i == 10 {
				continue
			}
			ctx := context.Background()
			msg := proto.Message{
				ConnectionID: "1234567890",
				Inbox:        "my-inbox",
				URI:          "api." + c.method,
				Payload:      []byte(c.body),
			}
			reqPayload, _ := json.Marshal(msg)
			req, rsp := handler.invoke(ctx, reqPayload)
			rm, err := rsp.AsStreaming(req)
			if c.error == "" {
				require.NoError(t, err, "case %d", i)
				require.Equal(t, proto.Response, rm.Type)
				require.Equal(t, msg.ConnectionID, rm.ConnectionID)
				require.Equal(t, msg.Inbox, rm.Inbox)
				require.Equal(t, msg.URI, rm.URI)
				require.Equal(t, c.rsp, string(rm.Payload))
			} else {
				require.Equal(t, err.Error(), c.error)
			}
		}
	})

	t.Run("raw message", func(t *testing.T) {
		handler := newHandler(api)
		for i, c := range cases {
			if i == 2 || i == 12 {
				continue
			}
			params := make(map[string]string)
			json.Unmarshal([]byte(c.params), &params)
			ctx := context.Background()
			msg := struct {
				URI                   string
				Body                  string
				QueryStringParameters map[string]string
			}{
				URI:                   c.method,
				Body:                  c.body,
				QueryStringParameters: params,
			}
			reqPayload, _ := json.Marshal(msg)
			_, rsp := handler.invoke(ctx, reqPayload)
			rspPayload, err := rsp.Raw()
			if c.error == "" {
				require.NoError(t, err, "case %d", i)
				require.Equal(t, c.rsp, string(rspPayload), "case %d", i)
			} else {
				require.Equal(t, c.error, err, "case %d", i)
			}
		}
	})

}

// Example of how to call method with response of the array of some structure.
//
// Razmisljam kako modelirati api kada ima potrebu vratiti
// vise odgovora na neki streaming kanal (websocket prema klijentima).
// A da tim kanalom ide poruka po pourka. Obicno postoji neki limit na velicinu poruke,
// pa ako saljem jednu po jednu necu naletiti na njega.
func TestCallMethodWithArrayResponse(t *testing.T) {
	api := &Hello{}
	i := interface{}(api)
	value := reflect.ValueOf(i)
	typ := reflect.TypeOf(i)

	var response []reflect.Value
	methodName := "arrayresponse"
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		if methodName != strings.ToLower(method.Name) {
			continue
		}
		var args []reflect.Value
		args = append(args, value)
		args = append(args, reflect.ValueOf(context.TODO()))
		args = append(args, reflect.ValueOf((WorldRequest{})))
		response = method.Func.Call(args)
	}

	v := response[0].Interface()
	if reflect.TypeOf(v).Kind() == reflect.Slice {
		//fmt.Printf("is slice\n")
		switch reflect.TypeOf(v).Kind() {
		case reflect.Slice:
			s := reflect.ValueOf(v)

			for i := 0; i < s.Len(); i++ {
				e := s.Index(i)
				buf, err := json.Marshal(e.Interface())
				if err != nil {
					log.Fatal(err)
				}
				expected := fmt.Sprintf(`{"Response":"Hello, %d"}`, i)
				require.Equal(t, expected, string(buf))
				//t.Logf("%s\n", buf)
			}
		}
	}
}

func TestStatusCodeInResponse(t *testing.T) {
	r := response{
		err: er.NewBadRequestError("missing"),
	}

	require.Equal(t, http.StatusBadRequest, r.StatusCode())
	require.Equal(t, http.StatusBadRequest, r.ErrorCode())
	require.Equal(t, "missing", r.Error())
}
