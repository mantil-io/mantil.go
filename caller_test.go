package mantil

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
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

func TestCaller(t *testing.T) {
	cases := []struct {
		method     string
		req        string
		rsp        string
		statusCode int
		error      string
	}{
		{
			method:     "ValueInRequest",
			req:        "",
			rsp:        `{"Response":"Hello, "}`,
			statusCode: 200,
		},
		{
			method:     "ValueInReqAndRsp",
			req:        "",
			rsp:        `{"Response":"Hello, "}`,
			statusCode: 200,
		},
		{
			method:     "world",
			req:        "",
			rsp:        "",
			statusCode: 204,
		},
		{
			method:     "ValueInRequest",
			req:        `{"name": "Pero"}`,
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "ValueInReqAndRsp",
			req:        `{"name": "Pero"}`,
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "world",
			req:        `{"name": "Pero"}`,
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "noctx", // method without ctx as first parameter
			req:        `{"name": "Pero"}`,
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "no-Ctx", // case and '-' insensitive
			req:        `{"name": "Pero"}`,
			rsp:        `{"Response":"Hello, Pero"}`,
			statusCode: 200,
		},
		{
			method:     "", // call Root method
			req:        "",
			rsp:        "",
			statusCode: 204,
		},
		{
			method:     "panic",
			req:        "",
			rsp:        "",
			statusCode: 500,
			error:      "PANIC runtime error: invalid memory address or nil pointer dereference",
		},
		{
			method:     "arrayresponse",
			req:        "",
			rsp:        "[{\"Response\":\"Hello, 0\"},{\"Response\":\"Hello, 1\"},{\"Response\":\"Hello, 2\"},{\"Response\":\"Hello, 3\"},{\"Response\":\"Hello, 4\"},{\"Response\":\"Hello, 5\"},{\"Response\":\"Hello, 6\"},{\"Response\":\"Hello, 7\"},{\"Response\":\"Hello, 8\"},{\"Response\":\"Hello, 9\"}]",
			statusCode: 200,
		},
	}

	api := &Hello{}

	t.Run("caller", func(t *testing.T) {
		for _, c := range cases {
			ctx := context.Background()
			var reqPayload []byte
			if len(c.req) > 0 {
				reqPayload = []byte(c.req)
			}
			caller := newCaller(api)

			rsp := caller.call(ctx, c.method, reqPayload)
			if c.error == "" {
				require.NoError(t, rsp.err)
			} else {
				require.Error(t, rsp.err)
				require.Equal(t, c.error, rsp.err.Error())
			}
			require.Equal(t, c.statusCode, rsp.StatusCode())
			require.Equal(t, c.rsp, string(rsp.payload))
			//t.Logf("\nmethod: %s\nrequest: %s\nresponse: %s\n", c.method, c.req, rsp)
		}
	})

	t.Run("lambda api gateway handler", func(t *testing.T) {
		handler := newLambdaApiGatewayHandler(api)
		for _, c := range cases {
			ctx := context.Background()
			req := events.APIGatewayProxyRequest{
				PathParameters: map[string]string{"path": c.method},
				Body:           c.req,
			}
			rsp, err := handler.callback(ctx, req)
			require.NoError(t, err)
			require.Equal(t, c.rsp, rsp.Body)
			require.Equal(t, c.statusCode, rsp.StatusCode)
			require.Equal(t, c.error, rsp.Headers["x-api-error"])

			//t.Logf("\nmethod: %s\nrequest: %s\nresponse: %s\n", c.method, c.req, rsp.Body)
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
