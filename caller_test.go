package mantil

import (
	"context"
	"fmt"
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

func Test(t *testing.T) {
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
