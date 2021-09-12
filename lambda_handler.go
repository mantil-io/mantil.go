package mantil

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
)

type lambdaHandler struct {
	caller    *caller
	requestNo int
}

func newHandler(api interface{}) *lambdaHandler {
	return &lambdaHandler{
		caller: newCaller(api),
	}
}

func LambdaHandler(api interface{}) {
	handler := newHandler(api)
	lambda.StartHandler(handler)
}

func (h *lambdaHandler) call(ctx context.Context, payload []byte) (Request, *callResponse) {
	req := parseRequest(payload)
	reqCtx := h.initContext(ctx, req)
	rsp := h.caller.call(reqCtx, req.Method, req.Body)
	return req, rsp
}

func (h *lambdaHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	req, rsp := h.call(ctx, payload)
	if err := rsp.Err(); err != nil {
		info("invoke of method %s failed with error: %v", req.Method, err)
	}
	switch req.Type {
	case APIGateway:
		return rsp.AsAPIGateway()
	case Streaming:
		rm, err := rsp.AsStreaming(req)
		if err != nil {
			return nil, err
		}
		return nil, toStreamingSqs(rm, fmt.Sprintf("%s-%s", rm.ConnectionID, rm.Inbox))
	case WSConnect, WSDisconnect, WSMessage:
		return rsp.AsWS()
	default:
		return rsp.Raw()
	}
}

func (h *lambdaHandler) initContext(ctx context.Context, req Request) context.Context {
	h.requestNo++
	log.SetFlags(0)
	cv := RequestContext{
		RequestNo: h.requestNo,
		Request:   req,
	}
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		cv.Lambda = lc
	}
	return context.WithValue(ctx, ContextKey, &cv)
}

type RequestContext struct {
	RequestNo int
	Request   Request
	Lambda    *lambdacontext.LambdaContext
}

// An unexported type to be used as the key for types in this package.
// This prevents collisions with keys defined in other packages.
type key struct{}

// The key for a LambdaContext in Contexts.
// Users of this package must use lambdacontext.NewContext and lambdacontext.FromContext
// instead of using this key directly.
var ContextKey = &key{}

// FromContext returns the LambdaContext value stored in ctx, if any.
func FromContext(ctx context.Context) (*RequestContext, bool) {
	lc, ok := ctx.Value(ContextKey).(*RequestContext)
	return lc, ok
}
