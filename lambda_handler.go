package mantil

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/mantil-io/mantil.go/pkg/streaming/logs"
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

func (h *lambdaHandler) invoke(ctx context.Context, payload []byte) (Request, response) {
	req := parseRequest(payload)
	reqCtx, lCtx := h.initContext(ctx, req)
	closeLogs := h.captureLogs(req, lCtx)
	defer closeLogs()

	rsp := h.caller.call(reqCtx, req.Body, req.Methods...)
	if err := rsp.Err(); err != nil {
		info("invoke of method %v failed with error: %v", req.Methods, err)
	}
	return req, rsp
}

func (h *lambdaHandler) formatResponse(req Request, rsp response) ([]byte, error) {
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

func (h *lambdaHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	return h.formatResponse(h.invoke(ctx, payload))
}

func (h *lambdaHandler) initContext(ctx context.Context, req Request) (context.Context, *lambdacontext.LambdaContext) {
	h.requestNo++
	log.SetFlags(0)
	cv := RequestContext{
		RequestNo: h.requestNo,
		Request:   req,
	}
	lc, ok := lambdacontext.FromContext(ctx)
	if ok {
		cv.Lambda = lc
	}
	return context.WithValue(ctx, ContextKey, &cv), lc
}

func (h *lambdaHandler) captureLogs(req Request, lCtx *lambdacontext.LambdaContext) func() {
	if logInbox := h.findHeader(logs.InboxHeaderKey, req, lCtx); logInbox != "" {
		streamingType := h.findHeader(logs.StreamingTypeHeaderKey, req, lCtx)
		var close func()
		var err error
		if streamingType == logs.StreamingTypeNATS {
			close, err = logs.CaptureNATS(logInbox)
		} else if streamingType == logs.StreamingTypeWs {
			close, err = logs.Capture(logInbox)
		}
		if err != nil {
			info("failed to capture logs %v", err)
		}
		if close != nil {
			return close
		}
	}
	return func() {}
}

func (h *lambdaHandler) findHeader(key string, req Request, lc *lambdacontext.LambdaContext) string {
	if val, ok := req.Headers[key]; ok {
		return val
	}
	if lc != nil {
		return lc.ClientContext.Custom[key]
	}
	return ""
}

type RequestContext struct {
	RequestNo int
	Request   Request
	Lambda    *lambdacontext.LambdaContext
}

func (r *RequestContext) Authorizer() map[string]interface{} {
	return r.Request.attr.RequestContext.Authorizer
}

func (r *RequestContext) WSConnectionID() string {
	return r.Request.attr.RequestContext.ConnectionID
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
