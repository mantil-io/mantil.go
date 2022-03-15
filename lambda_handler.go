// Package mantil integrates Lambda function with API's in a Mantil project.
//
// It is similar to the default AWS Go [Lambda function
// handler](https://docs.aws.amazon.com/lambda/latest/dg/golang-handler.html). The
// main difference is that mantil.go handler mantil.LmabdaHandler accepts struct
// instance and exposes each exported method of that struct. Where the default
// implementation has a single function as an entrypoint.
//
// Package is intended for usage inside Mantil project.
//
// Package also provides simple key value store interface backed by a DynamoDB table.
// It manages that table as part of the Mantil project. It is created on demand and
// destroyed with the project.
package mantil

import (
	"context"
	"log"
	"net/http"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/mantil-io/mantil.go/logs"
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

// LambdaHandler is entrypoint for Mantil Lambda functions.
// Use it in your Lambda functions:
//   mantil.LambdaHandler(api)
// where api is Go struct. All exported methods of the api struct will be
// exposed as API Gateway HTTP methods.
// Exported methods must follow this rules:
//
// 	* may take between 0 and two arguments.
// 	* if there are two arguments, the first argument must satisfy the "context.Context" interface.
// 	* may return between 0 and two arguments.
// 	* if there are two return values, the second argument must be an error.
// 	* if there is one return value it must be an error.
//
// valid signatures are:
//
//   func ()
//   func () error
//   func (TIn) error
//   func () (TOut, error)
//   func (context.Context) error
//   func (context.Context, TIn) error
//   func (context.Context) (TOut, error)
//   func (context.Context, TIn) (TOut, error)
//
// For example of Lambda function see this example:
// https://github.com/mantil-io/template-excuses/blob/master/functions/excuses/main.go
//
// That defines Lambda handler around this Go struct:
// https://github.com/mantil-io/template-excuses/blob/master/api/excuses/excuses.go
//
// When used with API Gateway in Mantil application exported methods are exposed at URLs:
//  Default - [root]/excuses
//  Count   - [root]/excuses/count
//  Random  - [root]/excuses/random
// ... and so on, where excuses is the name of this api.
//
// This is similar to the default Go Lambda integration: https://docs.aws.amazon.com/lambda/latest/dg/golang-handler.html
// With added feature that all struct exported methods all exposed.
//
// Context provided to the methods is RequestContext which is wrapper around
// default lambdacontext with few added attributes.
//
// If you are using AWS Console and test calling Lambda functions use this test data:
//   {
//     "uri": "count",
//     "payload": ...
//   }
// to call count method for example.
func LambdaHandler(api interface{}) {
	handler := newHandler(api)
	lambda.StartHandler(handler)
}

// Invoke implements lambda.Handler interface required by lambda.StartHandler in LmabdHandler function.
func (h *lambdaHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	return h.formatResponse(h.invoke(ctx, payload))
}

func (h *lambdaHandler) invoke(ctx context.Context, payload []byte) (Request, response) {
	req := parseRequest(payload)
	reqCtx := h.initContext(ctx, &req)

	cb, err := logs.LambdaResponse(req.Headers)
	if err != nil {
		info("failed to start nats lambda response: %v", err)
		return req, errResponse(err, http.StatusInternalServerError)
	}

	rsp := h.caller.call(reqCtx, req.Body, req.Params, req.Methods...)
	if err := rsp.Err(); err != nil {
		info("invoke of method %v failed with error: %v", req.Methods, err)
	}
	if cb != nil {
		cb(rsp.Value(), rsp.Err())
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
		return nil, toWsForwarder(rm)
	case WSConnect, WSDisconnect, WSMessage:
		return rsp.AsWS()
	default:
		return rsp.Raw()
	}
}

func (h *lambdaHandler) initContext(ctx context.Context, req *Request) context.Context {
	h.requestNo++
	log.SetFlags(log.Llongfile) // no need for timestamp, that will add cloudwatch
	cv := RequestContext{
		RequestNo: h.requestNo,
		Request:   *req,
	}
	lc, ok := lambdacontext.FromContext(ctx)
	if ok {
		cv.Lambda = lc
		// move custom headers to request
		if custom := lc.ClientContext.Custom; len(custom) > 0 {
			if req.Headers == nil {
				req.Headers = make(map[string]string)
			}
			for k, v := range lc.ClientContext.Custom {
				req.Headers[k] = v
			}
			cv.Request = *req
		}
	}
	return context.WithValue(ctx, contextKey, &cv)
}

// RequestContext is provided as first context attribute to all api methods handled by mantil.go
// You can get it by mantil.FromContext().
// It is wrapper around github.com/aws/aws-lambda-go/lambdacontext
type RequestContext struct {
	// Number of same worker Lambda function invocations
	// 1 - cold start
	RequestNo int
	// Lambda Request attributes
	Request Request
	// Ref: https://pkg.go.dev/github.com/aws/aws-lambda-go@v1.27.0/lambdacontext#LambdaContext
	Lambda *lambdacontext.LambdaContext
}

// Authorizer attributes.
// This is place where awuthorizer on API Gateway stores his metadata.
func (r *RequestContext) Authorizer() map[string]interface{} {
	return r.Request.attr.RequestContext.Authorizer
}

// WSConnectionID if the request is received through Websocket API Gateway this will return ID.
func (r *RequestContext) WSConnectionID() string {
	return r.Request.attr.RequestContext.ConnectionID
}

// An unexported type to be used as the key for types in this package.
// This prevents collisions with keys defined in other packages.
type key struct{}

// The key for a LambdaContext in Contexts.
// Users of this package must use lambdacontext.NewContext and lambdacontext.FromContext
// instead of using this key directly.
var contextKey = &key{}

// FromContext returns the LambdaContext value stored in ctx, if any.
func FromContext(ctx context.Context) (*RequestContext, bool) {
	lc, ok := ctx.Value(contextKey).(*RequestContext)
	return lc, ok
}
