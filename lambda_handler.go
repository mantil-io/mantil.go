package mantil

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/buger/jsonparser"
	"github.com/mantil-io/mantil.go/pkg/lambdactx"
	"github.com/mantil-io/mantil.go/pkg/proto"
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

func (h *lambdaHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	if isAPIGatewayRequest(payload) {
		return h.processAPIGatewayRequest(ctx, payload)
	}
	if isStreamingRequest(payload) {
		return h.processStreamingRequest(ctx, payload)
	}
	return h.processLambdaRequest(ctx, payload)
}

func isAPIGatewayRequest(data []byte) bool {
	// request is api gateway request if it contains api gateway request keys
	keys := []string{
		"resource",
		"path",
		"httpMethod",
		"headers",
		"multiValueHeaders",
		"queryStringParameters",
		"multiValueQueryStringParameters",
		"pathParameters",
		"stageVariables",
		"requestContext",
		"body",
		"isBase64Encoded",
	}
	for _, key := range keys {
		if !jsonKeyExists(data, key) {
			return false
		}
	}
	return true
}

func (h *lambdaHandler) processAPIGatewayRequest(ctx context.Context, payload []byte) ([]byte, error) {
	var req events.APIGatewayProxyRequest
	err := json.Unmarshal(payload, &req)
	if err != nil {
		return nil, err
	}

	callerCtx := h.initLog(ctx, &req)
	path := req.PathParameters["path"]
	rsp := h.caller.call(callerCtx, path, []byte(req.Body))

	var gwRsp events.APIGatewayProxyResponse
	gwRsp.StatusCode = rsp.StatusCode()
	gwRsp.Body = rsp.Body()

	hdrs := make(map[string]string)
	hdrs["Access-Control-Allow-Origin"] = "*"
	if e := rsp.Error(); e != "" {
		hdrs["x-api-error"] = e
	}
	gwRsp.Headers = hdrs

	return json.Marshal(gwRsp)
}

func isStreamingRequest(data []byte) bool {
	// request is streaming request if it contains keys of internal protocol used for streaming
	for _, key := range proto.MessageKeys {
		if !jsonKeyExists(data, key) {
			return false
		}
	}
	return true
}

func (h *lambdaHandler) processStreamingRequest(ctx context.Context, payload []byte) ([]byte, error) {
	var m proto.Message
	err := json.Unmarshal(payload, &m)
	if err != nil {
		return nil, err
	}
	var method string
	uriParts := strings.Split(m.URI, ".")
	if len(uriParts) == 2 {
		method = uriParts[1]
	}
	callerCtx := h.initLog(ctx, nil)
	rsp := h.caller.call(callerCtx, method, m.Payload)
	rspPayload, err := rsp.Raw()
	if err != nil {
		return nil, err
	}
	rm := &proto.Message{
		Type:         proto.Response,
		ConnectionID: m.ConnectionID,
		URI:          m.URI,
		Inbox:        m.Inbox,
		Payload:      rspPayload,
	}
	err = toStreamingSqs(rm, fmt.Sprintf("%s-%s", rm.ConnectionID, rm.Inbox))
	return nil, err
}

func (h *lambdaHandler) processLambdaRequest(ctx context.Context, payload []byte) ([]byte, error) {
	method, err := jsonparser.GetString(payload, "method")
	if err != nil {
		method = ""
	}
	callerCtx := h.initLog(ctx, nil)
	rsp := h.caller.call(callerCtx, method, payload)
	rspPayload, err := rsp.Raw()
	if err != nil {
		info("invoke of method %s failed with error: %v", method, err)
	}
	return rspPayload, err
}

func jsonKeyExists(data []byte, key string) bool {
	_, _, _, err := jsonparser.Get(data, key)
	return err == nil
}

func (h *lambdaHandler) initLog(ctx context.Context, req *events.APIGatewayProxyRequest) context.Context {
	h.requestNo++
	log.SetFlags(0)
	cv := lambdactx.Context{
		RequestNo: h.requestNo,
	}
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		cv.Lambda = lc
		cv.APIGatewayRequest = req
	}
	return context.WithValue(ctx, lambdactx.ContextKey, &cv)
}
