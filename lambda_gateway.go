package mantil

import (
	"context"
	"encoding/json"
	"log"

	"github.com/atoz-technology/mantil.go/pkg/lambdactx"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/buger/jsonparser"
)

type lambdaApiGatewayHandler struct {
	caller    *caller
	requestNo int
}

func newLambdaApiGatewayHandler(api interface{}) *lambdaApiGatewayHandler {
	return &lambdaApiGatewayHandler{
		caller: newCaller(api),
	}
}

func LambdaApiGatewayHandler(api interface{}) {
	handler := newLambdaApiGatewayHandler(api)
	lambda.Start(handler.callback)
}

func LambdaHandler(api interface{}) {
	handler := newLambdaApiGatewayHandler(api)
	lambda.StartHandler(handler)
}

func (h *lambdaApiGatewayHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	if hasAPIGatewayKeys(payload) {
		// request is APIGatewayProxyRequest
		var req events.APIGatewayProxyRequest
		err := json.Unmarshal(payload, &req)
		if err != nil {
			return nil, err
		}
		rsp, err := h.callback(ctx, req)
		if err != nil {
			return nil, err
		}
		return json.Marshal(rsp)
	}
	method, err := jsonparser.GetString(payload, "method")
	if err != nil {
		method = ""
	}
	callerCtx := h.initLog(ctx, nil)
	rsp := h.caller.call(callerCtx, method, payload)
	return rsp.Raw()
}

func hasAPIGatewayKeys(data []byte) bool {
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

func jsonKeyExists(data []byte, key string) bool {
	_, _, _, err := jsonparser.Get(data, key)
	return err == nil
}

func (h *lambdaApiGatewayHandler) callback(parent context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	ctx := h.initLog(parent, &req)

	path := req.PathParameters["path"]
	rsp := h.caller.call(ctx, path, []byte(req.Body))

	return h.response(rsp), nil
}

func (h *lambdaApiGatewayHandler) response(rsp *callResponse) events.APIGatewayProxyResponse {
	var gwRsp events.APIGatewayProxyResponse
	gwRsp.StatusCode = rsp.StatusCode()
	gwRsp.Body = rsp.Body()

	hdrs := make(map[string]string)
	hdrs["Access-Control-Allow-Origin"] = "*"
	if e := rsp.Error(); e != "" {
		hdrs["x-api-error"] = e
	}
	gwRsp.Headers = hdrs

	return gwRsp
}

func (h *lambdaApiGatewayHandler) initLog(ctx context.Context, req *events.APIGatewayProxyRequest) context.Context {
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
