package mantil

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mantil-io/mantil.go/pkg/proto"
)

func newCaller(i interface{}) *caller {
	return &caller{
		value: reflect.ValueOf(i),
		typ:   reflect.TypeOf(i),
	}
}

type caller struct {
	value reflect.Value
	typ   reflect.Type
}

type callResponse struct {
	payload    []byte
	err        error
	statusCode int
}

func (c *callResponse) StatusCode() int {
	if c.statusCode != 0 {
		return c.statusCode

	}
	if c.err != nil {
		return http.StatusInternalServerError
	}
	return http.StatusOK
}

func (c *callResponse) Error() string {
	if c.err == nil {
		return ""
	}
	return c.err.Error()
}

func (c *callResponse) Err() error {
	return c.err
}

func (c *callResponse) Body() string {
	if c.payload == nil {
		return ""
	}
	return string(c.payload)
}

func (c *callResponse) Raw() ([]byte, error) {
	return c.payload, c.err
}

func (c *callResponse) AsAPIGateway() ([]byte, error) {
	var gwRsp events.APIGatewayProxyResponse
	gwRsp.StatusCode = c.StatusCode()
	gwRsp.Body = c.Body()

	hdrs := make(map[string]string)
	hdrs["Access-Control-Allow-Origin"] = "*"
	if e := c.Error(); e != "" {
		hdrs["x-api-error"] = e
	}
	gwRsp.Headers = hdrs

	return json.Marshal(gwRsp)
}

func (c *callResponse) AsWS() ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	gwRsp := events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
	}
	return json.Marshal(gwRsp)
}

func (c *callResponse) AsStreaming(req Request) (*proto.Message, error) {
	if c.err != nil {
		return nil, c.err
	}
	rm := req.ToStreamingResponse(c.payload)
	return &rm, nil
}

func callerRsp(payload []byte) callResponse {
	if payload == nil {
		return callResponse{
			statusCode: http.StatusNoContent,
		}
	}
	return callResponse{
		payload:    payload,
		statusCode: http.StatusOK,
	}
}

func callerErr(err error, statusCode int) callResponse {
	if statusCode == 0 {
		statusCode = http.StatusServiceUnavailable
	}
	return callResponse{
		statusCode: statusCode,
		err:        err,
	}
}

// Inspiration: https://github.com/aws/aws-lambda-go/blob/master/lambda/handler.go
func (c *caller) call(ctx context.Context, methodName string, reqPayload []byte) callResponse {
	methodName = strings.Replace(strings.ToLower(methodName), "-", "", -1)
	if methodName == "" {
		for _, name := range []string{"Invoke", "Root", "Default"} {
			if method, ok := c.typ.MethodByName(name); ok {
				return c.callMethod(method, ctx, reqPayload)
			}
		}
		return callerErr(
			fmt.Errorf("can't find Invoke/Root/Default method in %s", c.typ.Name()),
			http.StatusNotImplemented,
		)
	}

	for i := 0; i < c.typ.NumMethod(); i++ {
		method := c.typ.Method(i)
		if methodName != strings.ToLower(method.Name) {
			continue
		}
		return c.callMethod(method, ctx, reqPayload)
	}
	return callerErr(
		fmt.Errorf("method %s not found", methodName),
		http.StatusNotImplemented,
	)
}

func (c *caller) callMethod(method reflect.Method, ctx context.Context, reqPayload []byte) callResponse {
	args, cr := c.args(method, ctx, reqPayload)
	if cr != nil {
		return *cr
	}
	rspArgs, cr := c.callWithRecover(method.Func, args)
	if cr != nil {
		return *cr
	}

	return c.parseRspArgs(rspArgs)
}

func (c *caller) callWithRecover(fun reflect.Value, args []reflect.Value) (rpsArgs []reflect.Value, cr *callResponse) {
	defer func() {
		if r := recover(); r != nil {
			// log panic stack trace
			stackTrace := make([]byte, 8192)
			_ = runtime.Stack(stackTrace, false)
			if logPanic {
				info("PANIC %s, stack: %s", r, stackTrace)
			}
			cErr := callerErr(fmt.Errorf("PANIC %s", r), http.StatusInternalServerError)
			cr = &cErr
		}
	}()
	cr = nil
	rpsArgs = fun.Call(args)
	return
}

func (c *caller) args(method reflect.Method, ctx context.Context, reqPayload []byte) ([]reflect.Value, *callResponse) {
	numIn := method.Type.NumIn()
	methodTakesContext := false
	if numIn > 1 {
		contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
		argumentType := method.Type.In(1)
		methodTakesContext = argumentType.Implements(contextType)
	}

	// construct arguments
	var args []reflect.Value
	args = append(args, c.value)
	if methodTakesContext {
		args = append(args, reflect.ValueOf(ctx))
	}

	if numIn == len(args)+1 {
		// unmarshal reqPayload into type
		eventType := method.Type.In(numIn - 1)
		event := reflect.New(eventType)
		//fmt.Printf("kind %s\n", eventType.Kind())
		if len(reqPayload) == 0 && eventType.Kind() == reflect.Ptr {
			args = append(args, event.Elem())
		} else {
			switch eventType.Kind() {
			case reflect.String:
				args = append(args, reflect.ValueOf(string(reqPayload)))
			default:
				if len(reqPayload) == 0 {
					reqPayload = []byte(`{}`)
				}
				if err := json.Unmarshal(reqPayload, event.Interface()); err != nil {
					cr := callerErr(
						fmt.Errorf("unable to unmarshal request into %s, error: %w", eventType.Name(), err),
						http.StatusBadRequest,
					)
					return nil, &cr
				}
				args = append(args, event.Elem())
			}
		}
	}
	return args, nil
}

func (c *caller) parseRspArgs(args []reflect.Value) callResponse {
	if len(args) == 0 {
		return callerRsp(nil)
	}
	// convert return values into (interface{}, error)
	var err error
	var isLastArgError bool
	if len(args) > 0 {
		if errVal, ok := args[len(args)-1].Interface().(error); ok {
			err = errVal
			isLastArgError = true
		}
	}
	if err != nil {
		return callerErr(err, http.StatusInternalServerError)
	}
	if len(args) == 1 && isLastArgError {
		return callerRsp(nil)
	}
	val := args[0].Interface()
	if val == nil {
		return callerRsp(nil)
	}

	var rspPayload []byte
	switch v := val.(type) {
	case []byte:
		rspPayload = v
	case string:
		rspPayload = []byte(v)
	default:
		// marshal val
		rspPayload, err = json.Marshal(val)
		if err != nil {
			return callerErr(fmt.Errorf("unable to marshal response, error %w", err), http.StatusServiceUnavailable)
		}
		if len(rspPayload) == 4 && string(rspPayload) == "null" {
			return callerRsp(nil)
		}
	}
	return callerRsp(rspPayload)
}
