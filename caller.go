package mantil

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mantil-io/mantil.go/proto"
	"github.com/mitchellh/mapstructure"
)

const (
	// ApiErrorHeader is the response header key for lambda errors
	ApiErrorHeader = "x-api-error"
	// ApiErrorCodeHeader is the response header key for lambda error codes
	ApiErrorCodeHeader = "x-api-error-code"
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

type response struct {
	value      interface{}
	payload    []byte
	err        error
	statusCode int
}

type iStatusCode interface {
	StatusCode() int
}

type iErrorCode interface {
	ErrorCode() int
}

func (c *response) StatusCode() int {
	if c.err != nil {
		if isc, ok := c.err.(iStatusCode); ok {
			return isc.StatusCode()
		}
		if c.statusCode != 0 {
			return c.statusCode
		}
		return http.StatusInternalServerError
	}
	if c.statusCode != 0 {
		return c.statusCode
	}
	return http.StatusOK
}

func (c *response) ErrorCode() int {
	if c.err != nil {
		if iec, ok := c.err.(iErrorCode); ok {
			return iec.ErrorCode()
		}
	}
	return 0
}

func (c *response) Error() string {
	if c.err == nil {
		return ""
	}
	return c.err.Error()
}

func (c *response) Err() error {
	return c.err
}

func (c *response) Value() interface{} {
	return c.value
}

func (c *response) Body() string {
	if c.payload == nil {
		return ""
	}
	return string(c.payload)
}

func (c *response) Raw() ([]byte, error) {
	return c.payload, c.err
}

func (c *response) AsAPIGateway() ([]byte, error) {
	var gwRsp events.APIGatewayProxyResponse
	gwRsp.StatusCode = c.StatusCode()
	body := c.Body()
	gwRsp.Body = body

	hdrs := make(map[string]string)
	if e := c.Error(); e != "" {
		hdrs[ApiErrorHeader] = e
	}
	if ec := c.ErrorCode(); ec != 0 {
		hdrs[ApiErrorCodeHeader] = strconv.Itoa(ec)
	}
	// try to set right content types
	if len(body) > 1 && (strings.HasPrefix(body, "{") || strings.HasPrefix(body, "[")) {
		hdrs["Content-Type"] = "application/json"
	}
	gwRsp.Headers = hdrs

	return json.Marshal(gwRsp)
}

func (c *response) AsWS() ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	gwRsp := events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
	}
	return json.Marshal(gwRsp)
}

func (c *response) AsStreaming(req Request) (*proto.Message, error) {
	if c.err != nil {
		return nil, c.err
	}
	rm := req.toStreamingResponse(c.payload)
	return &rm, nil
}

func okEmptyResponse() response {
	return okResponse(nil, nil)
}

func okResponse(payload []byte, val interface{}) response {
	if payload == nil {
		return response{
			statusCode: http.StatusNoContent,
		}
	}
	return response{
		value:      val,
		payload:    payload,
		statusCode: http.StatusOK,
	}
}

func errResponse(err error, statusCode int) response {
	if statusCode == 0 {
		statusCode = http.StatusInternalServerError
	}
	return response{
		statusCode: statusCode,
		err:        err,
	}
}

// Inspiration: https://github.com/aws/aws-lambda-go/blob/master/lambda/handler.go
func (c *caller) call(ctx context.Context, reqPayload []byte, reqParams map[string]string, methodNames ...string) response {
	for _, methodName := range methodNames {
		methodName = strings.Replace(strings.ToLower(methodName), "-", "", -1)
		if methodName == "" {
			for _, name := range []string{"Invoke", "Root", "Default"} {
				if method, ok := c.typ.MethodByName(name); ok {
					return c.callMethod(ctx, method, reqPayload, reqParams)
				}
			}
			return errResponse(
				fmt.Errorf("can't find Invoke/Root/Default method in %s", c.typ.Name()),
				http.StatusNotImplemented,
			)
		}

		for i := 0; i < c.typ.NumMethod(); i++ {
			method := c.typ.Method(i)
			if methodName != strings.ToLower(method.Name) {
				continue
			}
			return c.callMethod(ctx, method, reqPayload, reqParams)
		}
	}
	return errResponse(
		fmt.Errorf("method %v not found", methodNames),
		http.StatusNotImplemented,
	)
}

func (c *caller) callMethod(ctx context.Context, method reflect.Method, reqPayload []byte, reqParams map[string]string) response {
	args, cr := c.args(ctx, method, reqPayload, reqParams)
	if cr != nil {
		return *cr
	}
	rspArgs, cr := c.callWithRecover(method.Func, args)
	if cr != nil {
		return *cr
	}

	return c.parseRspArgs(rspArgs)
}

func (c *caller) callWithRecover(fun reflect.Value, args []reflect.Value) (rpsArgs []reflect.Value, cr *response) {
	defer func() {
		if r := recover(); r != nil {
			// log panic stack trace
			stackTrace := make([]byte, 8192)
			_ = runtime.Stack(stackTrace, false)
			if logPanic {
				info("PANIC %s, stack: %s", r, stackTrace)
			}
			cErr := errResponse(fmt.Errorf("PANIC %s", r), http.StatusInternalServerError)
			cr = &cErr
		}
	}()
	cr = nil
	rpsArgs = fun.Call(args)
	return
}

func (c *caller) args(ctx context.Context, method reflect.Method, reqPayload []byte, reqParams map[string]string) ([]reflect.Value, *response) {
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

		// query params from GET request
		if len(reqParams) > 0 {
			if err := mapstructure.WeakDecode(reqParams, event.Interface()); err != nil {
				cr := errResponse(
					fmt.Errorf("unable to unmarshal request into %s, error: %w", eventType.Name(), err),
					http.StatusBadRequest)
				return nil, &cr
			}
			args = append(args, event.Elem())
			return args, nil
		}

		// process payload
		if len(reqPayload) == 0 && eventType.Kind() == reflect.Ptr {
			args = append(args, event.Elem())
			return args, nil
		}

		switch eventType.Kind() {
		case reflect.String:
			args = append(args, reflect.ValueOf(string(reqPayload)))
		default:
			if len(reqPayload) == 0 {
				reqPayload = []byte(`{}`)
			}
			if err := json.Unmarshal(reqPayload, event.Interface()); err != nil {
				cr := errResponse(
					fmt.Errorf("unable to unmarshal request into %s, error: %w", eventType.Name(), err),
					http.StatusBadRequest,
				)
				return nil, &cr
			}
			args = append(args, event.Elem())
		}
	}
	return args, nil
}

func (c *caller) parseRspArgs(args []reflect.Value) response {
	if len(args) == 0 {
		return okEmptyResponse()
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
		return errResponse(err, http.StatusInternalServerError)
	}
	if len(args) == 1 && isLastArgError {
		return okEmptyResponse()
	}
	val := args[0].Interface()
	if val == nil {
		return okEmptyResponse()
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
			return errResponse(fmt.Errorf("unable to marshal response, error %w", err), http.StatusInternalServerError)
		}
		if len(rspPayload) == 4 && string(rspPayload) == "null" {
			return okEmptyResponse()
		}
	}
	return okResponse(rspPayload, val)
}
