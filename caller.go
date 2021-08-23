package mantil

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"strings"

	"github.com/mantil-io/mantil.go/pkg/logs"
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

func (c *callResponse) Body() string {
	if c.payload == nil {
		return ""
	}
	return string(c.payload)
}

func (c *callResponse) Raw() ([]byte, error) {
	return c.payload, c.err
}

func callerRsp(payload []byte) *callResponse {
	if payload == nil {
		return &callResponse{
			statusCode: http.StatusNoContent,
		}
	}
	return &callResponse{
		payload:    payload,
		statusCode: http.StatusOK,
	}
}

func callerErr(err error, statusCode int) *callResponse {
	if statusCode == 0 {
		statusCode = http.StatusServiceUnavailable
	}
	return &callResponse{
		statusCode: statusCode,
		err:        err,
	}
}

// Inspiration: https://github.com/aws/aws-lambda-go/blob/master/lambda/handler.go
func (c *caller) call(ctx context.Context, methodName string, reqPayload []byte) *callResponse {
	close, err := logs.CaptureLambda(ctx)
	if err != nil {
		return callerErr(err, http.StatusInternalServerError)
	}
	defer close()
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

func (c *caller) callMethod(method reflect.Method, ctx context.Context, reqPayload []byte) *callResponse {
	args, cr := c.args(method, ctx, reqPayload)
	if cr != nil {
		return cr
	}
	response, cr := c.callWithRecover(method.Func, args)
	if cr != nil {
		return cr
	}

	return c.parseResponse(response)
}

func (c *caller) callWithRecover(fun reflect.Value, args []reflect.Value) (response []reflect.Value, cr *callResponse) {
	defer func() {
		if r := recover(); r != nil {
			// log panic stack trace
			stackTrace := make([]byte, 8192)
			_ = runtime.Stack(stackTrace, false)
			if logPanic {
				info("PANIC %s, stack: %s", r, stackTrace)
			}
			cr = callerErr(fmt.Errorf("PANIC %s", r), http.StatusInternalServerError)
		}
	}()
	cr = nil
	response = fun.Call(args)
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
			if len(reqPayload) == 0 {
				reqPayload = []byte(`{}`)
			}
			if err := json.Unmarshal(reqPayload, event.Interface()); err != nil {
				return nil, callerErr(
					fmt.Errorf("unable to unmarshal request into %s, error: %w", eventType.Name(), err),
					http.StatusBadRequest,
				)
			}
			args = append(args, event.Elem())
		}
	}
	return args, nil
}

func (c *caller) parseResponse(response []reflect.Value) *callResponse {
	if len(response) == 0 {
		return callerRsp(nil)
	}
	// convert return values into (interface{}, error)
	var err error
	if len(response) > 0 {
		if errVal, ok := response[len(response)-1].Interface().(error); ok {
			err = errVal
		}
	}
	if err != nil {
		return callerErr(err, http.StatusInternalServerError)
	}
	if len(response) == 1 {
		return callerRsp(nil)
	}
	val := response[0].Interface()
	if val == nil {
		return callerRsp(nil)
	}
	// marshal val
	rspPayload, err := json.Marshal(val)
	if err != nil {
		return callerErr(fmt.Errorf("unable to marshal response, error %w", err), http.StatusServiceUnavailable)
	}
	if len(rspPayload) == 4 && string(rspPayload) == "null" {
		return callerRsp(nil)
	}
	return callerRsp(rspPayload)
}
