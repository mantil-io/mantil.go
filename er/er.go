// Package er is a small helper for handling errors in apis. Usually I want to
// log error location and bubble it up the chain. When returning to the user I
// usually want to hide internal error and return something which has meaning
// to the user. InternalServerError and Drugstore are examples of two
// errors in the user domain language.
//
// If Lambda function returns error which implements StatusCode mantil.go will
// use that status code in API Gateway response.
//
// If Lambda function returns error which implements ErrorCode mantil.go will
// set that code in X-Api-Error-Code header. It can be used client side to raise
// application domain error.
package er

import (
	"fmt"
	"log"
	"net/http"
)

var ErrInternalServer = NewInternalServerError("")

// InternalServerError reporesents a general server error (500)
type InternalServerError struct {
	msg string
}

// NewInternalServerError creates a new ErrInternalServer with the given message
func NewInternalServerError(msg string) *InternalServerError {
	return &InternalServerError{msg: msg}
}

func (e *InternalServerError) Error() string {
	if e.msg == "" {
		return "internal server error"
	}
	return e.msg
}

// StatusCode returns the error's status code
func (e *InternalServerError) StatusCode() int {
	return http.StatusInternalServerError
}

// ErrorCode returns the error's error code
func (e *InternalServerError) ErrorCode() int {
	return http.StatusInternalServerError
}

// BadRequestError reporesents an error for malformed requests
type BadRequestError struct {
	msg string
}

// NewBadRequestError creates a new BadRequestError with the given message
func NewBadRequestError(msg string) *BadRequestError {
	return &BadRequestError{msg: msg}
}

func (e *BadRequestError) Error() string {
	if e.msg == "" {
		return "bad request"
	}
	return e.msg
}

// StatusCode returns the error's status code
func (e *BadRequestError) StatusCode() int {
	return http.StatusBadRequest
}

// ErrorCode returns the error's error code
func (e *BadRequestError) ErrorCode() int {
	return http.StatusBadRequest
}

// ApplicationError represents a general application error
type ApplicationError struct {
	msg    string
	status int
	code   int
}

// NewApplicationError creates a new ApplicationError with the given message and status/error codes
func NewApplicationError(msg string, errorCode int, httpStatusCode int) *ApplicationError {
	return &ApplicationError{
		msg:    msg,
		code:   errorCode,
		status: httpStatusCode,
	}
}

func (e *ApplicationError) Error() string {
	return e.msg
}

// StatusCode returns the error's status code
func (e *ApplicationError) StatusCode() int {
	if e.status == 0 {
		return http.StatusInternalServerError
	}
	return e.status
}

// ErrorCode returns the error's error code
func (e *ApplicationError) ErrorCode() int {
	return e.code
}

// E will log error and return it.
// Should be used as:
//   return er.E(err)
// to get the stack of error bubling in log.
//
// replaceWith is for separtion of server error from the errors we want to
// return to the user.
// Example usage:
//   return er.E(err, er.ErrInternalServer)
// Developer will have internal error in log, clinet will get error with
// 'internal server error' message.
func E(err error, replaceWith ...error) error {
	if err == nil {
		return nil
	}
	f := log.Flags()
	log.SetFlags(f | log.Llongfile)
	log.Output(2, fmt.Sprintf("%s", err))
	log.SetFlags(f)
	if len(replaceWith) > 0 {
		return replaceWith[0]
	}
	return err
}
