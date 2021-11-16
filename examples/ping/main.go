package main

import (
	"context"
	"log"

	"github.com/mantil-io/mantil.go"
)

func main() {
	var api = &ping{}
	mantil.LambdaHandler(api)
}

type ping struct{}

// default
func (p *ping) Default() string {
	return "pong"
}

// non default method example:
//  {
//    "uri": "ping",
//  }
func (p *ping) Ping() string {
	return "ping"
}

type Request struct {
	Name string
}

type Response struct {
	Greeting string
}

// example request:
//  {
//    "uri": "req",
//    "body": "{\"name\":\"Foo\"}"
//  }
func (p *ping) Req(ctx context.Context, req Request) (Response, error) {
	rc, ok := mantil.FromContext(ctx)
	if ok {
		// examine context a bit
		log.Printf("lambda handler request no: %d", rc.RequestNo)
		log.Printf("api method to call: %v", rc.Request.Methods)
		log.Printf("raw request: %s", rc.Request.Raw)
	}
	return Response{Greeting: "Hello, " + req.Name}, nil
}

// try also non existent method
//  {
//    "uri": "missing",
//  }
