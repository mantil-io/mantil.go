package lambdactx

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
)

type Context struct {
	RequestNo         int
	APIGatewayRequest *events.APIGatewayProxyRequest
	Lambda            *lambdacontext.LambdaContext
}

// An unexported type to be used as the key for types in this package.
// This prevents collisions with keys defined in other packages.
type key struct{}

// The key for a LambdaContext in Contexts.
// Users of this package must use lambdacontext.NewContext and lambdacontext.FromContext
// instead of using this key directly.
var ContextKey = &key{}

// FromContext returns the LambdaContext value stored in ctx, if any.
func FromContext(ctx context.Context) (*Context, bool) {
	lc, ok := ctx.Value(ContextKey).(*Context)
	return lc, ok
}
