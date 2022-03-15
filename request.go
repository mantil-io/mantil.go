package mantil

import (
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/mantil-io/mantil.go/proto"
)

// RequestType represents the type of the incoming Lambda request
type RequestType int

// RequestType enum possible values:
const (
	RequestTypeUnknown = iota
	APIGateway
	WSConnect
	WSMessage
	WSDisconnect
	Streaming
)

// Request contains Lambda function request attributes.
// It can be many sources of calling Lambda function:
//  * API Gateway
//  * AWS Console - detected at Type Unknown
//  * SDK         - detected as Type Unknown
//  * Websocket API Gateway methods
// Request contains most usefull attributes regarding of calling method.
type Request struct {
	Type    RequestType
	Methods []string
	Params  map[string]string
	Body    []byte
	Raw     []byte
	Headers map[string]string
	HTTP    httpData
	attr    requestAttributes
}

type httpData struct {
	Method   string
	Path     string
	Protocol string
}

type requestAttributes struct {
	// API gateway and WebSocket attributes
	Version               string            `json:"version"`
	Path                  string            `json:"path"`       // payload format version 1.0
	HTTPMethod            string            `json:"httpMethod"` // payload format version 1.0
	PathParameters        map[string]string `json:"pathParameters"`
	QueryStringParameters map[string]string `json:"queryStringParameters"`
	RequestContext        struct {
		Authorizer   map[string]interface{} `json:"authorizer"`
		ConnectionID string                 `json:"connectionId"` // only for websocket
		EventType    string                 `json:"eventType"`    // ws: MESSAGE,
		Protocol     string                 `json:"protocol"`     // HTTP... // only for API payload format version 1.0
		HTTP         struct {
			Method   string `json:"method"`
			Path     string `json:"path"`
			Protocol string `json:"protocol"`
		} `json:"http"` // payload format version 2.0
	} `json:"requestContext"`
	Headers         map[string]string `json:"headers"`
	Body            string            `json:"body"`
	IsBase64Encoded bool              `json:"isBase64Encoded"`

	// streaming
	ConnectionID string `json:"connectionID"`
	Inbox        string `json:"inbox"`
	URI          string `json:"uri"`
	Payload      []byte `json:"payload"`

	// for use in aws lmabda console to avoid putting quoted json into body
	RawRequest json.RawMessage `json:"req"`
}

func parseRequest(raw []byte) (req Request) {
	req = Request{
		Type: RequestTypeUnknown,
		Raw:  raw,
	}
	if err := json.Unmarshal(raw, &req.attr); err != nil {
		req.Methods = []string{req.method()}
		req.Body = raw
		if b := req.body(); b != nil {
			req.Body = b
		}
		return
	}
	req.parseHTTP()
	req.detectType()
	req.Body = req.body()
	req.Methods = req.methods()
	req.Headers = req.attr.Headers
	req.Params = req.attr.QueryStringParameters
	return
}

func (r *Request) parseHTTP() {
	if r.attr.Version == "2.0" {
		r.HTTP.Path = r.attr.RequestContext.HTTP.Path
		r.HTTP.Method = r.attr.RequestContext.HTTP.Method
		r.HTTP.Protocol = r.attr.RequestContext.HTTP.Protocol
		return
	}
	r.HTTP.Path = r.attr.Path
	r.HTTP.Method = r.attr.HTTPMethod
	r.HTTP.Protocol = r.attr.RequestContext.Protocol
}

func (r *Request) detectType() {
	context := r.attr.RequestContext
	if protocol := r.HTTP.Protocol; protocol != "" && strings.HasPrefix(protocol, "HTTP") {
		r.Type = APIGateway
		return
	}
	if context.ConnectionID != "" {
		switch context.EventType {
		case "MESSAGE":
			r.Type = WSMessage
			return
		case "CONNECT":
			r.Type = WSConnect
			return
		case "DISCONNECT":
			r.Type = WSDisconnect
			return
		default:
			return
		}
	}
	if r.HTTP.Path != "" && r.HTTP.Method != "" {
		r.Type = APIGateway
		return
	}
	if r.attr.URI != "" && r.attr.ConnectionID != "" {
		r.Type = Streaming
		return
	}
}

func (r *Request) methods() []string {
	switch r.Type {
	case WSConnect:
		return []string{"Connect", ""}
	case WSDisconnect:
		return []string{"Disconnect", ""}
	case WSMessage:
		return []string{"Message", ""}
	default:
		return []string{r.method()}
	}
}

func (r *Request) method() string {
	if r.Type == APIGateway {
		return r.attr.PathParameters["proxy"]
	}
	if r.attr.URI != "" {
		uriParts := strings.Split(r.attr.URI, ".")
		if len(uriParts) >= 2 {
			return uriParts[1]
		}
		return uriParts[0]
	}
	return ""
}

func (r *Request) body() []byte {
	if len(r.attr.Body) > 0 {
		var b = []byte(r.attr.Body)
		if r.attr.IsBase64Encoded {
			b, _ = base64.StdEncoding.DecodeString(r.attr.Body)
		}
		return b
	}
	if len(r.attr.Payload) > 0 {
		return r.attr.Payload
	}
	if len(r.attr.RawRequest) > 0 {
		return r.attr.RawRequest
	}
	if r.Type == RequestTypeUnknown {
		return r.Raw
	}
	return nil
}

func (r *Request) toStreamingResponse(rspPayload []byte) proto.Message {
	return proto.Message{
		Type:         proto.Response,
		ConnectionID: r.attr.ConnectionID,
		URI:          r.attr.URI,
		Inbox:        r.attr.Inbox,
		Payload:      rspPayload,
	}
}

// RemoteIP returns remote IP (client IP) for request recived through API Gateway
func (r *Request) RemoteIP() string {
	if r.Headers == nil {
		return ""
	}
	ips := r.Headers["X-Forwarded-For"]
	if len(ips) == 0 {
		return ""
	}
	if !strings.Contains(ips, ",") {
		return ips

	}
	return strings.Split(ips, ",")[0]
}
