package mantil

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mantil-io/mantil.go/pkg/proto"
)

type RequestType int

const (
	RequestTypeUnknown = iota
	APIGateway
	WSConnect
	WSMessage
	WSDisconnect
	Streaming
)

type Request struct {
	Type    RequestType
	Method  string
	Body    []byte
	Raw     []byte
	Headers map[string]string
	attr    requestAttributes
}

type requestAttributes struct {
	// API gateway and WebSocket attributes
	Path           string            `json:"path"`
	HTTPMethod     string            `json:"httpMethod"`
	PathParameters map[string]string `json:"pathParameters"`
	RequestContext struct {
		Identity     map[string]interface{} `json:"identity"`
		ConnectionID string                 `json:"connectionId"` // postoji samo kod ws
		EventType    string                 `json:"eventType"`    // ws: MESSAGE,
		Protocol     string                 `json:"protocol"`     // HTTP... // postoji samo kod API
	} `json:"requestContext"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`

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
		req.Method = req.method()
		req.Body = raw
		if b := req.body(); b != nil {
			req.Body = b
		}
		return
	}
	req.detectType()
	req.Body = req.body()
	req.Method = req.method()
	req.Headers = req.attr.Headers
	return
}

func (r *Request) detectType() {
	context := r.attr.RequestContext
	if protocol := context.Protocol; protocol != "" && strings.HasPrefix(protocol, "HTTP") {
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
	if r.attr.Path != "" && r.attr.HTTPMethod != "" {
		r.Type = APIGateway
		return
	}
	if r.attr.URI != "" && r.attr.ConnectionID != "" {
		r.Type = Streaming
		return
	}
}

func (r *Request) method() string {
	if r.Type == APIGateway {
		return r.attr.PathParameters["path"]
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
		return []byte(r.attr.Body)
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

func (r *Request) AsAPIGatewayRequest() events.APIGatewayProxyRequest {
	var agw events.APIGatewayProxyRequest
	_ = json.Unmarshal(r.Raw, &agw)
	return agw
}

func (r *Request) AsWebsocketRequest() events.APIGatewayWebsocketProxyRequest {
	var wsr events.APIGatewayWebsocketProxyRequest
	_ = json.Unmarshal(r.Raw, &wsr)
	return wsr
}

func (r *Request) AsStreamingRequest() proto.Message {
	var msg proto.Message
	_ = json.Unmarshal(r.Raw, &msg)
	return msg
}

func (r *Request) ToStreamingResponse(rspPayload []byte) proto.Message {
	return proto.Message{
		Type:         proto.Response,
		ConnectionID: r.attr.ConnectionID,
		URI:          r.attr.URI,
		Inbox:        r.attr.Inbox,
		Payload:      rspPayload,
	}
}
