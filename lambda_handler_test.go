package mantil

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mantil-io/mantil.go/pkg/proto"
	"github.com/stretchr/testify/require"
)

func TestDetectPayloadType(t *testing.T) {
	data := []struct {
		filename    string
		payloadType RequestType
		method      string
	}{
		{"api-gateway.json", APIGateway, "MethodName"},
		{"ws-connect.json", WSConnect, ""},
		{"ws-disconnect.json", WSDisconnect, ""},
		{"ws-message.json", WSMessage, ""},
		{"streaming.json", Streaming, "methodName"},
		{"console_test.json", RequestTypeUnknown, "myMethod"},
		{"bad_attribute_type.json", RequestTypeUnknown, "method"},
	}

	for i, d := range data {
		buf, err := ioutil.ReadFile("testdata/" + d.filename)
		require.NoError(t, err)
		py := detectPayloadType(buf)
		require.Equal(t, d.payloadType, py.Type, "request type detect failed for %d", i)
		require.Equal(t, buf, py.Raw)
		require.Equal(t, d.method, py.Method(), d.filename)
		if py.Type == WSConnect || py.Type == WSDisconnect {
			continue
		}
		if py.Type == Streaming {
			continue
		}
		var kv struct {
			Key string
		}
		//fmt.Printf("going to unmarshal body: `%s`\n%v\n", py.Body(), py.Body())
		err = json.Unmarshal(py.Body(), &kv)
		require.NoError(t, err, d.filename)
		require.Equal(t, "value", kv.Key, "case %d", i)
	}
}

type RequestType int

const (
	RequestTypeUnknown = iota
	APIGateway
	WSConnect
	WSMessage
	WSDisconnect
	Streaming
)

type Payload struct {
	Type  RequestType
	Raw   []byte
	probe typeProbe
}

func (p *Payload) Method() string {
	if p.Type == APIGateway {
		return p.probe.PathParameters["path"]
	}
	if p.probe.URI != "" {
		uriParts := strings.Split(p.probe.URI, ".")
		if len(uriParts) >= 2 {
			return uriParts[1]
		}
		return uriParts[0]
	}
	return ""
}

func (p *Payload) Body() []byte {
	if len(p.probe.Body) > 0 {
		return unquoteJSON(p.probe.Body)
	}
	if len(p.probe.Payload) > 0 {
		return p.probe.Payload
	}
	return nil
}

func detectPayloadType(raw []byte) (payload Payload) {
	payload = Payload{
		Type: RequestTypeUnknown,
		Raw:  raw,
	}
	if err := json.Unmarshal(raw, &payload.probe); err != nil {
		return
	}
	context := payload.probe.RequestContext
	if protocol := context.Protocol; protocol != "" && strings.HasPrefix(protocol, "HTTP") {
		payload.Type = APIGateway
		return
	}
	if context.ConnectionID != "" {
		switch context.EventType {
		case "MESSAGE":
			payload.Type = WSMessage
			return
		case "CONNECT":
			payload.Type = WSConnect
			return
		case "DISCONNECT":
			payload.Type = WSDisconnect
			return
		default:
			return
		}
	}
	if payload.probe.Path != "" && payload.probe.HTTPMethod != "" {
		payload.Type = APIGateway
		return
	}
	if payload.probe.URI != "" && payload.probe.ConnectionID != "" {
		payload.Type = Streaming
		return
	}
	return
}

type typeProbe struct {
	Path           string            `json:"path"`
	HTTPMethod     string            `json:"httpMethod"`
	PathParameters map[string]string `json:"pathParameters"`
	RequestContext struct {
		Identity     map[string]interface{} `json:"identity"`
		ConnectionID string                 `json:"connectionId"` // postoji samo kod ws
		EventType    string                 `json:"eventType"`    // ws: MESSAGE,
		Protocol     string                 `json:"protocol"`     // HTTP... // postoji samo kod API
	} `json:"requestContext"`
	Body json.RawMessage `json:"body"`

	// streaming
	ConnectionID string `json:"connectionID"`
	Inbox        string `json:"inbox"`
	URI          string `json:"uri"`
	Payload      []byte `json:"payload"`

	// for use in aws lmabda console to avoid putting quoted json into body
	RawRequest json.RawMessage `json:"req"`
}

func (p *Payload) AsAPIGatewayRequest() events.APIGatewayProxyRequest {
	var req events.APIGatewayProxyRequest
	_ = json.Unmarshal(p.Raw, &req)
	return req
}

func (p *Payload) AsWebsocketRequest() events.APIGatewayWebsocketProxyRequest {
	var req events.APIGatewayWebsocketProxyRequest
	_ = json.Unmarshal(p.Raw, &req)
	return req
}

func (p *Payload) AsStreamingRequest() proto.Message {
	var m proto.Message
	_ = json.Unmarshal(p.Raw, &m)
	return m
}

func TestNoDoubleEncoding(t *testing.T) {
	json1 := `{"body": {"key": "value"}}`
	var s struct {
		Body json.RawMessage
	}
	var s2 struct {
		Key string
	}

	err := json.Unmarshal([]byte(json1), &s)
	require.NoError(t, err)
	fmt.Printf("body: %v\n", s.Body)
	fmt.Printf("body string: %s\n", s.Body)
	fmt.Printf("is object: %v\n", s.Body[0] == 123 && s.Body[len(s.Body)-1] == 125)
	err = json.Unmarshal(s.Body, &s2)
	require.NoError(t, err)
	require.Equal(t, "value", s2.Key)

	json2 := `{"body": "{\"key\": \"value\"}"}`
	err = json.Unmarshal([]byte(json2), &s)
	require.NoError(t, err)
	fmt.Printf("body: %v\n", s.Body)
	fmt.Printf("body string: %s\n", s.Body)
	fmt.Printf("is string: %v\n", s.Body[0] == 34 && s.Body[len(s.Body)-1] == 34)

	b, err := strconv.Unquote(string(s.Body))
	require.NoError(t, err)
	fmt.Printf("unqouted: %s\n", b)
	fmt.Printf("unqouted: %v\n", []byte(b))
	err = json.Unmarshal([]byte(b), &s2)
	require.NoError(t, err)
	require.Equal(t, "value", s2.Key)
}

func unquoteJSON(raw []byte) []byte {
	if raw == nil {
		return nil
	}
	if len(raw) < 4 {
		return raw
	}
	// check for { at begin and } at end
	// that is raw object return
	if raw[0] == 123 && raw[len(raw)-1] == 125 {
		return raw
	}
	// check for "{ at begin and }" at end
	if raw[0] == 34 &&
		raw[1] == 123 &&
		raw[len(raw)-2] == 125 &&
		raw[len(raw)-1] == 34 {
		if uq, err := strconv.Unquote(string(raw)); err == nil {
			return []byte(uq)
		}
	}

	// check for base64 quoted json
	// when json is stored into []byte and then marshaled again
	if raw[0] == 34 &&
		raw[len(raw)-1] == 34 {
		s := raw[1 : len(raw)-1]
		// check for json in []byte
		b := make([]byte, base64.StdEncoding.DecodedLen(len(s)))
		n, err := base64.StdEncoding.Decode(b, s)
		if err == nil {
			return b[:n]
		}
	}

	if uq, err := strconv.Unquote(string(raw)); err == nil {
		return []byte(uq)
	}

	return raw
}

func TestDouble(t *testing.T) {
	st := struct {
		Key string
	}{Key: "value"}
	buf, _ := json.Marshal(st)

	d := struct {
		Payload []byte
		Body    string
	}{Payload: buf, Body: string(buf)}

	buf, _ = json.Marshal(d)
	fmt.Printf("buf: %v\n%s\n", buf, buf)

	var d2 struct {
		Payload json.RawMessage
		Body    json.RawMessage
	}
	require.NoError(t, json.Unmarshal(buf, &d2))
	fmt.Printf("payload: %s\n%v\n", d2.Payload, d2.Payload)
	fmt.Printf("body: %s\n", d2.Body)

	fmt.Printf("unqoted body: %s\n", unquoteJSON(d2.Body))
	fmt.Printf("unqoted payload: %s\n", unquoteJSON(d2.Payload))
}

func randString(l int) string {
	buf := make([]byte, l)
	for i := 0; i < (l+1)/2; i++ {
		buf[i] = byte(rand.Intn(256))
	}
	return fmt.Sprintf("%x", buf)[:l]
}

func generate() []*A {
	a := make([]*A, 0, 1000)
	for i := 0; i < 1000; i++ {
		one := generateA()
		a = append(a, &one)
	}
	return a
}

func generateA() A {
	return A{
		Name:     randString(16),
		BirthDay: time.Now(),
		Phone:    randString(10),
		Siblings: rand.Intn(5),
		Spouse:   rand.Intn(2) == 1,
		Money:    rand.Float64(),
	}
}

type A struct {
	Name     string
	BirthDay time.Time
	Phone    string
	Siblings int
	Spouse   bool
	Money    float64
}

var rndA = generate()

func BenchmarkString(b *testing.B) {
	a1 := generateA()
	a1Buf, _ := json.Marshal(a1)

	s := struct {
		Body string
	}{
		Body: string(a1Buf),
	}
	buf, _ := json.Marshal(s)
	//	fmt.Printf("BenchmarkString buf: %s\n", buf)

	for n := 0; n < b.N; n++ {
		req := detectPayloadType(buf)
		var a A
		err := json.Unmarshal(req.Body(), &a)
		if err != nil {
			b.Fail()
		}
	}
}

func BenchmarkBytes(b *testing.B) {
	a1 := generateA()
	a1Buf, _ := json.Marshal(a1)

	s := struct {
		Body []byte
	}{
		Body: a1Buf,
	}
	buf, _ := json.Marshal(s)
	//fmt.Printf("BenchmarkBytes buf: %s\n", buf)

	for n := 0; n < b.N; n++ {
		req := detectPayloadType(buf)
		var a A
		err := json.Unmarshal(req.Body(), &a)
		if err != nil {
			b.Fail()
		}
	}
}

func BenchmarkInner(b *testing.B) {
	a1 := generateA()
	s := struct {
		Body A
	}{
		Body: a1,
	}
	buf, _ := json.Marshal(s)
	//fmt.Printf("BenchmarkInner buf: %s\n", buf)

	for n := 0; n < b.N; n++ {
		req := detectPayloadType(buf)
		var a A
		err := json.Unmarshal(req.Body(), &a)
		if err != nil {
			b.Fail()
		}
	}
}

func BenchmarkStringArray(b *testing.B) {
	a1 := generate()
	a1Buf, _ := json.Marshal(a1)

	s := struct {
		Body string
	}{
		Body: string(a1Buf),
	}
	buf, _ := json.Marshal(s)
	//fmt.Printf("BenchmarkString buf: %s\n", buf)

	for n := 0; n < b.N; n++ {
		req := detectPayloadType(buf)
		var a []A
		err := json.Unmarshal(req.Body(), &a)
		if err != nil {
			fmt.Printf("error: %s\n", err)
			b.Fail()
		}
	}
}

func BenchmarkBytesArray(b *testing.B) {
	a1 := generate()
	a1Buf, _ := json.Marshal(a1)

	s := struct {
		Body []byte
	}{
		Body: a1Buf,
	}
	buf, _ := json.Marshal(s)
	//fmt.Printf("BenchmarkBytes buf: %s\n", buf)

	for n := 0; n < b.N; n++ {
		req := detectPayloadType(buf)
		var a []A
		err := json.Unmarshal(req.Body(), &a)
		if err != nil {
			b.Fail()
		}
	}
}

func BenchmarkInnerArray(b *testing.B) {
	a1 := generate()
	s := struct {
		Body []*A
	}{
		Body: a1,
	}
	buf, _ := json.Marshal(s)
	//fmt.Printf("BenchmarkInner buf: %s\n", buf)

	for n := 0; n < b.N; n++ {
		req := detectPayloadType(buf)
		var a []A
		err := json.Unmarshal(req.Body(), &a)
		if err != nil {
			b.Fail()
		}
	}
}
