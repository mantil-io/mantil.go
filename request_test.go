package mantil

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseRequest(t *testing.T) {
	data := []struct {
		filename string
		typ      RequestType
		methods  []string
	}{
		{"api-gateway.json", APIGateway, []string{"MethodName"}},
		{"ws-connect.json", WSConnect, []string{"Connect", ""}},
		{"ws-disconnect.json", WSDisconnect, []string{"Disconnect", ""}},
		{"ws-message.json", WSMessage, []string{"Message", ""}},
		{"streaming.json", Streaming, []string{"methodName"}},
		{"console_test.json", RequestTypeUnknown, []string{"myMethod"}},
		{"bad_attribute_type.json", RequestTypeUnknown, []string{"method"}},
	}

	for _, d := range data {
		buf, err := ioutil.ReadFile("testdata/" + d.filename)
		require.NoError(t, err)
		req := parseRequest(buf)
		require.Equal(t, d.typ, req.Type, d.filename)
		require.Equal(t, buf, req.Raw)
		require.Equal(t, d.methods, req.Methods, d.filename)
		if req.Type == WSConnect || req.Type == WSDisconnect {
			require.Nil(t, req.Body)
			continue
		}
		var kv struct {
			Key string
		}
		err = json.Unmarshal(req.Body, &kv)
		require.NoError(t, err, d.filename)
		require.Equal(t, "value", kv.Key, d.filename)
	}
}

func TestNoDoubleEncoding(t *testing.T) {
	t.Skip()
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
	t.Skip()
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
		req := parseRequest(buf)
		var a A
		err := json.Unmarshal(req.Body, &a)
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
		req := parseRequest(buf)
		var a A
		err := json.Unmarshal(req.Body, &a)
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
		req := parseRequest(buf)
		var a A
		err := json.Unmarshal(req.Body, &a)
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
		req := parseRequest(buf)
		var a []A
		err := json.Unmarshal(req.Body, &a)
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
		req := parseRequest(buf)
		var a []A
		err := json.Unmarshal(req.Body, &a)
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
		req := parseRequest(buf)
		var a []A
		err := json.Unmarshal(req.Body, &a)
		if err != nil {
			b.Fail()
		}
	}
}
