package mantil

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	golambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
)

type StreamConfig struct {
	Name       string
	Subjects   []string
	NatsURL    string
	MaxMsgSize int // TODO make use of this limit
}

type ConsumerConfig struct {
	Stream  StreamConfig
	Name    string
	Handler string
}

type Stream struct {
	conf        StreamConfig
	nc          *nats.Conn
	st          *jsm.Stream
	reconnected chan struct{}
}

var defaultNatsConn *nats.Conn

func ConnectStream(conf StreamConfig) (*Stream, error) {
	s := Stream{conf: conf}
	if err := s.natsConnect(); err != nil {
		return nil, err
	}
	if err := s.loadOrCreateStream(); err != nil {
		return nil, err
	}
	return &s, nil
}

func (s *Stream) natsConnect() error {
	if s.nc != nil {
		return nil
	}
	if defaultNatsConn != nil {
		s.nc = defaultNatsConn
	}
	url := s.natsURL()
	nc, err := nats.Connect(
		url,
		nats.UseOldRequestStyle(), // TODO remove old style
	)
	if err != nil {
		return fmt.Errorf("connect to %s failed %w", url, err)
	}
	s.nc = nc

	s.reconnected = make(chan struct{})
	s.nc.SetReconnectHandler(func(*nats.Conn) {
		close(s.reconnected)
		s.reconnected = make(chan struct{})
	})
	if defaultNatsConn == nil {
		defaultNatsConn = nc
	}
	return nil
}

func (s *Stream) natsURL() string {
	if s.conf.NatsURL != "" {
		return s.conf.NatsURL
	}
	// try in environment
	if val, ok := os.LookupEnv(envNatsServers); ok {
		return val
	}
	// // try in lambda environment
	// if lc, ok := lambdacontext.FromContext(ctx); ok {
	// 	if val, ok := lc.ClientContext.Env[envNatsServers]; ok {
	// 		return val
	// 	}
	// }
	// return local url
	return nats.DefaultURL
}

func (s *Stream) Servers() []string {
	if s.nc == nil {
		return nil
	}
	return s.nc.Servers()
}

func (w *Stream) loadOrCreateStream() error {
	if w.st != nil {
		return nil
	}
	mgr, err := jsm.New(w.nc)
	if err != nil {
		return fmt.Errorf("jsm.New failed %w", err)
	}
	st, err := mgr.LoadOrNewStream(w.conf.Name,
		jsm.Subjects(w.conf.Subjects...),
		// TODO other default max msg size, age, bytes ...
	)
	if err != nil {
		return fmt.Errorf("LoadOrNewStream failed %w", err)
	}
	w.st = st
	return nil
}

type Consumer struct {
	conf   ConsumerConfig
	stream *Stream
	cs     *jsm.Consumer
	seq    uint64
}

func (s *Stream) Consumer(conf ConsumerConfig) (*Consumer, error) {
	cs, err := s.st.LoadOrNewConsumer(conf.Name,
		jsm.DurableName(conf.Name),
		jsm.DeliverAllAvailable(),
		//TODO other options
	)
	if err != nil {
		return nil, fmt.Errorf("LoadOrNewConsumer failed %w", err)
	}
	c := Consumer{
		stream: s,
		conf:   conf,
		cs:     cs,
	}
	return &c, nil
}

// Wait waits for new unprocessed message for the consumer
// returns sequence of that message
// 0, nil if ctx expired/canceled
func (c *Consumer) Wait(parent context.Context) (uint64, error) {
	for {

		msgReceived := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-c.stream.reconnected:
			case <-parent.Done():
			case <-msgReceived:
			}
			cancel()
		}()

		nm, err := c.cs.NextMsgContext(ctx)
		if err != nil {
			if parent.Err() != nil {
				break
			}
			if err == context.Canceled {
				continue // on stream reconnect
			}
			return 0, err
		}
		close(msgReceived)

		md, err := nm.Metadata()
		if err != nil {
			return 0, fmt.Errorf("nm.Metadata failed %w", err)
		}
		seq := md.Sequence.Stream
		if err := nm.Nak(); err != nil {
			return 0, fmt.Errorf("Nak failed %w", err)
		}
		_ = c.cs.Reset()
		return seq, nil
	}
	return 0, nil
}

// Consume all undelivered messages from the stream consumer
// call callback cb for each message
func (c *Consumer) Consume(ctx context.Context, cb func(*Msg) error) error {
	// subscribe for incomming messages
	batchSize := defaultBatchSize
	inbox := nats.NewInbox()
	inboxChan := make(chan *nats.Msg, batchSize)
	sub, err := c.stream.nc.ChanSubscribe(inbox, inboxChan)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	drainInbox := func() {
		for {
			select {
			case nm := <-inboxChan:
				_ = nm.Nak()
			default:
				return
			}
		}
	}

	for {
		// request batch to be deliverd to the inbox
		consumedFromBatch := 0
		req := api.JSApiConsumerGetNextRequest{
			Batch:  batchSize,
			NoWait: true,
		}
		if err := c.cs.NextMsgRequest(inbox, &req); err != nil {
			return err
		}
		for {
			if consumedFromBatch == batchSize {
				// request another batch
				break
			}
			select {
			case <-ctx.Done():
				drainInbox()
				return nil
			case nm := <-inboxChan:
				if len(nm.Data) == 0 && len(nm.Header) > 0 && nm.Header.Get(statusHdr) != "" {
					//ref:
					//https://github.com/nats-io/nats.go/blob/3b1f6fcc1e1014c838036367494c3012523166b0/nats.go#L3075
					//https://github.com/nats-io/nats.go/blob/bb2c206532b903844e8d437e9ea0b92652121817/js.go#L1243
					if nm.Header.Get(statusHdr) == noMessages {
						// no more unprocessed messages in the stream for this consumer
						return nil
					}
					if nm.Header.Get(statusHdr) == controlMsg {
						// skip control message
						// ref: https://natsio.slack.com/archives/CM3T6T7JQ/p1624622913159000?thread_ts=1624539893.142200&cid=CM3T6T7JQ
						nm.Ack()
						continue
					}
					// treat all other status messages (hopefully none) as errors
					return fmt.Errorf("consumer NextMsgRequest failed; status: %s, description: %s",
						nm.Header.Get(statusHdr), nm.Header.Get(descrHdr))
				}
				var msg Msg
				msg.from(nm)
				if err := cb(&msg); err != nil {
					_ = nm.Nak()
					drainInbox()
					return err
				}
				if err := nm.Ack(); err != nil {
					return err
				}
				consumedFromBatch++
			}
		}
	}
}

const (
	// from nats.go: https://github.com/nats-io/nats.go/blob/274aa57115bb5ba10c3db65a336334d5fdf90b7e/nats.go#L3076
	statusHdr    = "Status"
	descrHdr     = "Description"
	noResponders = "503"
	noMessages   = "404"
	controlMsg   = "100"
	// end from nats.go
	defaultBatchSize     = 1024
	envNatsServers       = "NATS_SERVERS"
	streamHandlerCleanup = 10 // in ms
)

func LambdaEnv(ctx context.Context, key string) (string, bool) {
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		val, ok := lc.ClientContext.Env[key]
		return val, ok
	}
	return "", false
}

func LambdaStreamHandler(cb func(*Msg) error) {
	handler := func(ctx context.Context, conf ConsumerConfig) error {

		if deadline, ok := ctx.Deadline(); ok {
			deadline = deadline.Add(-streamHandlerCleanup * time.Millisecond)
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}

		st, err := ConnectStream(conf.Stream)
		if err != nil {
			return err
		}
		cs, err := st.Consumer(conf)
		if err != nil {
			return err
		}
		if err := cs.Consume(ctx, cb); err != nil {
			return err
		}
		_ = st.nc.Drain()
		return nil
	}
	golambda.Start(handler)
}

type Msg struct {
	Subject  string
	Header   http.Header
	Data     []byte
	Sequence uint64
}

func (m *Msg) from(nm *nats.Msg) {
	if md, _ := nm.Metadata(); md != nil {
		m.Sequence = md.Sequence.Stream
	}
	m.Subject = nm.Subject
	m.Header = http.Header(nm.Header)
	m.Data = nm.Data
}

// ConsumerWatchLoop waits for new message in the consumer.
// Calls lambda function handler to process messages from the consumer.
// Starts waiting for the new message again.
// Loops until ctx is canceled.
func ConsumerWatchLoop(ctx context.Context, conf ConsumerConfig) error {
	l := listener{conf: conf}
	if err := l.connect(ctx); err != nil {
		return err
	}
	return l.loop(ctx)
}

type listener struct {
	conf    ConsumerConfig
	st      *Stream
	cs      *Consumer
	client  *lambda.Client
	payload []byte
}

func (l *listener) connect(ctx context.Context) error {
	st, err := ConnectStream(l.conf.Stream)
	if err != nil {
		return err
	}
	cs, err := st.Consumer(l.conf)
	if err != nil {
		return err
	}
	l.st = st
	l.cs = cs
	return l.setup()
}

func (l *listener) setup() error {
	iid, cfg, err := instanceMetadata()
	if err != nil {
		return err
	}

	l.client = lambda.New(lambda.Options{
		Region:      cfg.Region,
		Credentials: cfg.Credentials,
	})

	l.setNatsURL(iid.PrivateIP)
	buf, err := json.Marshal(l.conf)
	if err != nil {
		return err
	}
	l.payload = buf
	return nil
}

func (l *listener) setNatsURL(privateIP string) {
	url := strings.Join(l.st.Servers(), ",")
	if privateIP != "" {
		url = strings.Replace(url, "127.0.0.1", privateIP, -1)
	}
	l.conf.Stream.NatsURL = url
}

func (l *listener) loop(ctx context.Context) error {
	lastSeq := uint64(0)
	for {
		seq, err := l.cs.Wait(ctx)
		if err != nil {
			return err
		}
		if seq == 0 {
			return nil
		}
		if lastSeq > 0 && seq == lastSeq {
			// TODO rethink how to stop infinite loop, maybe few retries are OK
			return fmt.Errorf("handler called but nothing changed sequence is still %d", seq)
		}
		if err := l.callHandler(); err != nil {
			return err
		}
		lastSeq = seq
	}
}

func (l *listener) callHandler() error {
	input := &lambda.InvokeInput{
		FunctionName: &l.conf.Handler,
		LogType:      types.LogTypeTail,
		Payload:      l.payload,
	}
	output, err := l.client.Invoke(context.Background(), input)
	if err != nil {
		return err
	}

	if !(output.StatusCode >= http.StatusOK && output.StatusCode < http.StatusMultipleChoices) {
		if output.FunctionError != nil {
			return fmt.Errorf("failed with error: %s, status code: %d", *output.FunctionError, output.StatusCode)
		}
		return fmt.Errorf("failed with status code: %d", output.StatusCode)
	}
	if err := l.showLog(output.LogResult); err != nil {
		return fmt.Errorf("showLog failed %w", err)
	}

	//fmt.Printf("output payload: %s, log: %s, version: %s\n", output.Payload, outputLog, *output.ExecutedVersion)
	return nil
}

func (l *listener) showLog(logResult *string) error {
	if logResult == nil {
		return nil
	}
	dec, err := base64.StdEncoding.DecodeString(*logResult)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(bytes.NewBuffer(dec))
	for scanner.Scan() {
		log.Printf("%s >> %s", l.conf.Handler, scanner.Text())
	}
	return nil
}

// reqex wich parses report line: https://regex101.com/r/fhriQd/1
