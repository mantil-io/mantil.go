package mantil

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
)

type StreamConfig struct {
	Name       string
	Subjects   []string
	MaxMsgSize int // TODO make use of this limit
}

type ConsumerConfig struct {
	Stream  StreamConfig
	Name    string
	Handler string
}

type Stream struct {
	conf StreamConfig
	nc   *nats.Conn
	st   *jsm.Stream
}

var defaultNatsConn *nats.Conn

func ConnectStream(ctx context.Context, conf StreamConfig) (*Stream, error) {
	s := Stream{conf: conf}
	if err := s.natsConnect(ctx); err != nil {
		return nil, err
	}
	if err := s.loadOrCreateStream(); err != nil {
		return nil, err
	}
	return &s, nil
}

func (s *Stream) natsConnect(ctx context.Context) error {
	if s.nc != nil {
		return nil
	}
	if defaultNatsConn != nil {
		s.nc = defaultNatsConn
	}
	url := natsURL(ctx)
	nc, err := nats.Connect(
		url,
		nats.UseOldRequestStyle(), // TODO remove old style
	)
	if err != nil {
		return fmt.Errorf("connect to %s failed %w", url, err)
	}
	s.nc = nc
	s.nc.SetReconnectHandler(func(*nats.Conn) {
		log.Printf("nats reconnect")
	})
	if defaultNatsConn == nil {
		defaultNatsConn = nc
	}
	return nil
}

func natsURL(ctx context.Context) string {
	// try in environment
	if val, ok := os.LookupEnv(envNatsServers); ok {
		return val
	}
	// try in lambda environment
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		if val, ok := lc.ClientContext.Env[envNatsServers]; ok {
			return val
		}
	}
	// return local url
	return nats.DefaultURL
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
func (c *Consumer) Wait(ctx context.Context) (uint64, error) {
	for {
		nm, err := c.cs.NextMsgContext(ctx)
		if err != nil {
			if err == context.DeadlineExceeded {
				break
			}
			if err == context.Canceled {
				break
			}
			return 0, err
		}
		md, err := nm.Metadata()
		if err != nil {
			return 0, fmt.Errorf("nm.Metadata failed %w", err)
		}
		seq := md.Sequence.Stream
		if err := nm.Nak(); err != nil {
			return 0, fmt.Errorf("Nak failed %w", err)
		}
		//log.Printf("seq consumer %d stream %d", md.Sequence.Consumer, md.Sequence.Stream)
		_ = c.cs.Reset()
		return seq, nil
	}
	return 0, nil
}

// Consume all undelivered messages from the stream consumer
// call callback cb for each message
func (c *Consumer) Consume(ctx context.Context, cb func(*nats.Msg) error) error {
	// subscribe for incomming messages
	batchSize := defaultBatchSize
	inbox := nats.NewInbox()
	inboxChan := make(chan *nats.Msg, batchSize)
	sub, err := c.stream.nc.ChanSubscribe(inbox, inboxChan)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

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
				if err := cb(nm); err != nil {
					_ = nm.Nak()
					// drain inboxChan, return all messages to the stream
					for {
						select {
						case nm := <-inboxChan:
							_ = nm.Nak()
						default:
							return err
						}
					}
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
	defaultBatchSize = 1024
	envNatsServers   = "NATS_SERVERS"
)

func LambdaEnv(ctx context.Context, key string) (string, bool) {
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		val, ok := lc.ClientContext.Env[key]
		return val, ok
	}
	return "", false
}
