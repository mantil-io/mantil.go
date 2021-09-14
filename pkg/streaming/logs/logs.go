package logs

const (
	StreamingTypeHeaderKey = "mantil-streaming-type"
	InboxHeaderKey         = "mantil-logs-inbox"

	StreamingTypeWs   = "ws"
	StreamingTypeNATS = "nats"
)

type LogMessage struct {
	Message string
}
