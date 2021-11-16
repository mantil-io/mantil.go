package mantil

import (
	"log"
	"os"
)

var logger *log.Logger
var config func() cfg

// option to hide panic logs in tests
var logPanic = true

func init() {
	logger = log.New(os.Stderr, "[mantil] ", log.LstdFlags|log.Lmicroseconds|log.Lmsgprefix)

	// hide lazy load variable into init function
	// expose function
	var c *cfg
	config = func() cfg {
		if c != nil {
			return *c
		}
		c = &cfg{}
		if err := c.load(); err != nil {
			info("%v", err)
		}
		return *c
	}
}

// SetLogger changes library logger.
// By default it will log to stdout.
// Use SetLogger(nil) to discard logs.
func SetLogger(l *log.Logger) {
	logger = l
}

func info(format string, v ...interface{}) {
	if logger == nil {
		return
	}
	logger.Printf(format, v...)
}
