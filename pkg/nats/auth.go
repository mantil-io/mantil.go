package nats

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const (
	publicUserNkey = "UABHHTKJSSOVURA2CQGJNJNTW4O7KJDGAEIGPCPPGPF2CZF7NIBLBDGJ"
	publicUserSeed = "SUAG3MCPJAMXVTQDKZGPBJQPEGNZU3ERUW3GPFGTFXREZ7VS76BBPZVNNM"
)

func publicUserAuth() nats.Option {
	opt := nats.Nkey(publicUserNkey, func(nonce []byte) ([]byte, error) {
		user, err := nkeys.FromSeed([]byte(publicUserSeed))
		if err != nil {
			return nil, err
		}
		return user.Sign(nonce)
	})
	return opt
}
