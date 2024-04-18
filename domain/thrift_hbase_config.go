package domain

import "time"

var (
	DefaultThrift2GHBaseClientConfig = &Thrift2GHBaseClientConfig{
		ConnectTimeout:     time.Second * 10,
		SocketTimeout:      time.Second * 60,
		MaxFrameSize:       1024 * 1024 * 256,
		TBinaryStrictRead:  true,
		TBinaryStrictWrite: true,
	}
)

type Thrift2GHBaseClientConfig struct {
	ThriftApiHost      string
	ConnectTimeout     time.Duration
	SocketTimeout      time.Duration
	MaxFrameSize       int32
	TBinaryStrictRead  bool
	TBinaryStrictWrite bool

	NeedCredential bool
	User           string
	Pass           string
}
