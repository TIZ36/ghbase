package main

import (
	"context"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/tiz36/ghbase"
	"github.com/tiz36/ghbase/domain"
)

func initPool() {
	host := "127.0.0.1:55032"
	zeroLoggerAdapter :=
		ghbase.ZeroLogAdapter{
			ZeroLog: zerolog.New(os.Stdout).With().Timestamp().Logger(),
		}

	clientType := domain.TypeAliyunThrift2GHBaseHttpClient

	hbasePool, _ := ghbase.NewHbaseClientPool(
		context.Background(),
		&zeroLoggerAdapter,
		clientType,
		domain.Thrift2GHBaseClientConfig{
			ThriftApiHost:      host,
			ConnectTimeout:     15 * time.Second,
			SocketTimeout:      60 * time.Second,
			MaxFrameSize:       1024 * 1024 * 256,
			TBinaryStrictRead:  true,
			TBinaryStrictWrite: true,
			NeedCredential:     true,
			User:               "user",
			Pass:               "pass",
		},
		domain.PoolConf{
			InitSize:    10,
			MaxSize:     20,
			IdleSize:    5,
			IdleTimeout: time.Second * 30,
		})

	// use hbasePool
	hbasePool.Release()
}
