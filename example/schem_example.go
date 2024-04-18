package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/tiz36/ghbase"
	"github.com/tiz36/ghbase/domain"
)

type User struct {
	Uid      string              `ghbase:"rowkey"`
	BaseInfo domain.ColumnFamily `ghbase:"column_family:info;columns:name,age,avatar"`
}

func (u *User) GetNamespace() string {
	return "user"
}

func (u *User) GetTableName() string {
	return "user"
}

func main() {
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

	u := User{
		Uid: "123",
		BaseInfo: map[string]string{
			"name":   "zhangsan",
			"age":    "18",
			"avatar": "http://www.baidu.com",
		},
	}

	hbaseUser := domain.HbaseModel[domain.Schema]{
		S: &u,
	}

	fmt.Println(hbaseUser.ToMap())

	hbasePool.Migrate(context.Background(), hbaseUser)
}
