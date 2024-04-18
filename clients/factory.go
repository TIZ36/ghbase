package clients

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/tiz36/ghbase/clients/ghbase_aliyun_thrift2"
	"github.com/tiz36/ghbase/clients/ghbase_thrift2"
	"github.com/tiz36/ghbase/domain"
)

type Thrift2GHBaseClientFactory struct {
	Conf            domain.Thrift2GHBaseClientConfig
	Logger          domain.Logger
	ClientType      int64
	ServiceProvider string
}

func SelectClientFactory(clientType int64, config domain.Thrift2GHBaseClientConfig, options ...interface{}) domain.ClientFactory {
	var zLogger domain.Logger
	for _, option := range options {
		switch option.(type) {
		case domain.Logger:
			zLogger = option.(domain.Logger)
			break
		default:
			fmt.Println(option)
			break
		}
	}

	return &Thrift2GHBaseClientFactory{
		Conf:       config,
		Logger:     zLogger,
		ClientType: clientType,
	}
}

func (tHbaseFactory *Thrift2GHBaseClientFactory) Produce() func() (interface{}, error) {
	return func() (interface{}, error) {
		switch tHbaseFactory.ClientType {
		case domain.TypeCommonThrift2HbaseTcpClient:
			tHbaseFactory.Logger.Infof("new common tcp client")
			client, err := ghbase_thrift2.NewThrift2GHBaseClient(tHbaseFactory.Logger, tHbaseFactory.Conf)
			return client, err
		case domain.TypeCommonThrift2HbaseHttpClient:
			tHbaseFactory.Logger.Infof("new common http client")
			client, err := ghbase_thrift2.NewThrift2GHBaseClientHttp(tHbaseFactory.Logger, tHbaseFactory.Conf)
			return client, err
		case domain.TypeAliyunThrift2GHBaseHttpClient:
			tHbaseFactory.Logger.Infof("new aliyun http client")
			client, err := ghbase_aliyun_thrift2.NewThrift2GHBaseAliyunHttpClient(tHbaseFactory.Logger, tHbaseFactory.Conf)
			return client, err
		default:
			tHbaseFactory.Logger.Errorf("client type is not supported, clientType: %v", tHbaseFactory.ClientType)
		}

		return nil, errors.New("client type is not supported")
	}
}

func (tHbaseFactory *Thrift2GHBaseClientFactory) GenId() string {
	return uuid.New().String()
}

func (tHbaseFactory *Thrift2GHBaseClientFactory) Close(client interface{}) error {
	if client == nil {
		return nil
	}

	return client.(domain.Client).Close()
}

func (tHbaseFactory *Thrift2GHBaseClientFactory) IsAlive(client interface{}) error {
	if client == nil {
		tHbaseFactory.Logger.Errorf("client is nil")
		return errors.New("client is nil")
	}

	if thriftClient, ok := client.(domain.Client); !ok {
		tHbaseFactory.Logger.Errorf("client is not domain.HbaseClient")
		return errors.New("client is not domain.HbaseClient")
	} else {
		if isOpen := thriftClient.IsTransOpen(); isOpen {
			return nil
		} else {
			tHbaseFactory.Logger.Errorf("transport is closed, prepare to close client, client: %v", thriftClient)
			return errors.New("transport is closed")
		}
	}
}
