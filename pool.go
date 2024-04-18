package ghbase

import (
	"context"
	"errors"

	"github.com/silenceper/pool"
	"github.com/tiz36/ghbase/clients"
	"github.com/tiz36/ghbase/domain"
)

type HbaseClientPool struct {
	pool.Pool
	domain.Logger
	domain.HbaseApi
	domain.Migrator
}

func NewHbaseClientPool(
	ctx context.Context,
	logger domain.Logger,
	clientType int64,
	clientConfig domain.Thrift2GHBaseClientConfig,
	poolConf domain.PoolConf) (*HbaseClientPool, error) {

	clientFactory := clients.SelectClientFactory(
		clientType,
		clientConfig,
		logger,
	)

	poolConfig := pool.Config{
		InitialCap:  poolConf.InitSize,
		MaxCap:      poolConf.MaxSize,
		MaxIdle:     poolConf.InitSize,
		Factory:     clientFactory.Produce(),
		Ping:        clientFactory.IsAlive,
		Close:       clientFactory.Close,
		IdleTimeout: poolConf.IdleTimeout,
	}
	hbaseClientPool, err := pool.NewChannelPool(&poolConfig)
	if err != nil {
		return nil, err
	}
	return &HbaseClientPool{
		Pool:   hbaseClientPool,
		Logger: logger,
	}, nil
}

func (hcp *HbaseClientPool) Migrate(ctx context.Context, schema domain.HbaseModel[domain.Schema]) error {
	var c interface{}
	var client domain.Client
	var err error
	var ok bool

	// try to get client from pool
	if c, err = hcp.Get(); err != nil {
		hcp.Logger.Errorf("get client from pool error: %v", err)
		return err
	} else {
		defer func() {
			// put client back to pool
			if err = hcp.Put(client); err != nil {
				hcp.Logger.Errorf("put client to pool error: %v", err)
			}
		}()
	}

	hcp.Logger.Debugf("get client from pool: %v", c)
	// assert client
	if client, ok = c.(domain.Client); !ok {
		hcp.Logger.Errorf("client is not domain.Client")
		return errors.New("client is not domain.Client")
	}

	// New Migrator
	migrator := NewDefaultMigrator(client)
	if err := migrator.Migrate(schema); err != nil {
		hcp.Logger.Errorf("migrate error: %v", err)
		return err
	} else {
		hcp.Logger.Infof("migrate success, schema: ", schema)
		return nil
	}
}

func (hcp *HbaseClientPool) CloseClient(client interface{}) error {
	if client == nil {
		hcp.Logger.Errorf("client is nil")
		return errors.New("client is nil")
	}
	if client != nil {
		if c, ok := client.(domain.Client); !ok {
			hcp.Logger.Errorf("client is not domain.HbaseClient")
			return errors.New("client is not domain.HbaseClient")
		} else {
			return c.Close()
		}
	}

	return nil
}

func (hcp *HbaseClientPool) CreateNamespace(ctx context.Context, namespace string) error {
	var c interface{}
	var client domain.Client
	var err error
	var ok bool

	// try to get client from pool
	if c, err = hcp.Get(); err != nil {
		hcp.Logger.Errorf("get client from pool error: %v", err)
		return err
	} else {
		defer func() {
			// put client back to pool
			if err = hcp.Put(client); err != nil {
				hcp.Logger.Errorf("put client to pool error: %v", err)
			}
		}()
	}

	hcp.Logger.Debugf("get client from pool: %v", c)
	// assert client
	if client, ok = c.(domain.Client); !ok {
		hcp.Logger.Errorf("client is not domain.Client")
		return errors.New("client is not domain.Client")
	}

	// create namespace
	if err = client.CreateNamespace(ctx, namespace); err != nil {
		hcp.Logger.Errorf("create namespace error: %v", err)
		return err
	}

	return nil
}

func (hcp *HbaseClientPool) CreateTable(ctx context.Context, namespace string, tableName string, columnFamilies []string) error {
	var c interface{}
	var client domain.Client
	var err error
	var ok bool

	// try to get client from pool
	if c, err = hcp.Get(); err != nil {
		hcp.Logger.Errorf("get client from pool error: %v", err)
		return err
	} else {
		defer func() {
			// put client back to pool
			if err = hcp.Put(client); err != nil {
				hcp.Logger.Errorf("put client to pool error: %v", err)
			} else {
				hcp.Logger.Debugf("put client to pool success")
			}
		}()
	}

	hcp.Logger.Debugf("get client from pool: %v", c)
	// assert client
	if client, ok = c.(domain.Client); !ok {
		hcp.Logger.Errorf("client is not domain.Client")
		return errors.New("client is not domain.Client")
	}

	// create table
	if err = client.CreateTable(ctx, namespace, tableName, columnFamilies); err != nil {
		hcp.Logger.Errorf("create table error: %v", err)
		return err
	}

	return nil
}

func (hcp *HbaseClientPool) PutCf(ctx context.Context, namespace string, tableName string, rowKey string, values map[string]map[string]string) error {
	var c interface{}
	var client domain.Client
	var err error
	var ok bool

	// try to get client from pool
	if c, err = hcp.Get(); err != nil {
		hcp.Logger.Errorf("get client from pool error: %v", err)
		return err
	} else {
		defer func() {
			// put client back to pool
			if err = hcp.Put(client); err != nil {
				hcp.Logger.Errorf("put client to pool error: %v", err)
			}
		}()
	}

	// assert client
	if client, ok = c.(domain.Client); !ok {
		hcp.Logger.Errorf("client is not domain.Client")
		return errors.New("client is not domain.Client")
	}

	// put cf
	if err = client.PutCf(ctx, namespace, tableName, rowKey, values); err != nil {
		hcp.Logger.Errorf("put cf error: %v", err)
		return err
	}

	return nil
}

// GetRow Get Data
func (hcp *HbaseClientPool) GetRow(ctx context.Context, namespace string, tableName string, rowKey string) (map[string]map[string]string, error) {
	var c interface{}
	var client domain.Client
	var err error
	var ok bool

	// try to get client from pool
	if c, err = hcp.Get(); err != nil {
		hcp.Logger.Errorf("get client from pool error: %v", err)
		return nil, err
	} else {
		defer func() {
			// put client back to pool
			if err = hcp.Put(client); err != nil {
				hcp.Logger.Errorf("put client to pool error: %v", err)
			}
		}()
	}

	// assert client
	if client, ok = c.(domain.Client); !ok {
		hcp.Logger.Errorf("client is not domain.Client")
		return nil, errors.New("client is not domain.Client")
	}

	// get row
	if row, err := client.GetRow(ctx, namespace, tableName, rowKey); err != nil {
		hcp.Logger.Errorf("get row error: %v", err)
		return nil, err
	} else {
		return row, nil
	}
}

func (hcp *HbaseClientPool) GetRows(ctx context.Context, namespace string, tableName string, rowKeys []string) ([]map[string]map[string]string, error) {
	var c interface{}
	var client domain.Client
	var err error
	var ok bool

	// try to get client from pool
	if c, err = hcp.Get(); err != nil {
		hcp.Logger.Errorf("get client from pool error: %v", err)
		return nil, err
	} else {
		defer func() {
			// put client back to pool
			if err = hcp.Put(client); err != nil {
				hcp.Logger.Errorf("put client to pool error: %v", err)
			}
		}()
	}

	// assert client
	if client, ok = c.(domain.Client); !ok {
		hcp.Logger.Errorf("client is not domain.Client")
		return nil, errors.New("client is not domain.Client")
	}

	// get row
	if row, err := client.GetRows(ctx, namespace, tableName, rowKeys); err != nil {
		hcp.Logger.Errorf("get_rows error: %v", err)
		return nil, err
	} else {
		return row, nil
	}
}

func (hcp *HbaseClientPool) BatchPut(ctx context.Context, namespace string, tableName string, rows map[string]map[string]map[string]string) error {
	var c interface{}
	var client domain.Client
	var err error
	var ok bool

	// try to get client from pool
	if c, err = hcp.Get(); err != nil {
		hcp.Logger.Errorf("get client from pool error: %v", err)
		return err
	} else {
		defer func() {
			// put client back to pool
			if err = hcp.Put(client); err != nil {
				hcp.Logger.Errorf("put client to pool error: %v", err)
			}
		}()
	}

	// assert client
	if client, ok = c.(domain.Client); !ok {
		hcp.Logger.Errorf("client is not domain.Client")
		return errors.New("client is not domain.Client")
	}

	// put cf
	if err = client.BatchPut(ctx, namespace, tableName, rows); err != nil {
		hcp.Logger.Errorf("batch_put error: %v", err)
		return err
	}

	return nil
}

// BatchGetCell Get Data By Batch
func (hcp *HbaseClientPool) BatchGetCell(ctx context.Context, namespace string, tableName string, rowKeys []string, columnFamily string, column string) (map[string]string, error) {
	var c interface{}
	var client domain.Client
	var err error
	var ok bool

	// try to get client from pool
	if c, err = hcp.Get(); err != nil {
		hcp.Logger.Errorf("get client from pool error: %v", err)
		return nil, err
	} else {
		defer func() {
			// put client back to pool
			if err = hcp.Put(client); err != nil {
				hcp.Logger.Errorf("put client to pool error: %v", err)
			}
		}()
	}

	// assert client
	if client, ok = c.(domain.Client); !ok {
		hcp.Logger.Errorf("client is not domain.Client")
		return nil, errors.New("client is not domain.Client")
	}

	// get row
	if cells, err := client.BatchGetCell(ctx, namespace, tableName, rowKeys, columnFamily, column); err != nil {
		hcp.Logger.Errorf("batch_get_cels error: %v", err)
		return nil, err
	} else {
		return cells, nil
	}
}

func (hcp *HbaseClientPool) Scan(
	ctx context.Context,
	namespace string,
	tableName string,
	startRow string,
	stopRow string,
	caching int32,
) ([]map[string]map[string]string, error) {

	var c interface{}
	var client domain.Client
	var err error
	var ok bool

	// try to get client from pool
	if c, err = hcp.Get(); err != nil {
		hcp.Logger.Errorf("get client from pool error: %v", err)
		return nil, err
	} else {
		defer func() {
			// put client back to pool
			if err = hcp.Put(client); err != nil {
				hcp.Logger.Errorf("put client to pool error: %v", err)
			}
		}()
	}

	// assert client
	if client, ok = c.(domain.Client); !ok {
		hcp.Logger.Errorf("client is not domain.Client")
		return nil, errors.New("client is not domain.Client")
	}

	// get row
	if row, err := client.Scan(ctx, namespace, tableName, startRow, stopRow, caching); err != nil {
		hcp.Logger.Errorf("scan error: %v", err)
		return nil, err
	} else {
		return row, nil
	}
}

func (hcp *HbaseClientPool) GetTablesByNamespace(ctx context.Context, namespace string) ([]string, error) {

	var c interface{}
	var client domain.Client
	var err error
	var ok bool

	// try to get client from pool
	if c, err = hcp.Get(); err != nil {
		hcp.Logger.Errorf("get client from pool error: %v", err)
		return nil, err
	} else {
		defer func() {
			// put client back to pool
			if err = hcp.Put(client); err != nil {
				hcp.Logger.Errorf("put client to pool error: %v", err)
			}
		}()
	}

	// assert client
	if client, ok = c.(domain.Client); !ok {
		hcp.Logger.Errorf("client is not domain.Client")
		return nil, errors.New("client is not domain.Client")
	}

	// get tables
	if tables, err := client.GetTablesByNamespace(ctx, namespace); err != nil {
		hcp.Logger.Errorf("get tables error: %v", err)
		return nil, err
	} else {
		return tables, nil
	}
}
