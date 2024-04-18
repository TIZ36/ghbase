package ghbase_thrift2

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/google/uuid"
	"github.com/tiz36/ghbase/clients/ghbase_thrift2/thrift2/gen-go/hbase"
	"github.com/tiz36/ghbase/domain"
)

type Thrift2GHBaseClient struct {
	domain.Client

	// id of client
	Id string

	// logger
	logger domain.Logger

	// is active
	Active bool

	// thrift api host
	ThriftApiHost string

	// thrift2 hbase client
	THbaseClient *hbase.THBaseServiceClient

	// connection of THbaseClient
	transport thrift.TTransport
}

func NewThrift2GHBaseClientHttp(
	logger domain.Logger,
	config domain.Thrift2GHBaseClientConfig) (*Thrift2GHBaseClient, error) {

	clientId := uuid.New().String()
	tConfiguration := &thrift.TConfiguration{
		ConnectTimeout:     config.ConnectTimeout,
		SocketTimeout:      config.SocketTimeout,
		MaxFrameSize:       config.MaxFrameSize,
		TBinaryStrictRead:  thrift.BoolPtr(config.TBinaryStrictRead),
		TBinaryStrictWrite: thrift.BoolPtr(config.TBinaryStrictWrite),
	}

	logger.Debugf("new thrift2 hbase client config: %v", config)
	protocolFactory := thrift.NewTBinaryProtocolFactoryConf(tConfiguration)
	trans, err := thrift.NewTHttpClient(config.ThriftApiHost)

	if err != nil {
		return nil, err
	}

	if config.NeedCredential {
		httClient := trans.(*thrift.THttpClient)
		httClient.SetHeader("ACCESSKEYID", config.User)
		httClient.SetHeader("ACCESSSIGNATURE", config.Pass)
	}

	if err := trans.Open(); err != nil {
		return nil, err
	}

	return &Thrift2GHBaseClient{
		Id:            clientId,
		logger:        logger,
		Active:        true,
		ThriftApiHost: config.ThriftApiHost,
		THbaseClient:  hbase.NewTHBaseServiceClientFactory(trans, protocolFactory),
		transport:     trans,
	}, nil
}

func NewThrift2GHBaseClient(
	logger domain.Logger,
	config domain.Thrift2GHBaseClientConfig) (*Thrift2GHBaseClient, error) {

	clientId := uuid.New().String()
	tConfiguration := &thrift.TConfiguration{
		ConnectTimeout:     config.ConnectTimeout,
		SocketTimeout:      config.SocketTimeout,
		MaxFrameSize:       config.MaxFrameSize,
		TBinaryStrictRead:  thrift.BoolPtr(config.TBinaryStrictRead),
		TBinaryStrictWrite: thrift.BoolPtr(config.TBinaryStrictWrite),
	}

	logger.Debugf("new thrift2 hbase client config: %v", config)
	tSocket := thrift.NewTSocketConf(config.ThriftApiHost, tConfiguration)

	trans := thrift.NewTFramedTransportConf(tSocket, tConfiguration)
	if err := trans.Open(); err != nil {
		return nil, err
	}

	logger.Infof(
		"new transport opened :: connected to thrift api host %s, config: %v, clientId: %s",
		config.ThriftApiHost,
		config,
		clientId,
	)

	return &Thrift2GHBaseClient{
		Id:            clientId,
		logger:        logger,
		Active:        true,
		ThriftApiHost: config.ThriftApiHost,
		THbaseClient:  hbase.NewTHBaseServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryConf(tConfiguration)),
		transport:     trans,
	}, nil
}

func (t *Thrift2GHBaseClient) IsTransOpen() bool {
	isOpen := t.transport.IsOpen()
	if !isOpen {
		t.logger.Infof("transport of client id: %s is open: %v", t.Id, isOpen)
	}
	return isOpen
}

func (t *Thrift2GHBaseClient) Close() error {
	return t.transport.Close()
}

func (t *Thrift2GHBaseClient) GetId() string {
	return t.Id
}

func (t *Thrift2GHBaseClient) IsNamespaceExist(ctx context.Context, namespace string) (bool, error) {
	t.logger.Infof("is namespace exist: %s", namespace)

	if namespaces, e := t.THbaseClient.ListNamespaces(ctx); e != nil {
		return false, e
	} else {
		for _, ns := range namespaces {
			if ns == namespace {
				t.logger.Infof("namespace already exists: %s", namespace)
				return true, nil
			}
		}
	}

	return false, nil
}

func (t *Thrift2GHBaseClient) IsTableExist(ctx context.Context, namespace string, tableName string) (bool, error) {
	t.logger.Infof("is table exist: %s", tableName)

	if v, e := t.THbaseClient.GetTableNamesByNamespace(ctx, namespace); e != nil {
		t.logger.Errorf("get tables error: %v", e)
		return false, e
	} else {
		for _, table := range v {
			if string(table.Qualifier) == tableName {
				t.logger.Infof("table already exists: %s", tableName)
				return true, nil
			}
		}
	}

	return false, nil
}

// CreateNamespace Create Namespace
func (t *Thrift2GHBaseClient) CreateNamespace(ctx context.Context, namespace string) error {
	t.logger.Infof("create namespace: %s", namespace)

	if isExist, err := t.IsNamespaceExist(ctx, namespace); err != nil {
		return err
	} else if isExist {
		t.logger.Infof("namespace already exists: %s", namespace)
		return nil
	}

	err := t.THbaseClient.CreateNamespace(ctx, &hbase.TNamespaceDescriptor{
		Name: namespace,
	})

	if err != nil {
		return err
	} else {
		t.logger.Infof("create namespace success: %s", namespace)
	}

	return nil
}

// CreateTable Create Table
func (t *Thrift2GHBaseClient) CreateTable(ctx context.Context, namespace string, tableName string, columnFamilies []string) error {
	t.logger.Infof("create table: %s", tableName)

	if isExist, err := t.IsTableExist(ctx, namespace, tableName); err != nil {
		return err
	} else if isExist {
		t.logger.Infof("table already exists: %s", tableName)
		return nil
	}

	columnFamiliesObj := make([]*hbase.TColumnFamilyDescriptor, 0, len(columnFamilies))
	for _, cf := range columnFamilies {
		columnFamiliesObj = append(columnFamiliesObj,
			&hbase.TColumnFamilyDescriptor{
				Name: []byte(cf),
			})
	}

	err := t.THbaseClient.CreateTable(ctx, &hbase.TTableDescriptor{
		TableName: &hbase.TTableName{
			Ns:        []byte(namespace),
			Qualifier: []byte(tableName),
		},
		Columns: columnFamiliesObj,
	}, nil)
	if err != nil {
		return err
	} else {
		t.logger.Infof("create table success: %s", tableName)
	}
	return nil
}

func getNSTable(ns, table string) string {
	return fmt.Sprintf("%s:%s", ns, table)
}

// DeleteTable Delete Table
func (t *Thrift2GHBaseClient) DeleteTable(ctx context.Context, namespace string, tableName string) error {
	t.logger.Infof("delete table: %s", tableName)
	return nil
}

// PutCf Put Cfs
func (t *Thrift2GHBaseClient) PutCf(ctx context.Context, namespace string, tableName string, rowKey string, values map[string]map[string]string) error {
	t.logger.Debugf("put cf: %s", tableName)

	nsTableName := getNSTable(namespace, tableName)
	var columnValues []*hbase.TColumnValue
	for columnFamily, columnValue := range values {
		for k, v := range columnValue {
			columnValues = append(columnValues, &hbase.TColumnValue{
				Family:    []byte(columnFamily),
				Qualifier: []byte(k),
				Value:     []byte(v),
			})
		}

	}

	if err := t.THbaseClient.Put(ctx, []byte(nsTableName), &hbase.TPut{
		Row:          []byte(rowKey),
		ColumnValues: columnValues,
	}); err != nil {
		t.logger.Errorf("put cf error: %v", err)
		return err
	}

	t.logger.Debugf("put cf success: table: %s, rowKey: %v, values: %v", tableName, rowKey, values)

	return nil
}

// PutCell Put Data
func (t *Thrift2GHBaseClient) PutCell(ctx context.Context, namespace string, tableName string, rowKey string, columnFamily string, column string, value string) error {
	t.logger.Debugf("put cell: %s", tableName)
	return nil
}

func (t *Thrift2GHBaseClient) BatchPut(ctx context.Context, namespace string, tableName string, rows map[string]map[string]map[string]string) error {
	t.logger.Infof("batch_put: %s, %v", tableName, len(rows))

	var tPuts []*hbase.TPut

	tableBytes := []byte(getNSTable(namespace, tableName))
	for rowKey, row := range rows {
		var columnValues []*hbase.TColumnValue
		for cf, columnValue := range row {
			for column, value := range columnValue {
				columnValues = append(columnValues, &hbase.TColumnValue{
					Family:    []byte(cf),
					Qualifier: []byte(column),
					Value:     []byte(value),
				})
			}
		}

		tPuts = append(tPuts, &hbase.TPut{
			Row:          []byte(rowKey),
			ColumnValues: columnValues,
		})
	}

	if err := t.THbaseClient.PutMultiple(ctx, tableBytes, tPuts); err != nil {
		return err
	}

	return nil
}

// GetRow Get Data
func (t *Thrift2GHBaseClient) GetRow(ctx context.Context, namespace string, tableName string, rowKey string) (map[string]map[string]string, error) {
	t.logger.Infof("get_row: table: %s, row: %s", tableName, rowKey)

	tableBytes := []byte(getNSTable(namespace, tableName))
	keyBytes := []byte(rowKey)
	result, err := t.THbaseClient.Get(ctx, tableBytes, &hbase.TGet{Row: keyBytes})
	if err != nil {
		t.logger.Errorf("get_row err: %v", err)
		return nil, err
	}

	m := make(map[string]map[string]string)
	for i := range result.ColumnValues {
		cf := string(result.ColumnValues[i].Family)
		if _, ok := m[cf]; !ok {
			m[cf] = make(map[string]string)
		}

		m[cf][string(result.ColumnValues[i].Qualifier)] = string(result.ColumnValues[i].Value)

	}
	return m, nil
}

// GetCell Get Data
func (t *Thrift2GHBaseClient) GetCell(ctx context.Context, namespace string, tableName string, rowKey string, columnFamily string, column string) (string, error) {
	t.logger.Infof("get cell: %s", tableName)
	return "", nil
}

// BatchGetCell Get Data By Batch
func (t *Thrift2GHBaseClient) BatchGetCell(ctx context.Context, namespace string, tableName string, rowKeys []string, columnFamily string, column string) (map[string]string, error) {
	t.logger.Debugf("batch get cell: %s, rowKeys: %v, cf: %v, column: %v", tableName, len(rowKeys), columnFamily, column)

	var tGets []*hbase.TGet
	tableNameBytes := []byte(getNSTable(namespace, tableName))
	for _, rowKey := range rowKeys {
		tGets = append(tGets, &hbase.TGet{
			Row: []byte(rowKey),
			Columns: []*hbase.TColumn{
				{
					Family:    []byte(columnFamily),
					Qualifier: []byte(column),
				},
			},
		})
	}

	result, err := t.THbaseClient.GetMultiple(ctx, tableNameBytes, tGets)

	if err != nil {
		t.logger.Errorf("batch get cell err: %v", err)
		return nil, err
	}
	var cells = map[string]string{}
	for _, r := range result {
		if r.ColumnValues == nil || len(r.ColumnValues) == 0 {
			continue
		} else {
			cells[string(r.Row)] = string(r.ColumnValues[0].Value)
		}
	}

	return cells, nil
}

// GetRows Get Data
func (t *Thrift2GHBaseClient) GetRows(ctx context.Context, namespace string, tableName string, rowKeys []string) ([]map[string]map[string]string, error) {
	t.logger.Infof("get_rows: table: %s, row_num: %s", tableName, len(rowKeys))

	var tGets []*hbase.TGet
	for _, rowKey := range rowKeys {
		tGets = append(tGets, &hbase.TGet{Row: []byte(rowKey)})
	}

	result, err := t.THbaseClient.GetMultiple(ctx, []byte(getNSTable(namespace, tableName)), tGets)

	if err != nil {
		t.logger.Errorf("get_row err: %v", err)
		return nil, err
	}

	var rows []map[string]map[string]string
	for _, r := range result {
		m := make(map[string]map[string]string)
		for i := range r.ColumnValues {
			cf := string(r.ColumnValues[i].Family)
			if _, ok := m[cf]; !ok {
				m[cf] = make(map[string]string)
			}

			m[cf][string(r.ColumnValues[i].Qualifier)] = string(r.ColumnValues[i].Value)
		}

		rows = append(rows, m)
	}

	return rows, nil
}

func (t *Thrift2GHBaseClient) GetTablesByNamespace(ctx context.Context, namespace string) ([]string, error) {
	var tables []string

	if v, e := t.THbaseClient.GetTableNamesByNamespace(ctx, namespace); e != nil {
		t.logger.Errorf("get tables error: %v", e)
		return nil, e
	} else {
		for _, table := range v {
			tables = append(tables, string(table.Qualifier))
		}
	}

	t.logger.Debugf("get tables: %v", tables)
	return tables, nil
}

func (t *Thrift2GHBaseClient) Scan(
	ctx context.Context,
	namespace string,
	table string,
	startRow string,
	stopRow string,
	caching int32,
) ([]map[string]map[string]string, error) {
	tableBytes := []byte(getNSTable(namespace, table))
	startRowBin := []byte(startRow)
	stopRowBin := []byte(stopRow)
	scan := &hbase.TScan{StartRow: startRowBin, StopRow: stopRowBin}

	//caching的大小为每次从服务器返回的行数，设置太大会导致服务器处理过久，太小会导致范围扫描与服务器做过多交互
	//根据每行的大小，caching的值一般设置为10到100之间

	// 扫描的结果
	var scanResults []*hbase.TResult_
	var round int32 = 0
	for {
		t.logger.Debugf("scan round: %v", round)
		var lastResult *hbase.TResult_ = nil
		// getScannerResults会自动完成open,close 等scanner操作，HBase增强版必须使用此方法进行范围扫描
		currentResults, _ := t.THbaseClient.GetScannerResults(ctx, tableBytes, scan, caching)
		for _, tResult := range currentResults {
			lastResult = tResult
			scanResults = append(scanResults, tResult)
		}

		// 如果一行都没有扫描出来，说明扫描已经结束，我们已经获得startRow和stopRow之间所有的result
		if lastResult == nil {
			// 跳出
			break
		} else {
			// 如果此次扫描是有结果的，我们必须构造一个比当前最后一个result的行大的最小row，继续进行扫描，以便返回所有结果
			nextStartRow := createClosestRowAfter(lastResult.Row)
			scan = &hbase.TScan{StartRow: nextStartRow, StopRow: stopRowBin}
		}

		round++
	}

	var rows []map[string]map[string]string
	for _, r := range scanResults {
		m := make(map[string]map[string]string)

		for i := range r.ColumnValues {
			cf := string(r.ColumnValues[i].Family)
			if _, ok := m[cf]; !ok {
				m[cf] = make(map[string]string)
			}

			m[cf][string(r.ColumnValues[i].Qualifier)] = string(r.ColumnValues[i].Value)
		}

		rows = append(rows, m)
	}

	t.logger.Debugf("scan rows: %v", rows)

	return rows, nil
}

func createClosestRowAfter(row []byte) []byte {
	var nextRow []byte
	var i int
	for i = 0; i < len(row); i++ {
		nextRow = append(nextRow, row[i])
	}
	nextRow = append(nextRow, 0x00)
	return nextRow
}
