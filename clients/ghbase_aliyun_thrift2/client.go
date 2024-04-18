package ghbase_aliyun_thrift2

import (
	// hbase模块通过 thrift --gen go hbase.thrift 来生成
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/google/uuid"
	"github.com/tiz36/ghbase/clients/ghbase_aliyun_thrift2/aliyun_thrift2/gen-go/hbase"
	"github.com/tiz36/ghbase/domain"
)

type Thrift2GHBaseAliyunClient struct {
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

func NewThrift2GHBaseAliyunHttpClient(
	logger domain.Logger,
	config domain.Thrift2GHBaseClientConfig) (*Thrift2GHBaseAliyunClient, error) {

	clientId := uuid.New().String()
	tConfiguration := &thrift.TConfiguration{
		ConnectTimeout:     config.ConnectTimeout,
		SocketTimeout:      config.SocketTimeout,
		MaxFrameSize:       config.MaxFrameSize,
		TBinaryStrictRead:  thrift.BoolPtr(config.TBinaryStrictRead),
		TBinaryStrictWrite: thrift.BoolPtr(config.TBinaryStrictWrite),
	}

	logger.Infof("new thrift2 hbase client config: %v", config)
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

	return &Thrift2GHBaseAliyunClient{
		Id:            clientId,
		logger:        logger,
		Active:        true,
		ThriftApiHost: config.ThriftApiHost,
		THbaseClient:  hbase.NewTHBaseServiceClientFactory(trans, protocolFactory),
		transport:     trans,
	}, nil
}

func (t *Thrift2GHBaseAliyunClient) Close() error {
	return t.transport.Close()
}

func (t *Thrift2GHBaseAliyunClient) IsTransOpen() bool {
	isOpen := t.transport.IsOpen()
	if !isOpen {
		t.logger.Infof("transport of client id: %s is open: %v", t.Id, isOpen)
	}

	return isOpen
}

func (t *Thrift2GHBaseAliyunClient) GetId() string {
	return t.Id
}

func (t *Thrift2GHBaseAliyunClient) IsNamespaceExist(ctx context.Context, namespace string) (bool, error) {
	t.logger.Infof("is namespace exist: %s", namespace)

	if namespaces, e := t.THbaseClient.ListNamespaceDescriptors(ctx); e != nil {
		return false, e
	} else {
		for _, ns := range namespaces {
			if ns.GetName() == namespace {
				t.logger.Infof("namespace already exists: %s", namespace)
				return true, nil
			}
		}
	}

	return false, nil
}

func (t *Thrift2GHBaseAliyunClient) IsTableExist(ctx context.Context, namespace string, tableName string) (bool, error) {
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
func (t *Thrift2GHBaseAliyunClient) CreateNamespace(ctx context.Context, namespace string) error {
	t.logger.Infof("create namespace: %s", namespace)

	if IsExist, e := t.IsNamespaceExist(ctx, namespace); e != nil {
		return e
	} else if IsExist {
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
func (t *Thrift2GHBaseAliyunClient) CreateTable(ctx context.Context, namespace string, tableName string, columnFamilies []string) error {
	t.logger.Infof("create table: %s", tableName)

	if IsExist, e := t.IsTableExist(ctx, namespace, tableName); e != nil {
		return e
	} else if IsExist {
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
func (t *Thrift2GHBaseAliyunClient) DeleteTable(ctx context.Context, namespace string, tableName string) error {
	t.logger.Infof("delete table: %s", tableName)
	return nil
}

// PutCf Put Cfs
func (t *Thrift2GHBaseAliyunClient) PutCf(ctx context.Context, namespace string, tableName string, rowKey string, values map[string]map[string]string) error {
	t.logger.Debugf("put cf: table: %s, row: %s, values: %v", tableName, rowKey, values)

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
func (t *Thrift2GHBaseAliyunClient) PutCell(ctx context.Context, namespace string, tableName string, rowKey string, columnFamily string, column string, value string) error {
	t.logger.Infof("put cell: %s", tableName)
	return nil
}

func (t *Thrift2GHBaseAliyunClient) BatchPut(ctx context.Context, namespace string, tableName string, rows map[string]map[string]map[string]string) error {
	t.logger.Infof("batch put: %s", tableName)

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

// GetCell Get Data
func (t *Thrift2GHBaseAliyunClient) GetCell(ctx context.Context, namespace string, tableName string, rowKey string, columnFamily string, column string) (string, error) {
	t.logger.Infof("get cell: %s", tableName)
	return "", nil
}

// GetRow Get Data
func (t *Thrift2GHBaseAliyunClient) GetRow(ctx context.Context, namespace string, tableName string, rowKey string) (map[string]map[string]string, error) {
	t.logger.Infof("get row: %s", tableName)

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

// BatchGetCell Get Data By Batch
func (t *Thrift2GHBaseAliyunClient) BatchGetCell(ctx context.Context, namespace string, tableName string, rowKeys []string, columnFamily string, column string) (map[string]string, error) {
	t.logger.Debugf("batch get cell: %s, rowKeys: %v, cf: %v, column: %v", tableName, rowKeys, columnFamily, column)

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
func (t *Thrift2GHBaseAliyunClient) GetRows(ctx context.Context, namespace string, tableName string, rowKeys []string) ([]map[string]map[string]string, error) {
	t.logger.Infof("get rows: %s", tableName)

	var tGets []*hbase.TGet
	for _, rowKey := range rowKeys {
		tGets = append(tGets, &hbase.TGet{
			Row: []byte(rowKey),
		})
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

func (t *Thrift2GHBaseAliyunClient) GetTablesByNamespace(ctx context.Context, namespace string) ([]string, error) {
	var tables []string

	if v, e := t.THbaseClient.GetTableNamesByNamespace(ctx, namespace); e != nil {
		t.logger.Errorf("get tables error: %v", e)
		return nil, e
	} else {
		for _, table := range v {
			tables = append(tables, string(table.Qualifier))
		}
	}

	t.logger.Infof("get tables: %v", tables)
	return tables, nil
}

func (t *Thrift2GHBaseAliyunClient) Scan(
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
	caching = 30

	// 扫描的结果
	var scanResults []*hbase.TResult_
	for {
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

	return rows, nil
}

//	func main() {
//		defaultCtx := context.Background()
//		protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
//		trans, err := thrift.NewTHttpClient(HOST)
//		if err != nil {
//			fmt.Fprintln(os.Stderr, "error resolving address:", err)
//			os.Exit(1)
//		}
//		// 设置用户名密码
//		httClient := trans.(*thrift.THttpClient)
//		httClient.SetHeader("ACCESSKEYID", USER)
//		httClient.SetHeader("ACCESSSIGNATURE", PASSWORD)
//		client := hbase.NewTHBaseServiceClientFactory(trans, protocolFactory)
//		if err := trans.Open(); err != nil {
//			fmt.Fprintln(os.Stderr, "Error opening "+HOST, err)
//			os.Exit(1)
//		}
//		// create Namespace
//		err = client.CreateNamespace(defaultCtx, &hbase.TNamespaceDescriptor{Name: "ns"})
//		if err != nil {
//			fmt.Fprintln(os.Stderr, "error CreateNamespace:", err)
//			os.Exit(1)
//		}
//		// create table
//		tableName := hbase.TTableName{Ns: []byte("ns"), Qualifier: []byte("table1")}
//		err = client.CreateTable(defaultCtx, &hbase.TTableDescriptor{TableName: &tableName, Columns: []*hbase.TColumnFamilyDescriptor{&hbase.TColumnFamilyDescriptor{Name: []byte("f")}}}, nil)
//		if err != nil {
//			fmt.Fprintln(os.Stderr, "error CreateTable:", err)
//			os.Exit(1)
//		}
//		//做DML操作时，表名参数为bytes，表名的规则是namespace + 冒号 + 表名
//		tableInbytes := []byte("ns:table1")
//		// 插入数据
//		err = client.Put(defaultCtx, tableInbytes, &hbase.TPut{Row: []byte("row1"), ColumnValues: []*hbase.TColumnValue{&hbase.TColumnValue{
//			Family:    []byte("f"),
//			Qualifier: []byte("q1"),
//			Value:     []byte("value1")}}})
//		if err != nil {
//			fmt.Fprintln(os.Stderr, "error Put:", err)
//			os.Exit(1)
//		}
//		//批量插入数据
//		puts := []*hbase.TPut{&hbase.TPut{Row: []byte("row2"), ColumnValues: []*hbase.TColumnValue{&hbase.TColumnValue{
//			Family:    []byte("f"),
//			Qualifier: []byte("q1"),
//			Value:     []byte("value2")}}}, &hbase.TPut{Row: []byte("row3"), ColumnValues: []*hbase.TColumnValue{&hbase.TColumnValue{
//			Family:    []byte("f"),
//			Qualifier: []byte("q1"),
//			Value:     []byte("value3")}}}}
//		err = client.PutMultiple(defaultCtx, tableInbytes, puts)
//		if err != nil {
//			fmt.Fprintln(os.Stderr, "error PutMultiple:", err)
//			os.Exit(1)
//		}
//		// 单行查询数据
//		result, err := client.Get(defaultCtx, tableInbytes, &hbase.TGet{Row: []byte("row1")})
//		fmt.Println("Get result:")
//		fmt.Println(result)
//		// 批量单行查询
//		gets := []*hbase.TGet{&hbase.TGet{Row: []byte("row2")}, &hbase.TGet{Row: []byte("row3")}}
//		results, err := client.GetMultiple(defaultCtx, tableInbytes, gets)
//		fmt.Println("GetMultiple result:")
//		fmt.Println(results)
//		//范围扫描
//		//扫描时需要设置startRow和stopRow，否则会变成全表扫描
//		startRow := []byte("row0")
//		stopRow := []byte("row9")
//		scan := &hbase.TScan{StartRow: startRow, StopRow: stopRow}
//		//caching的大小为每次从服务器返回的行数，设置太大会导致服务器处理过久，太小会导致范围扫描与服务器做过多交互
//		//根据每行的大小，caching的值一般设置为10到100之间
//		caching := 2
//		// 扫描的结果
//		var scanResults []*hbase.TResult_
//		for true {
//			var lastResult *hbase.TResult_ = nil
//			// getScannerResults会自动完成open,close 等scanner操作，HBase增强版必须使用此方法进行范围扫描
//			currentResults, _ := client.GetScannerResults(defaultCtx, tableInbytes, scan, int32(caching))
//			for _, tResult := range currentResults {
//				lastResult = tResult
//				scanResults = append(scanResults, tResult)
//			}
//			// 如果一行都没有扫描出来，说明扫描已经结束，我们已经获得startRow和stopRow之间所有的result
//			if lastResult == nil {
//				break
//			} else {
//				// 如果此次扫描是有结果的，我们必须构造一个比当前最后一个result的行大的最小row，继续进行扫描，以便返回所有结果
//				nextStartRow := createClosestRowAfter(lastResult.Row)
//				scan = &hbase.TScan{StartRow: nextStartRow, StopRow: stopRow}
//			}
//		}
//		fmt.Println("Scan result:")
//		fmt.Println(scanResults)
//		//disable table
//		err = client.DisableTable(defaultCtx, &tableName)
//		if err != nil {
//			fmt.Fprintln(os.Stderr, "error DisableTable:", err)
//			os.Exit(1)
//		}
//		// delete table
//		err = client.DeleteTable(defaultCtx, &tableName)
//		if err != nil {
//			fmt.Fprintln(os.Stderr, "error DisableTable:", err)
//			os.Exit(1)
//		}
//		// delete namespace
//		err = client.DeleteNamespace(defaultCtx, "ns")
//		if err != nil {
//			fmt.Fprintln(os.Stderr, "error DisableTable:", err)
//			os.Exit(1)
//		}
//		//tableName, err := client.GetTableNamesByNamespace(defaultCtx, "default")
//		if err != nil {
//			fmt.Fprintln(os.Stderr, "error getting table:", err)
//			os.Exit(1)
//		}
//		//s := string(tableName[0].Qualifier)
//		//fmt.Println(s)
//	}
//
// // 此函数可以找到比当前row大的最小row，方法是在当前row后加入一个0x00的byte
// // 从比当前row大的最小row开始scan，可以保证中间不会漏扫描数据
func createClosestRowAfter(row []byte) []byte {
	var nextRow []byte
	var i int
	for i = 0; i < len(row); i++ {
		nextRow = append(nextRow, row[i])
	}
	nextRow = append(nextRow, 0x00)
	return nextRow
}
