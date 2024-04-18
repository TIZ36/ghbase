package domain

const (
	// ok for vecloud(火山云), aws
	TypeCommonThrift2HbaseTcpClient int64 = 1 + iota
	TypeCommonThrift2HbaseHttpClient

	// ok for aliyun
	TypeAliyunThrift2GHBaseHttpClient

	// DML
	HbaseApiTagCreateNamespace = "create_namespace"
	HbaseApiTagCreateTable     = "create_table"
	HbaseApiTagDeleteTable     = "delete_table"
	// DQL
	// query
	HbaseApiTagIsNamespaceExist = "is_namespace_exist"
	HbaseApiTagIsTableExist     = "is_table_exist"
	// read
	HbaseApiTagScan         = "scan"
	HbaseApiTagGetRow       = "get_row"
	HbaseApiTagGetRows      = "get_rows"
	HbaseApiTagGetCell      = "get_cell"
	HbaseApiTagBatchGetCell = "batch_get_cell"
	// write
	HbaseApiTagPutCf    = "put_cf"
	HbaseApiTagPutCell  = "put_cell"
	HbaseApiTagBatchPut = "batch_put"
)

type Client interface {
	Close() error
	IsTransOpen() bool
	HbaseApi
}
