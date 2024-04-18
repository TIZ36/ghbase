package domain

import "context"

type HbaseApi interface {
	// Scan Scan Data
	Scan(ctx context.Context, namespace string, tableName string, startRow string, stopRow string, limit int32) ([]map[string]map[string]string, error)

	// CreateNamespace Create Namespace
	CreateNamespace(ctx context.Context, namespace string) error

	// CreateTable Create Table
	CreateTable(ctx context.Context, namespace string, tableName string, columnFamilies []string) error

	// DeleteTable Delete Table
	DeleteTable(ctx context.Context, namespace string, tableName string) error

	GetTablesByNamespace(ctx context.Context, namespace string) ([]string, error)

	// PutCf Put Cfs
	PutCf(ctx context.Context, namespace string, tableName string, rowKey string, values map[string]map[string]string) error

	// PutCell Put Data
	PutCell(ctx context.Context, namespace string, tableName string, rowKey string, columnFamily string, column string, value string) error

	BatchPut(ctx context.Context, namespace string, tableName string, rows map[string]map[string]map[string]string) error

	// GetRow Get Data
	GetRow(ctx context.Context, namespace string, tableName string, rowKey string) (map[string]map[string]string, error)

	// GetCell Get Data
	GetCell(ctx context.Context, namespace string, tableName string, rowKey string, columnFamily string, column string) (string, error)

	// BatchGetCell Get Data By Batch
	BatchGetCell(ctx context.Context, namespace string, tableName string, rowKeys []string, columnFamily string, column string) (map[string]string, error)

	// GetRows Get Data
	GetRows(ctx context.Context, namespace string, tableName string, rowKeys []string) ([]map[string]map[string]string, error)

	IsNamespaceExist(ctx context.Context, namespace string) (bool, error)

	IsTableExist(ctx context.Context, namespace string, tableName string) (bool, error)
}
