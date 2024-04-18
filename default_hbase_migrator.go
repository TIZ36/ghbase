package ghbase

import (
	"context"
	"errors"

	"github.com/tiz36/ghbase/domain"
)

type DefaultMigrator struct {
	Client domain.Client
	domain.HbaseModel[domain.Schema]
}

func NewDefaultMigrator(client domain.Client) *DefaultMigrator {
	return &DefaultMigrator{
		Client: client,
	}
}

func (dm *DefaultMigrator) Migrate(schema domain.HbaseModel[domain.Schema]) error {
	if err := dm.CreateNamespace(context.Background(), schema); err != nil {
		return err
	}
	if err := dm.CreateTable(context.Background(), schema); err != nil {
		return err
	}

	return nil
}

func (dm *DefaultMigrator) CreateNamespace(ctx context.Context, schema domain.HbaseModel[domain.Schema]) error {
	if schema.GetNamespace() == "" {
		return errors.New("namespace is empty")
	}
	return dm.Client.CreateNamespace(ctx, schema.GetNamespace())
}

func (dm *DefaultMigrator) CreateTable(ctx context.Context, schema domain.HbaseModel[domain.Schema]) error {
	if schema.GetTableName() == "" {
		return errors.New("table name is empty")
	}

	if cfs, err := schema.GetColumnFamilies(); err != nil {
		return err
	} else {
		if len(cfs) == 0 {
			return errors.New("column families is empty")
		}
		return dm.Client.CreateTable(ctx, schema.GetNamespace(), schema.GetTableName(), cfs)
	}
}
