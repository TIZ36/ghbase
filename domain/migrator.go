package domain

import "context"

type Migrator interface {
	Migrate(ctx context.Context, table HbaseModel[Schema]) error
}
