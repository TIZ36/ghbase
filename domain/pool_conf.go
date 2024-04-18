package domain

import "time"

type PoolConf struct {
	InitSize    int
	MaxSize     int
	IdleSize    int
	IdleTimeout time.Duration
}
