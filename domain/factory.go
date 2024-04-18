package domain

type ClientFactory interface {
	Produce() func() (interface{}, error)
	IsAlive(client any) error
	Close(client any) error
	GenId() string
}
