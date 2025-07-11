package domain

import "context"

type ExchangePort interface {
	Connect() error
	Close() error
	ReadPriceUpdates(ctx context.Context) (<-chan Message, <-chan error)
	IsConnected() bool
	Name() string
}

type StoragePort interface {
	SavePriceUpdate(message Message) error
	GetLatestPrice(symbol string) (Message, error)
}

type CachePort interface {
	CachePrice(message Message) error
	GetCachedPrice(symbol string) (Message, error)
}

type PriceServicePort interface {
	ProcessPriceUpdate(message Message) error
	GetLatestPrice(symbol string, exchange string) (Message, error)
}
