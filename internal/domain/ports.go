package domain

import (
	"context"
	"time"
)

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
	SaveAggregatedData(data AggregatedData) error
	GetAggregatedData(symbol, exchange string, from, to time.Time) ([]AggregatedData, error)
}

type CachePort interface {
	CachePrice(message Message) error
	GetCachedPrice(symbol string) (Message, error)
	GetCachedPriceByExchange(symbol, exchange string) (Message, error)
	GetPricesInRange(symbol string, duration time.Duration) ([]Message, error)
}

type PriceServicePort interface {
	ProcessPriceUpdate(message Message) error
	GetLatestPrice(symbol string, exchange string) (Message, error)
}
