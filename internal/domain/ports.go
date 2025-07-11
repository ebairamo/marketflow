package domain

type ExchangePort interface {
	Connect() error
	ReadPriceUpdates() (<-chan Message, <-chan error)
	Close() error
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
