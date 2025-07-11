package services

import (
	"context"
	"log/slog"
	"marketflow/internal/domain"
)

type MarketService struct {
	exchanges []domain.ExchangePort // Список бирж
	logger    *slog.Logger          // Логгер
}

func NewMarketService(exchanges []domain.ExchangePort, logger *slog.Logger) *MarketService {
	return &MarketService{
		exchanges: exchanges,
		logger:    logger,
	}
}

func (s *MarketService) Start(ctx context.Context) (<-chan domain.Message, <-chan error) {
	messaheCh := make(chan domain.Message, 100*len(s.exchanges))
	errCh := make(chan error, 10*len(s.exchanges))

	s.logger.Info("Запуск получения данных со всех бирж", "count", len(s.exchanges))

	for _, exchange := range s.exchanges {
		if err := exchange.Connect(); err != nil {
			s.logger.Error("Ошибка подключения к бирже",
				"exchange", exchange.Name(),
				"error", err)
			continue // Пропускаем эту биржу и переходим к следующей

		}
		exchMsgCh, exchErrCh := exchange.ReadPriceUpdates(ctx)
	}
}
