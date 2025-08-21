package services

import (
	"context"
	"log/slog"
	"marketflow/internal/domain"
)

type MarketService struct {
	exchanges []domain.ExchangePort
	storage   domain.StoragePort
	cache     domain.CachePort
	logger    *slog.Logger
}

func NewMarketService(exchanges []domain.ExchangePort, storage domain.StoragePort, cache domain.CachePort, logger *slog.Logger) *MarketService {
	return &MarketService{
		exchanges: exchanges,
		storage:   storage,
		cache:     cache,
		logger:    logger,
	}
}

// Start запускает получение данных со всех бирж
func (s *MarketService) Start(ctx context.Context) (<-chan domain.Message, <-chan error) {
	messageCh := make(chan domain.Message, 100*len(s.exchanges))
	errCh := make(chan error, 10*len(s.exchanges))

	s.logger.Info("Запуск получения данных со всех бирж", "count", len(s.exchanges))

	for _, exchange := range s.exchanges {
		if err := exchange.Connect(); err != nil {
			s.logger.Error("Ошибка подключения к бирже",
				"exchange", exchange.Name(),
				"error", err)
			continue
		}

		exchMsgCh, exchErrCh := exchange.ReadPriceUpdates(ctx)

		// Горутина для перенаправления сообщений
		go func(name string, msgCh <-chan domain.Message) {
			s.logger.Info("Запуск обработки сообщений", "exchange", name)
			for msg := range msgCh {
				select {
				case <-ctx.Done():
					return
				case messageCh <- msg:
				}
			}
			s.logger.Info("Завершение обработки сообщений", "exchange", name)
		}(exchange.Name(), exchMsgCh)

		// Горутина для перенаправления ошибок
		go func(name string, errChan <-chan error) {
			s.logger.Info("Запуск обработки ошибок", "exchange", name)
			for err := range errChan {
				select {
				case <-ctx.Done():
					return
				case errCh <- err:
				}
			}
			s.logger.Info("Завершение обработки ошибок", "exchange", name)
		}(exchange.Name(), exchErrCh)
	}

	// Горутина для закрытия каналов при отмене контекста
	go func() {
		<-ctx.Done()
		s.logger.Info("Получен сигнал завершения работы")

		for _, exchange := range s.exchanges {
			if exchange.IsConnected() {
				if err := exchange.Close(); err != nil {
					s.logger.Error("Ошибка закрытия соединения с биржей",
						"exchange", exchange.Name(),
						"error", err)
				}
			}
		}

		close(messageCh)
		close(errCh)
		s.logger.Info("Каналы закрыты, работа завершена")
	}()

	return messageCh, errCh
}
