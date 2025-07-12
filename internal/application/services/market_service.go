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

// Start запускает получение данных со всех бирж
func (s *MarketService) Start(ctx context.Context) (<-chan domain.Message, <-chan error) {
	// Создаем каналы для объединения сообщений и ошибок от всех бирж
	messageCh := make(chan domain.Message, 100*len(s.exchanges))
	errCh := make(chan error, 10*len(s.exchanges))

	s.logger.Info("Запуск получения данных со всех бирж", "count", len(s.exchanges))

	// Запускаем получение данных с каждой биржи
	for _, exchange := range s.exchanges {
		// Подключаемся к бирже
		if err := exchange.Connect(); err != nil {
			s.logger.Error("Ошибка подключения к бирже",
				"exchange", exchange.Name(),
				"error", err)
			continue // Пропускаем эту биржу и переходим к следующей
		}

		// Получаем каналы сообщений и ошибок от биржи
		exchMsgCh, exchErrCh := exchange.ReadPriceUpdates(ctx)

		// Запускаем горутину для перенаправления сообщений в общий канал
		go func(name string, msgCh <-chan domain.Message) {
			s.logger.Info("Запуск обработки сообщений", "exchange", name)
			for msg := range msgCh {
				messageCh <- msg // Перенаправляем сообщение в общий канал
			}
			s.logger.Info("Завершение обработки сообщений", "exchange", name)
		}(exchange.Name(), exchMsgCh)

		// Запускаем горутину для перенаправления ошибок в общий канал
		go func(name string, errChan <-chan error) {
			s.logger.Info("Запуск обработки ошибок", "exchange", name)
			for err := range errChan {
				errCh <- err // Перенаправляем ошибку в общий канал
			}
			s.logger.Info("Завершение обработки ошибок", "exchange", name)
		}(exchange.Name(), exchErrCh)
	}

	// Запускаем горутину для закрытия каналов при отмене контекста
	go func() {
		<-ctx.Done() // Ожидаем отмены контекста
		s.logger.Info("Получен сигнал завершения работы")

		// Закрываем соединения с биржами
		for _, exchange := range s.exchanges {
			if exchange.IsConnected() {
				if err := exchange.Close(); err != nil {
					s.logger.Error("Ошибка закрытия соединения с биржей",
						"exchange", exchange.Name(),
						"error", err)
				}
			}
		}

		// Закрываем каналы
		close(messageCh)
		close(errCh)
		s.logger.Info("Каналы закрыты, работа завершена")
	}()

	return messageCh, errCh
}
