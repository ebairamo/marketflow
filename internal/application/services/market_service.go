package services

import (
	"context"
	"log/slog"
	"marketflow/internal/domain"
	"sync"
	"time"
)

type ExchangeManager struct {
	exchange          domain.ExchangePort
	messageCh         chan<- domain.Message
	errCh             chan<- error
	logger            *slog.Logger
	reconnectDelay    time.Duration
	maxReconnectDelay time.Duration
	cancelFunc        context.CancelFunc
	wg                *sync.WaitGroup
}

func NewExchangeManager(
	exchange domain.ExchangePort,
	messageCh chan<- domain.Message,
	errCh chan<- error,
	logger *slog.Logger,
	wg *sync.WaitGroup,
) *ExchangeManager {
	return &ExchangeManager{
		exchange:          exchange,
		messageCh:         messageCh,
		errCh:             errCh,
		logger:            logger,
		reconnectDelay:    100 * time.Millisecond,
		maxReconnectDelay: 30 * time.Second,
		wg:                wg,
	}
}

func (em *ExchangeManager) Start(ctx context.Context) {
	em.wg.Add(1)
	managerCtx, cancel := context.WithCancel(ctx)
	em.cancelFunc = cancel

	go em.run(managerCtx)
}

func (em *ExchangeManager) run(ctx context.Context) {
	defer em.wg.Done()

	currentDelay := em.reconnectDelay

	for {
		select {
		case <-ctx.Done():
			em.logger.Info("Exchange manager stopping", "exchange", em.exchange.Name())
			if em.exchange.IsConnected() {
				em.exchange.Close()
			}
			return
		default:
		}

		// Пытаемся подключиться
		if !em.exchange.IsConnected() {
			em.logger.Info("Attempting to connect", "exchange", em.exchange.Name())

			if err := em.exchange.Connect(); err != nil {
				em.logger.Error("Failed to connect to exchange",
					"exchange", em.exchange.Name(),
					"error", err,
					"retry_in", currentDelay)

				// Отправляем ошибку
				select {
				case em.errCh <- err:
				case <-ctx.Done():
					return
				default:
				}

				// Ждём перед следующей попыткой
				select {
				case <-time.After(currentDelay):
					// Увеличиваем задержку экспоненциально
					currentDelay = minDuration(currentDelay*2, em.maxReconnectDelay)
					continue
				case <-ctx.Done():
					return
				}
			}

			// Успешное подключение - сбрасываем задержку
			currentDelay = em.reconnectDelay
			em.logger.Info("Successfully connected", "exchange", em.exchange.Name())
		}

		// Читаем данные
		exchMsgCh, exchErrCh := em.exchange.ReadPriceUpdates(ctx)

		// Обрабатываем сообщения и ошибки
		for {
			select {
			case <-ctx.Done():
				return

			case msg, ok := <-exchMsgCh:
				if !ok {
					// Канал закрыт - переподключаемся
					em.logger.Warn("Message channel closed, reconnecting",
						"exchange", em.exchange.Name())
					if em.exchange.IsConnected() {
						em.exchange.Close()
					}
					break
				}

				// Отправляем сообщение
				select {
				case em.messageCh <- msg:
				case <-ctx.Done():
					return
				}

			case err, ok := <-exchErrCh:
				if !ok {
					// Канал ошибок закрыт - переподключаемся
					em.logger.Warn("Error channel closed, reconnecting",
						"exchange", em.exchange.Name())
					if em.exchange.IsConnected() {
						em.exchange.Close()
					}
					break
				}

				// Логируем ошибку
				em.logger.Error("Exchange error received",
					"exchange", em.exchange.Name(),
					"error", err)

				// Отправляем ошибку дальше
				select {
				case em.errCh <- err:
				case <-ctx.Done():
					return
				default:
				}

				// При критических ошибках переподключаемся
				if em.shouldReconnect(err) {
					em.logger.Info("Critical error, reconnecting",
						"exchange", em.exchange.Name())
					if em.exchange.IsConnected() {
						em.exchange.Close()
					}
					break
				}
			}
		}

		// Небольшая задержка перед переподключением
		select {
		case <-time.After(currentDelay):
			currentDelay = minDuration(currentDelay*2, em.maxReconnectDelay)
		case <-ctx.Done():
			return
		}
	}
}

func (em *ExchangeManager) shouldReconnect(err error) bool {
	// Определяем, нужно ли переподключаться при данной ошибке
	errStr := err.Error()

	// Список критических ошибок, требующих переподключения
	criticalErrors := []string{
		"EOF",
		"connection reset",
		"broken pipe",
		"connection refused",
		"no such host",
		"timeout",
		"closed network connection",
	}

	for _, critical := range criticalErrors {
		if contains(errStr, critical) {
			return true
		}
	}

	return false
}

func (em *ExchangeManager) Stop() {
	if em.cancelFunc != nil {
		em.cancelFunc()
	}
}

type MarketService struct {
	exchanges []domain.ExchangePort
	storage   domain.StoragePort
	cache     domain.CachePort
	logger    *slog.Logger
	managers  []*ExchangeManager
	wg        sync.WaitGroup
}

func NewMarketService(exchanges []domain.ExchangePort, storage domain.StoragePort, cache domain.CachePort, logger *slog.Logger) *MarketService {
	return &MarketService{
		exchanges: exchanges,
		storage:   storage,
		cache:     cache,
		logger:    logger,
		managers:  make([]*ExchangeManager, 0, len(exchanges)),
	}
}

// Start запускает получение данных со всех бирж с автоматическим переподключением
func (s *MarketService) Start(ctx context.Context) (<-chan domain.Message, <-chan error) {
	// Буферизированные каналы для предотвращения блокировок
	messageCh := make(chan domain.Message, 1000)
	errCh := make(chan error, 100)

	s.logger.Info("Starting market service", "exchanges_count", len(s.exchanges))

	// Создаём менеджеры для каждой биржи
	for _, exchange := range s.exchanges {
		manager := NewExchangeManager(exchange, messageCh, errCh, s.logger, &s.wg)
		s.managers = append(s.managers, manager)
		manager.Start(ctx)
	}

	// Горутина для graceful shutdown
	go func() {
		<-ctx.Done()
		s.logger.Info("Context cancelled, stopping all exchange managers")

		// Останавливаем все менеджеры
		for _, manager := range s.managers {
			manager.Stop()
		}

		// Ждём завершения всех горутин
		s.wg.Wait()

		// Закрываем каналы
		close(messageCh)
		close(errCh)

		s.logger.Info("Market service stopped gracefully")
	}()

	return messageCh, errCh
}

// Stop останавливает все менеджеры бирж
func (s *MarketService) Stop() {
	s.logger.Info("Stopping market service")

	for _, manager := range s.managers {
		manager.Stop()
	}

	// Ждём завершения всех горутин
	s.wg.Wait()

	s.logger.Info("All exchange managers stopped")
}

// Вспомогательные функции
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && len(substr) > 0 &&
		(s[0:len(substr)] == substr || contains(s[1:], substr)))
}
