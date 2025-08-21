package concurrency

import (
	"context"
	"log/slog"
	"marketflow/internal/domain"
	"sync"
)

type WorkerPool struct {
	workersPerExchange int
	storage            domain.StoragePort
	cache              domain.CachePort
	logger             *slog.Logger
}

func NewWorkerPool(workersPerExchange int, storage domain.StoragePort, cache domain.CachePort, logger *slog.Logger) *WorkerPool {
	return &WorkerPool{
		workersPerExchange: workersPerExchange,
		storage:            storage,
		cache:              cache,
		logger:             logger,
	}
}

func (wp *WorkerPool) Start(ctx context.Context, inputCh <-chan domain.Message) {
	exchangeChannels := make(map[string]chan domain.Message)
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Fan-out: распределяем сообщения по биржам
	go func() {
		for {
			select {
			case <-ctx.Done():
				// Закрываем все каналы
				mu.Lock()
				for _, ch := range exchangeChannels {
					close(ch)
				}
				mu.Unlock()
				return

			case msg, ok := <-inputCh:
				if !ok {
					mu.Lock()
					for _, ch := range exchangeChannels {
						close(ch)
					}
					mu.Unlock()
					return
				}

				mu.Lock()
				ch, exists := exchangeChannels[msg.Exchange]
				if !exists {
					ch = make(chan domain.Message, 100)
					exchangeChannels[msg.Exchange] = ch

					// Запускаем воркеры для новой биржи
					for i := 0; i < wp.workersPerExchange; i++ {
						wg.Add(1)
						go wp.worker(ctx, &wg, msg.Exchange, i, ch)
					}
				}
				mu.Unlock()

				select {
				case ch <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Ждём завершения всех воркеров
	go func() {
		wg.Wait()
		wp.logger.Info("Все воркеры завершены")
	}()
}

func (wp *WorkerPool) worker(ctx context.Context, wg *sync.WaitGroup, exchange string, id int, ch <-chan domain.Message) {
	defer wg.Done()

	wp.logger.Info("🔧 Воркер запущен",
		"exchange", exchange,
		"worker_id", id)

	processedCount := 0

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				wp.logger.Info("🔧 Воркер завершён",
					"exchange", exchange,
					"worker_id", id,
					"processed", processedCount)
				return
			}

			// Кэшируем цену
			if err := wp.cache.CachePrice(msg); err != nil {
				wp.logger.Error("Ошибка кэширования",
					"error", err,
					"exchange", exchange,
					"symbol", msg.Symbol)
			}

			processedCount++

			// Логируем каждое 1000-е сообщение воркера
			if processedCount%1000 == 0 {
				wp.logger.Debug("Воркер статистика",
					"exchange", exchange,
					"worker_id", id,
					"processed", processedCount)
			}

		case <-ctx.Done():
			wp.logger.Info("🔧 Воркер остановлен по контексту",
				"exchange", exchange,
				"worker_id", id,
				"processed", processedCount)
			return
		}
	}
}
