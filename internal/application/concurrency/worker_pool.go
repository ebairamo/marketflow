package concurrency

import (
	"context"
	"log/slog"
	"marketflow/internal/domain"
	"sync"
	"sync/atomic"
	"time"
)

type WorkerPool struct {
	workersPerExchange int
	storage            domain.StoragePort
	cache              domain.CachePort
	logger             *slog.Logger
	wg                 sync.WaitGroup
	totalProcessed     int64 // Атомарный счётчик обработанных сообщений
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
	var channelsMu sync.RWMutex

	// Канал для завершения диспетчера
	dispatcherDone := make(chan struct{})

	// Fan-out диспетчер: распределяем сообщения по биржам
	go func() {
		defer close(dispatcherDone)

		for {
			select {
			case <-ctx.Done():
				wp.logger.Info("Dispatcher stopping, closing exchange channels")

				// Закрываем все каналы бирж
				channelsMu.Lock()
				for exchange, ch := range exchangeChannels {
					close(ch)
					wp.logger.Debug("Closed channel for exchange", "exchange", exchange)
				}
				channelsMu.Unlock()
				return

			case msg, ok := <-inputCh:
				if !ok {
					wp.logger.Info("Input channel closed, stopping dispatcher")

					// Входной канал закрыт - закрываем все каналы бирж
					channelsMu.Lock()
					for exchange, ch := range exchangeChannels {
						close(ch)
						wp.logger.Debug("Closed channel for exchange", "exchange", exchange)
					}
					channelsMu.Unlock()
					return
				}

				// Получаем или создаём канал для биржи
				channelsMu.RLock()
				ch, exists := exchangeChannels[msg.Exchange]
				channelsMu.RUnlock()

				if !exists {
					channelsMu.Lock()
					// Проверяем ещё раз под write lock
					ch, exists = exchangeChannels[msg.Exchange]
					if !exists {
						// Создаём новый канал и воркеры для биржи
						ch = make(chan domain.Message, 100)
						exchangeChannels[msg.Exchange] = ch

						// Запускаем воркеры для новой биржи
						for i := 0; i < wp.workersPerExchange; i++ {
							wp.wg.Add(1)
							go wp.worker(ctx, msg.Exchange, i, ch)
						}

						wp.logger.Info("Created workers for new exchange",
							"exchange", msg.Exchange,
							"workers", wp.workersPerExchange)
					}
					channelsMu.Unlock()
				}

				// Отправляем сообщение в канал биржи
				select {
				case ch <- msg:
				case <-ctx.Done():
					return
				default:
					// Канал переполнен - логируем предупреждение
					wp.logger.Warn("Channel full, dropping message",
						"exchange", msg.Exchange,
						"symbol", msg.Symbol)
				}
			}
		}
	}()

	// Горутина для мониторинга статистики
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processed := atomic.LoadInt64(&wp.totalProcessed)
				wp.logger.Info("🔧 Worker Pool Statistics",
					"total_processed", processed,
					"active_exchanges", len(exchangeChannels))
			}
		}
	}()

	// Горутина для ожидания завершения
	go func() {
		// Ждём завершения диспетчера
		<-dispatcherDone

		// Ждём завершения всех воркеров
		wp.wg.Wait()

		// Выводим финальную статистику
		finalProcessed := atomic.LoadInt64(&wp.totalProcessed)
		wp.logger.Info("🔧 Worker Pool stopped",
			"total_processed", finalProcessed)
	}()
}

func (wp *WorkerPool) worker(ctx context.Context, exchange string, id int, ch <-chan domain.Message) {
	defer wp.wg.Done()

	wp.logger.Info("🔧 Worker started",
		"exchange", exchange,
		"worker_id", id)

	processedCount := 0
	batchBuffer := make([]domain.Message, 0, 10)
	batchTicker := time.NewTicker(5 * time.Second)
	defer batchTicker.Stop()

	// Функция для обработки батча
	processBatch := func() {
		if len(batchBuffer) == 0 {
			return
		}

		// Здесь можно реализовать батчевую обработку
		for _, msg := range batchBuffer {
			// Кэшируем цену
			if err := wp.cache.CachePrice(msg); err != nil {
				wp.logger.Error("Failed to cache price",
					"error", err,
					"exchange", exchange,
					"symbol", msg.Symbol)
			}
		}

		// Обновляем общий счётчик
		atomic.AddInt64(&wp.totalProcessed, int64(len(batchBuffer)))

		// Очищаем буфер
		batchBuffer = batchBuffer[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Обрабатываем оставшийся батч перед выходом
			processBatch()

			wp.logger.Info("🔧 Worker stopped by context",
				"exchange", exchange,
				"worker_id", id,
				"processed", processedCount)
			return

		case <-batchTicker.C:
			// Периодически обрабатываем батч
			processBatch()

		case msg, ok := <-ch:
			if !ok {
				// Канал закрыт - обрабатываем оставшийся батч и выходим
				processBatch()

				wp.logger.Info("🔧 Worker stopped, channel closed",
					"exchange", exchange,
					"worker_id", id,
					"processed", processedCount)
				return
			}

			// Добавляем в батч
			batchBuffer = append(batchBuffer, msg)
			processedCount++

			// Если батч заполнен, обрабатываем его
			if len(batchBuffer) >= 10 {
				processBatch()
			}

			// Логируем прогресс каждые 1000 сообщений
			if processedCount%1000 == 0 {
				wp.logger.Debug("Worker progress",
					"exchange", exchange,
					"worker_id", id,
					"processed", processedCount)
			}
		}
	}
}

// Wait ожидает завершения всех воркеров
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

// GetTotalProcessed возвращает общее количество обработанных сообщений
func (wp *WorkerPool) GetTotalProcessed() int64 {
	return atomic.LoadInt64(&wp.totalProcessed)
}
