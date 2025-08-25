package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"marketflow/internal/domain"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// Константы для настройки батчинга
const (
	BatchSize       = 100 // Максимальное количество записей в одном батче
	BatchTimeoutSec = 10  // Таймаут записи батча в секундах
)

type PostgresStorage struct {
	db              *sql.DB
	logger          *slog.Logger
	aggregateBuffer []domain.AggregatedData // Буфер для накопления агрегированных данных
	bufferMutex     sync.Mutex              // Мьютекс для защиты буфера
	batchingCtx     context.Context         // Контекст для управления горутиной батчинга
	batchingCancel  context.CancelFunc      // Функция отмены контекста
	shutdownCh      chan struct{}           // Канал для сигнала о завершении
	wg              sync.WaitGroup          // Для ожидания завершения горутин
}

func NewPostgresStorage(connectionString string, logger *slog.Logger) (*PostgresStorage, error) {
	// Настраиваем пул соединений
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	// Настраиваем параметры пула
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Проверяем соединение
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	// Создаем контекст для управления горутиной батчинга
	batchCtx, batchCancel := context.WithCancel(context.Background())

	storage := &PostgresStorage{
		db:              db,
		logger:          logger,
		aggregateBuffer: make([]domain.AggregatedData, 0, BatchSize),
		batchingCtx:     batchCtx,
		batchingCancel:  batchCancel,
		shutdownCh:      make(chan struct{}),
	}

	if err := storage.createTables(); err != nil {
		batchCancel() // Отменяем контекст в случае ошибки
		db.Close()
		return nil, err
	}

	// Запускаем горутину для периодической записи батчей
	storage.wg.Add(1)
	go storage.startBatchProcessor()

	logger.Info("PostgreSQL storage initialized successfully with batching support",
		"batch_size", BatchSize,
		"batch_timeout", BatchTimeoutSec)
	return storage, nil
}

func (s *PostgresStorage) createTables() error {
	query := `
    CREATE TABLE IF NOT EXISTS price_aggregates (
        id SERIAL PRIMARY KEY,
        pair_name VARCHAR(20) NOT NULL,
        exchange VARCHAR(50) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        average_price DECIMAL(20, 8) NOT NULL,
        min_price DECIMAL(20, 8) NOT NULL,
        max_price DECIMAL(20, 8) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_pair_exchange_time 
    ON price_aggregates(pair_name, exchange, timestamp);
    
    CREATE INDEX IF NOT EXISTS idx_timestamp 
    ON price_aggregates(timestamp DESC);`

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	s.logger.Info("Database tables created successfully")
	return nil
}

// SavePriceUpdate - сохраняет одно обновление цены (для текущих данных)
func (s *PostgresStorage) SavePriceUpdate(message domain.Message) error {
	// Это может быть таблица для raw данных если нужно
	// Пока оставим пустым, так как основное хранение - агрегированные данные
	return nil
}

// GetLatestPrice - получает последнюю цену для символа
func (s *PostgresStorage) GetLatestPrice(symbol string) (domain.Message, error) {
	var msg domain.Message
	query := `
        SELECT pair_name, exchange, average_price, timestamp 
        FROM price_aggregates 
        WHERE pair_name = $1 
        ORDER BY timestamp DESC 
        LIMIT 1`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	row := s.db.QueryRowContext(ctx, query, symbol)
	err := row.Scan(&msg.Symbol, &msg.Exchange, &msg.Price, &msg.Timestamp)
	if err != nil {
		if err == sql.ErrNoRows {
			return msg, fmt.Errorf("no price found for symbol %s", symbol)
		}
		return msg, fmt.Errorf("failed to get latest price: %w", err)
	}

	return msg, nil
}

// SaveAggregatedData - добавляет агрегированные данные в буфер для пакетной записи
func (s *PostgresStorage) SaveAggregatedData(data domain.AggregatedData) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	// Проверяем, не идёт ли shutdown
	select {
	case <-s.shutdownCh:
		// Во время shutdown записываем напрямую
		return s.saveDirectly(data)
	default:
	}

	// Добавляем данные в буфер
	s.aggregateBuffer = append(s.aggregateBuffer, data)

	// Если буфер достиг максимального размера, записываем его в БД
	if len(s.aggregateBuffer) >= BatchSize {
		return s.flushBufferLocked()
	}

	return nil
}

// saveDirectly - сохраняет данные напрямую в БД (для критических случаев)
func (s *PostgresStorage) saveDirectly(data domain.AggregatedData) error {
	query := `
		INSERT INTO price_aggregates 
		(pair_name, exchange, timestamp, average_price, min_price, max_price)
		VALUES ($1, $2, $3, $4, $5, $6)`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.db.ExecContext(ctx,
		query,
		data.PairName,
		data.Exchange,
		data.Timestamp,
		data.AveragePrice,
		data.MinPrice,
		data.MaxPrice,
	)

	if err != nil {
		return fmt.Errorf("failed to save data directly: %w", err)
	}

	return nil
}

// flushBufferLocked - записывает все данные из буфера в БД (должен вызываться с заблокированным мьютексом)
func (s *PostgresStorage) flushBufferLocked() error {
	if len(s.aggregateBuffer) == 0 {
		return nil
	}

	// Создаём копию буфера для записи
	dataToSave := make([]domain.AggregatedData, len(s.aggregateBuffer))
	copy(dataToSave, s.aggregateBuffer)

	// Очищаем буфер сразу
	s.aggregateBuffer = make([]domain.AggregatedData, 0, BatchSize)

	// Разблокируем мьютекс на время записи в БД
	s.bufferMutex.Unlock()
	err := s.saveBatch(dataToSave)
	s.bufferMutex.Lock()

	if err != nil {
		// При ошибке возвращаем данные в буфер
		s.aggregateBuffer = append(dataToSave, s.aggregateBuffer...)
		return err
	}

	s.logger.Info("Batch saved to database", "records", len(dataToSave))
	return nil
}

// saveBatch - сохраняет батч данных в БД
func (s *PostgresStorage) saveBatch(data []domain.AggregatedData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Начинаем транзакцию
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Откатываем если не было commit

	// Prepare statement для пакетной вставки
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO price_aggregates 
		(pair_name, exchange, timestamp, average_price, min_price, max_price)
		VALUES ($1, $2, $3, $4, $5, $6)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Выполняем вставку для каждой записи
	for _, item := range data {
		_, err := stmt.ExecContext(ctx,
			item.PairName,
			item.Exchange,
			item.Timestamp,
			item.AveragePrice,
			item.MinPrice,
			item.MaxPrice,
		)
		if err != nil {
			return fmt.Errorf("failed to execute batch insert: %w", err)
		}
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// FlushBuffer - публичный метод для принудительной записи данных из буфера
func (s *PostgresStorage) FlushBuffer() error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	return s.flushBufferLocked()
}

// startBatchProcessor - запускает горутину для периодической записи батчей
func (s *PostgresStorage) startBatchProcessor() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Duration(BatchTimeoutSec) * time.Second)
	defer ticker.Stop()

	s.logger.Info("Starting batch processor", "timeout_sec", BatchTimeoutSec)

	for {
		select {
		case <-ticker.C:
			// Периодически записываем данные из буфера
			if err := s.FlushBuffer(); err != nil {
				s.logger.Error("Failed to flush buffer", "error", err)
			}

		case <-s.batchingCtx.Done():
			// Завершаем работу при отмене контекста
			s.logger.Info("Batch processor shutting down")

			// Финальная запись всех оставшихся данных
			retries := 3
			for i := 0; i < retries; i++ {
				if err := s.FlushBuffer(); err != nil {
					s.logger.Error("Failed to flush buffer during shutdown",
						"error", err,
						"attempt", i+1,
						"max_attempts", retries)

					if i < retries-1 {
						time.Sleep(time.Second)
						continue
					}
				}
				break
			}

			return
		}
	}
}

// GetAggregatedData - получает агрегированные данные за период
func (s *PostgresStorage) GetAggregatedData(symbol, exchange string, from, to time.Time) ([]domain.AggregatedData, error) {
	query := `
        SELECT id, pair_name, exchange, timestamp, average_price, min_price, max_price, created_at
        FROM price_aggregates
        WHERE pair_name = $1 
        AND ($2 = '' OR exchange = $2)
        AND timestamp >= $3 
        AND timestamp <= $4
        ORDER BY timestamp DESC
        LIMIT 1000`

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, query, symbol, exchange, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query aggregated data: %w", err)
	}
	defer rows.Close()

	var results []domain.AggregatedData
	for rows.Next() {
		var data domain.AggregatedData
		err := rows.Scan(
			&data.ID,
			&data.PairName,
			&data.Exchange,
			&data.Timestamp,
			&data.AveragePrice,
			&data.MinPrice,
			&data.MaxPrice,
			&data.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		results = append(results, data)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// Close - закрывает соединение с БД и выполняет необходимые действия при закрытии
func (s *PostgresStorage) Close() error {
	s.logger.Info("Closing PostgreSQL storage")

	// Сигнализируем о начале shutdown
	close(s.shutdownCh)

	// Отменяем контекст для остановки горутины батчинга
	s.batchingCancel()

	// Ждём завершения горутины батчинга
	s.wg.Wait()

	// Финальная попытка записать оставшиеся данные
	if err := s.FlushBuffer(); err != nil {
		s.logger.Error("Failed to flush buffer during closing", "error", err)
	}

	// Закрываем соединение с БД
	if err := s.db.Close(); err != nil {
		s.logger.Error("Failed to close database connection", "error", err)
		return err
	}

	s.logger.Info("PostgreSQL storage closed successfully")
	return nil
}
