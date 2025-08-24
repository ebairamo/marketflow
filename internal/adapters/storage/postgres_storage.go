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
}

func NewPostgresStorage(connectionString string, logger *slog.Logger) (*PostgresStorage, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	// Создаем контекст для управления горутиной батчинга
	ctx, cancel := context.WithCancel(context.Background())

	storage := &PostgresStorage{
		db:              db,
		logger:          logger,
		aggregateBuffer: make([]domain.AggregatedData, 0, BatchSize),
		batchingCtx:     ctx,
		batchingCancel:  cancel,
	}

	if err := storage.createTables(); err != nil {
		cancel() // Отменяем контекст в случае ошибки
		return nil, err
	}

	// Запускаем горутину для периодической записи батчей
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
    ON price_aggregates(pair_name, exchange, timestamp);`

	_, err := s.db.Exec(query)
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

	row := s.db.QueryRow(query, symbol)
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

	// Добавляем данные в буфер
	s.aggregateBuffer = append(s.aggregateBuffer, data)

	// Если буфер достиг максимального размера, записываем его в БД
	if len(s.aggregateBuffer) >= BatchSize {
		return s.flushBufferLocked()
	}

	return nil
}

// flushBufferLocked - записывает все данные из буфера в БД (должен вызываться с заблокированным мьютексом)
func (s *PostgresStorage) flushBufferLocked() error {
	if len(s.aggregateBuffer) == 0 {
		return nil
	}

	// Начинаем транзакцию
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Prepare statement для пакетной вставки
	stmt, err := tx.Prepare(`
		INSERT INTO price_aggregates 
		(pair_name, exchange, timestamp, average_price, min_price, max_price)
		VALUES ($1, $2, $3, $4, $5, $6)
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Выполняем вставку для каждой записи в буфере
	for _, data := range s.aggregateBuffer {
		_, err := stmt.Exec(
			data.PairName,
			data.Exchange,
			data.Timestamp,
			data.AveragePrice,
			data.MinPrice,
			data.MaxPrice,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute batch insert: %w", err)
		}
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.logger.Info("Batch saved to database", "records", len(s.aggregateBuffer))

	// Очищаем буфер
	s.aggregateBuffer = make([]domain.AggregatedData, 0, BatchSize)

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
	ticker := time.NewTicker(time.Duration(BatchTimeoutSec) * time.Second)
	defer ticker.Stop()

	s.logger.Info("Starting batch processor", "timeout_sec", BatchTimeoutSec)

	for {
		select {
		case <-ticker.C:
			// Периодически записываем данные из буфера, даже если он не заполнен
			if err := s.FlushBuffer(); err != nil {
				s.logger.Error("Failed to flush buffer", "error", err)
			}
		case <-s.batchingCtx.Done():
			// Завершаем работу при отмене контекста
			s.logger.Info("Batch processor shutting down")
			// Записываем оставшиеся данные
			if err := s.FlushBuffer(); err != nil {
				s.logger.Error("Failed to flush buffer during shutdown", "error", err)
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
        ORDER BY timestamp DESC`

	rows, err := s.db.Query(query, symbol, exchange, from, to)
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

	return results, nil
}

// Close - закрывает соединение с БД и выполняет необходимые действия при закрытии
func (s *PostgresStorage) Close() error {
	// Отменяем контекст для остановки горутины батчинга
	s.batchingCancel()

	// Записываем оставшиеся данные из буфера
	if err := s.FlushBuffer(); err != nil {
		s.logger.Error("Failed to flush buffer during closing", "error", err)
	}

	// Закрываем соединение с БД
	return s.db.Close()
}
