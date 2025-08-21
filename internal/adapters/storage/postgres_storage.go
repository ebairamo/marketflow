package storage

import (
	"database/sql"
	"fmt"
	"log/slog"
	"marketflow/internal/domain"
	"time"

	_ "github.com/lib/pq"
)

type PostgresStorage struct {
	db     *sql.DB
	logger *slog.Logger
}

func NewPostgresStorage(connectionString string, logger *slog.Logger) (*PostgresStorage, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	storage := &PostgresStorage{
		db:     db,
		logger: logger,
	}

	if err := storage.createTables(); err != nil {
		return nil, err
	}

	logger.Info("PostgreSQL storage initialized successfully")
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

// SaveAggregatedData - сохраняет агрегированные данные
func (s *PostgresStorage) SaveAggregatedData(data domain.AggregatedData) error {
	query := `
        INSERT INTO price_aggregates 
        (pair_name, exchange, timestamp, average_price, min_price, max_price)
        VALUES ($1, $2, $3, $4, $5, $6)`

	_, err := s.db.Exec(query,
		data.PairName,
		data.Exchange,
		data.Timestamp,
		data.AveragePrice,
		data.MinPrice,
		data.MaxPrice,
	)
	if err != nil {
		return fmt.Errorf("failed to save aggregated data: %w", err)
	}

	s.logger.Debug("Saved aggregated data",
		"pair", data.PairName,
		"exchange", data.Exchange,
		"avg", data.AveragePrice)

	return nil
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

// Close - закрывает соединение с БД
func (s *PostgresStorage) Close() error {
	return s.db.Close()
}
