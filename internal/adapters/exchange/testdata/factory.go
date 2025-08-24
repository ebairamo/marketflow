package testdata

import (
	"log/slog"
	"marketflow/internal/domain"
	"time"
)

// CreateTestExchanges создает набор тестовых генераторов, имитирующих биржи
func CreateTestExchanges(logger *slog.Logger) []domain.ExchangePort {
	// Создаем три генератора с разными интервалами для имитации разных скоростей бирж
	exchanges := []domain.ExchangePort{
		NewTestDataGenerator("TestExchange1", 100*time.Millisecond, logger),
		NewTestDataGenerator("TestExchange2", 120*time.Millisecond, logger),
		NewTestDataGenerator("TestExchange3", 80*time.Millisecond, logger),
	}

	logger.Info("Created test exchanges", "count", len(exchanges))
	return exchanges
}
