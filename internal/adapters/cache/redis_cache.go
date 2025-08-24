package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"marketflow/internal/domain"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Константы для настройки поведения
const (
	RedisReconnectInterval = 10 * time.Second // Интервал попыток переподключения к Redis
)

type RedisCache struct {
	client          *redis.Client
	logger          *slog.Logger
	ctx             context.Context
	storage         domain.StoragePort // Резервное хранилище (PostgreSQL)
	redisAvailable  bool               // Флаг доступности Redis
	mutex           sync.RWMutex       // Мьютекс для потокобезопасного доступа к флагу
	reconnectCtx    context.Context    // Контекст для управления горутиной переподключения
	reconnectCancel context.CancelFunc // Функция отмены контекста
}

func NewRedisCache(addr, password string, db int, logger *slog.Logger) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx := context.Background()
	reconnectCtx, reconnectCancel := context.WithCancel(ctx)

	// Проверяем подключение
	redisAvailable := true
	_, err := client.Ping(ctx).Result()
	if err != nil {
		// Логируем ошибку, но не прерываем инициализацию
		logger.Error("Failed to connect to Redis", "error", err)
		redisAvailable = false
	}

	logger.Info("Redis cache initialized", "available", redisAvailable)

	cache := &RedisCache{
		client:          client,
		logger:          logger,
		ctx:             ctx,
		redisAvailable:  redisAvailable,
		reconnectCtx:    reconnectCtx,
		reconnectCancel: reconnectCancel,
	}

	// Запускаем горутину для мониторинга состояния Redis
	go cache.monitorRedisConnection()

	return cache, nil
}

// SetStorage устанавливает резервное хранилище (PostgreSQL)
func (c *RedisCache) SetStorage(storage domain.StoragePort) {
	c.storage = storage
	c.logger.Info("Fallback storage set for Redis cache")
}

// monitorRedisConnection периодически проверяет доступность Redis
func (c *RedisCache) monitorRedisConnection() {
	ticker := time.NewTicker(RedisReconnectInterval)
	defer ticker.Stop()

	c.logger.Info("Starting Redis connection monitor")

	for {
		select {
		case <-c.reconnectCtx.Done():
			c.logger.Info("Redis connection monitor stopped")
			return
		case <-ticker.C:
			c.checkRedisConnection()
		}
	}
}

// checkRedisConnection проверяет подключение к Redis и обновляет статус
func (c *RedisCache) checkRedisConnection() {
	// Проверяем подключение к Redis
	_, err := c.client.Ping(c.ctx).Result()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if err != nil {
		// Если Redis был доступен, но стал недоступен, логируем ошибку
		if c.redisAvailable {
			c.logger.Error("Redis became unavailable", "error", err)
			c.redisAvailable = false
		}
	} else {
		// Если Redis был недоступен, но стал доступен, логируем восстановление
		if !c.redisAvailable {
			c.logger.Info("Redis connection restored")
			c.redisAvailable = true
		}
	}
}

// isRedisAvailable проверяет доступность Redis (потокобезопасно)
func (c *RedisCache) isRedisAvailable() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.redisAvailable
}

// CachePrice - кэширует последнюю цену
func (c *RedisCache) CachePrice(message domain.Message) error {
	// Если Redis недоступен и есть резервное хранилище
	if !c.isRedisAvailable() {
		c.logger.Debug("Redis unavailable, using fallback storage",
			"symbol", message.Symbol,
			"exchange", message.Exchange)

		if c.storage != nil {
			// Сохраняем сообщение напрямую в PostgreSQL
			return c.storage.SavePriceUpdate(message)
		}
		return fmt.Errorf("redis unavailable and no fallback storage")
	}

	// Ключ для последней цены: latest:BTCUSDT:Exchange1
	key := fmt.Sprintf("latest:%s:%s", message.Symbol, message.Exchange)

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Храним с TTL 2 минуты (последняя минута + запас)
	err = c.client.Set(c.ctx, key, data, 2*time.Minute).Err()
	if err != nil {
		// Если произошла ошибка Redis, обновляем статус и используем резервное хранилище
		c.mutex.Lock()
		c.redisAvailable = false
		c.mutex.Unlock()

		c.logger.Error("Failed to cache price in Redis", "error", err)

		if c.storage != nil {
			return c.storage.SavePriceUpdate(message)
		}
		return fmt.Errorf("redis operation failed and no fallback storage: %w", err)
	}

	// Также добавляем в sorted set для истории последней минуты
	historyKey := fmt.Sprintf("history:%s:%s", message.Symbol, message.Exchange)
	score := float64(message.Timestamp.UnixMilli())

	err = c.client.ZAdd(c.ctx, historyKey, redis.Z{
		Score:  score,
		Member: data,
	}).Err()
	if err != nil {
		c.logger.Error("Failed to add to history in Redis", "error", err)
		return err
	}

	// Удаляем старые записи (старше 1 минуты)
	minScore := float64(time.Now().Add(-time.Minute).UnixMilli())
	err = c.client.ZRemRangeByScore(c.ctx, historyKey, "0", fmt.Sprintf("%f", minScore)).Err()
	if err != nil {
		c.logger.Error("Failed to remove old records from Redis", "error", err)
	}

	return err
}

// GetCachedPrice - получает последнюю цену для символа (с любой биржи)
func (c *RedisCache) GetCachedPrice(symbol string) (domain.Message, error) {
	// Если Redis недоступен и есть резервное хранилище
	if !c.isRedisAvailable() {
		c.logger.Debug("Redis unavailable, using fallback storage for GetCachedPrice", "symbol", symbol)

		if c.storage != nil {
			// Получаем последнюю цену из PostgreSQL
			return c.storage.GetLatestPrice(symbol)
		}
		return domain.Message{}, fmt.Errorf("redis unavailable and no fallback storage")
	}

	pattern := fmt.Sprintf("latest:%s:*", symbol)
	keys, err := c.client.Keys(c.ctx, pattern).Result()
	if err != nil {
		// Если произошла ошибка Redis, обновляем статус и используем резервное хранилище
		c.mutex.Lock()
		c.redisAvailable = false
		c.mutex.Unlock()

		c.logger.Error("Failed to get keys from Redis", "error", err)

		if c.storage != nil {
			return c.storage.GetLatestPrice(symbol)
		}
		return domain.Message{}, fmt.Errorf("redis operation failed and no fallback storage: %w", err)
	}

	if len(keys) == 0 {
		return domain.Message{}, fmt.Errorf("no cached price for symbol %s", symbol)
	}

	// Берём первый найденный ключ
	data, err := c.client.Get(c.ctx, keys[0]).Result()
	if err != nil {
		if err == redis.Nil {
			return domain.Message{}, fmt.Errorf("no cached price for symbol %s", symbol)
		}

		// Если произошла ошибка Redis, обновляем статус и используем резервное хранилище
		c.mutex.Lock()
		c.redisAvailable = false
		c.mutex.Unlock()

		c.logger.Error("Failed to get data from Redis", "error", err)

		if c.storage != nil {
			return c.storage.GetLatestPrice(symbol)
		}
		return domain.Message{}, fmt.Errorf("redis operation failed and no fallback storage: %w", err)
	}

	var msg domain.Message
	err = json.Unmarshal([]byte(data), &msg)
	return msg, err
}

// GetCachedPriceByExchange - получает последнюю цену для символа с конкретной биржи
func (c *RedisCache) GetCachedPriceByExchange(symbol, exchange string) (domain.Message, error) {
	// Если Redis недоступен и есть резервное хранилище
	if !c.isRedisAvailable() {
		c.logger.Debug("Redis unavailable, using fallback storage for GetCachedPriceByExchange",
			"symbol", symbol,
			"exchange", exchange)

		if c.storage != nil {
			// В PostgreSQL нет прямого метода для получения последней цены для конкретной биржи
			// Используем workaround через получение агрегированных данных
			now := time.Now()
			from := now.Add(-24 * time.Hour) // Последние 24 часа
			data, err := c.storage.GetAggregatedData(symbol, exchange, from, now)
			if err != nil || len(data) == 0 {
				return domain.Message{}, fmt.Errorf("no data available from fallback storage: %w", err)
			}

			// Берем самую свежую запись
			latest := data[0]
			return domain.Message{
				Symbol:    latest.PairName,
				Exchange:  latest.Exchange,
				Price:     latest.AveragePrice,
				Timestamp: latest.Timestamp,
			}, nil
		}
		return domain.Message{}, fmt.Errorf("redis unavailable and no fallback storage")
	}

	key := fmt.Sprintf("latest:%s:%s", symbol, exchange)

	data, err := c.client.Get(c.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return domain.Message{}, fmt.Errorf("no cached price for %s on %s", symbol, exchange)
		}

		// Если произошла ошибка Redis, обновляем статус и используем резервное хранилище
		c.mutex.Lock()
		c.redisAvailable = false
		c.mutex.Unlock()

		c.logger.Error("Failed to get data from Redis", "error", err)

		if c.storage != nil {
			// Используем тот же workaround через GetAggregatedData
			now := time.Now()
			from := now.Add(-24 * time.Hour)
			data, err := c.storage.GetAggregatedData(symbol, exchange, from, now)
			if err != nil || len(data) == 0 {
				return domain.Message{}, fmt.Errorf("no data available from fallback storage: %w", err)
			}

			latest := data[0]
			return domain.Message{
				Symbol:    latest.PairName,
				Exchange:  latest.Exchange,
				Price:     latest.AveragePrice,
				Timestamp: latest.Timestamp,
			}, nil
		}
		return domain.Message{}, fmt.Errorf("redis operation failed and no fallback storage: %w", err)
	}

	var msg domain.Message
	err = json.Unmarshal([]byte(data), &msg)
	return msg, err
}

// GetPricesInRange - получает все цены за указанный период
func (c *RedisCache) GetPricesInRange(symbol string, duration time.Duration) ([]domain.Message, error) {
	// Если Redis недоступен и есть резервное хранилище
	if !c.isRedisAvailable() {
		c.logger.Debug("Redis unavailable, using fallback storage for GetPricesInRange",
			"symbol", symbol,
			"duration", duration)

		if c.storage != nil {
			// Используем данные из PostgreSQL
			now := time.Now()
			from := now.Add(-duration)

			// Получаем агрегированные данные
			aggregated, err := c.storage.GetAggregatedData(symbol, "", from, now)
			if err != nil {
				return nil, fmt.Errorf("failed to get data from fallback storage: %w", err)
			}

			// Преобразуем агрегированные данные в формат Message
			messages := make([]domain.Message, len(aggregated))
			for i, data := range aggregated {
				messages[i] = domain.Message{
					Symbol:    data.PairName,
					Exchange:  data.Exchange,
					Price:     data.AveragePrice, // Используем среднюю цену
					Timestamp: data.Timestamp,
				}
			}

			return messages, nil
		}
		return nil, fmt.Errorf("redis unavailable and no fallback storage")
	}

	pattern := fmt.Sprintf("history:%s:*", symbol)
	keys, err := c.client.Keys(c.ctx, pattern).Result()
	if err != nil {
		// Если произошла ошибка Redis, обновляем статус и используем резервное хранилище
		c.mutex.Lock()
		c.redisAvailable = false
		c.mutex.Unlock()

		c.logger.Error("Failed to get keys from Redis", "error", err)

		if c.storage != nil {
			// Используем данные из PostgreSQL (аналогично коду выше)
			now := time.Now()
			from := now.Add(-duration)

			aggregated, err := c.storage.GetAggregatedData(symbol, "", from, now)
			if err != nil {
				return nil, fmt.Errorf("failed to get data from fallback storage: %w", err)
			}

			messages := make([]domain.Message, len(aggregated))
			for i, data := range aggregated {
				messages[i] = domain.Message{
					Symbol:    data.PairName,
					Exchange:  data.Exchange,
					Price:     data.AveragePrice,
					Timestamp: data.Timestamp,
				}
			}

			return messages, nil
		}
		return nil, fmt.Errorf("redis operation failed and no fallback storage: %w", err)
	}

	var allMessages []domain.Message
	minTime := float64(time.Now().Add(-duration).UnixMilli())
	maxTime := float64(time.Now().UnixMilli())

	for _, key := range keys {
		// Получаем записи из sorted set за нужный период
		results, err := c.client.ZRangeByScoreWithScores(c.ctx, key, &redis.ZRangeBy{
			Min: fmt.Sprintf("%f", minTime),
			Max: fmt.Sprintf("%f", maxTime),
		}).Result()
		if err != nil {
			c.logger.Error("Failed to get history", "key", key, "error", err)
			continue
		}

		for _, result := range results {
			var msg domain.Message
			if err := json.Unmarshal([]byte(result.Member.(string)), &msg); err != nil {
				c.logger.Error("Failed to unmarshal message", "error", err)
				continue
			}
			allMessages = append(allMessages, msg)
		}
	}

	return allMessages, nil
}

// Close - закрывает соединение с Redis
func (c *RedisCache) Close() error {
	// Останавливаем горутину мониторинга
	c.reconnectCancel()

	// Закрываем соединение с Redis
	return c.client.Close()
}
