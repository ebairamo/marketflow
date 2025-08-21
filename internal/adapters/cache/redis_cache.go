package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"marketflow/internal/domain"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client *redis.Client
	logger *slog.Logger
	ctx    context.Context
}

func NewRedisCache(addr, password string, db int, logger *slog.Logger) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx := context.Background()

	// Проверяем подключение
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	logger.Info("Redis cache initialized successfully")

	return &RedisCache{
		client: client,
		logger: logger,
		ctx:    ctx,
	}, nil
}

// CachePrice - кэширует последнюю цену
func (c *RedisCache) CachePrice(message domain.Message) error {
	// Ключ для последней цены: latest:BTCUSDT:Exchange1
	key := fmt.Sprintf("latest:%s:%s", message.Symbol, message.Exchange)

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Храним с TTL 2 минуты (последняя минута + запас)
	err = c.client.Set(c.ctx, key, data, 2*time.Minute).Err()
	if err != nil {
		return fmt.Errorf("failed to cache price: %w", err)
	}

	// Также добавляем в sorted set для истории последней минуты
	historyKey := fmt.Sprintf("history:%s:%s", message.Symbol, message.Exchange)
	score := float64(message.Timestamp.UnixMilli())

	err = c.client.ZAdd(c.ctx, historyKey, redis.Z{
		Score:  score,
		Member: data,
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to add to history: %w", err)
	}

	// Удаляем старые записи (старше 1 минуты)
	minScore := float64(time.Now().Add(-time.Minute).UnixMilli())
	err = c.client.ZRemRangeByScore(c.ctx, historyKey, "0", fmt.Sprintf("%f", minScore)).Err()

	return err
}

// GetCachedPrice - получает последнюю цену для символа (с любой биржи)
func (c *RedisCache) GetCachedPrice(symbol string) (domain.Message, error) {
	pattern := fmt.Sprintf("latest:%s:*", symbol)
	keys, err := c.client.Keys(c.ctx, pattern).Result()
	if err != nil {
		return domain.Message{}, fmt.Errorf("failed to get keys: %w", err)
	}

	if len(keys) == 0 {
		return domain.Message{}, fmt.Errorf("no cached price for symbol %s", symbol)
	}

	// Берём первый найденный ключ
	data, err := c.client.Get(c.ctx, keys[0]).Result()
	if err != nil {
		return domain.Message{}, fmt.Errorf("failed to get cached price: %w", err)
	}

	var msg domain.Message
	err = json.Unmarshal([]byte(data), &msg)
	return msg, err
}

// GetCachedPriceByExchange - получает последнюю цену для символа с конкретной биржи
func (c *RedisCache) GetCachedPriceByExchange(symbol, exchange string) (domain.Message, error) {
	key := fmt.Sprintf("latest:%s:%s", symbol, exchange)

	data, err := c.client.Get(c.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return domain.Message{}, fmt.Errorf("no cached price for %s on %s", symbol, exchange)
		}
		return domain.Message{}, fmt.Errorf("failed to get cached price: %w", err)
	}

	var msg domain.Message
	err = json.Unmarshal([]byte(data), &msg)
	return msg, err
}

// GetPricesInRange - получает все цены за указанный период
func (c *RedisCache) GetPricesInRange(symbol string, duration time.Duration) ([]domain.Message, error) {
	pattern := fmt.Sprintf("history:%s:*", symbol)
	keys, err := c.client.Keys(c.ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get keys: %w", err)
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
	return c.client.Close()
}
