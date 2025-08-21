package main

import (
	"context"
	"fmt"
	"marketflow/config"
	"marketflow/internal/adapters/cache"
	"marketflow/internal/adapters/exchange"
	"marketflow/internal/adapters/storage"
	"marketflow/internal/application/concurrency"
	"marketflow/internal/application/services"
	"marketflow/internal/domain"
	"marketflow/pkg/logger"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log := logger.SetupLogger()
	log.Info("🚀 Запуск MarketFlow - системы обработки биржевых данных")

	// Загружаем конфигурацию
	cfg, err := config.LoadConfig("config/config.yaml")
	if err != nil {
		log.Error("❌ Ошибка загрузки конфигурации", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Подключаемся к PostgreSQL
	log.Info("📊 Подключаемся к PostgreSQL...")
	postgresStorage, err := storage.NewPostgresStorage(cfg.Postgres.ConnectionString(), log)
	if err != nil {
		log.Error("❌ Ошибка подключения к PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer postgresStorage.Close()

	// Подключаемся к Redis
	log.Info("💾 Подключаемся к Redis...")
	redisAddr := fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port)
	redisCache, err := cache.NewRedisCache(redisAddr, cfg.Redis.Password, cfg.Redis.DB, log)
	if err != nil {
		log.Error("❌ Ошибка подключения к Redis", "error", err)
		os.Exit(1)
	}
	defer redisCache.Close()

	// Создаём биржи
	log.Info("🏛️ Подключаемся к биржам...")
	var exchanges []domain.ExchangePort
	for _, exCfg := range cfg.Exchanges {
		exchange := exchange.NewTCPExchange(exCfg.Address, exCfg.Name, log)
		exchanges = append(exchanges, exchange)
	}

	// Создаём сервис обработки
	marketService := services.NewMarketService(exchanges, postgresStorage, redisCache, log)

	workerPool := concurrency.NewWorkerPool(5, postgresStorage, redisCache, log)

	// Запускаем обработку
	messageCh, errCh := marketService.Start(ctx)

	workerPool.Start(ctx, messageCh)

	// Обработка сигналов завершения
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	log.Info("✅ Система запущена и готова к работе!")

	// Счётчики для статистики
	messageCount := 0
	startTime := time.Now()

	for {
		select {
		case msg := <-messageCh:
			messageCount++

			// Кэшируем в Redis
			if err := redisCache.CachePrice(msg); err != nil {
				log.Error("Ошибка кэширования", "error", err)
			}

			// Выводим каждое 100-е сообщение для экономии логов
			if messageCount%100 == 0 {
				rate := float64(messageCount) / time.Since(startTime).Seconds()
				log.Info("📈 Статистика обработки",
					"total", messageCount,
					"rate", fmt.Sprintf("%.2f msg/sec", rate),
					"last_symbol", msg.Symbol,
					"last_price", msg.Price)
			}

		case err := <-errCh:
			log.Error("❌ Ошибка обработки", "error", err)

		case <-signalCh:
			log.Info("🛑 Получен сигнал завершения, останавливаем систему...")
			cancel()
			time.Sleep(time.Second) // Даём время на graceful shutdown
			log.Info("👋 MarketFlow остановлен. До свидания!")
			return
		}
	}
}
