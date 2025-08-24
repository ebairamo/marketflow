package main

import (
	"context"
	"fmt"
	"log/slog"
	"marketflow/config"
	"marketflow/internal/adapters/cache"
	"marketflow/internal/adapters/exchange"
	"marketflow/internal/adapters/exchange/testdata"
	"marketflow/internal/adapters/storage"
	"marketflow/internal/adapters/web"
	"marketflow/internal/application/concurrency"
	"marketflow/internal/application/services"
	"marketflow/internal/domain"
	"marketflow/pkg/logger"
	"net/http"
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

	// Создаем основной контекст с возможностью отмены
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
	// После создания redisCache
	redisCache.SetStorage(postgresStorage)
	defer redisCache.Close()

	// Создаём биржи для Live режима
	log.Info("🏛️ Инициализация настройки бирж...")
	var liveExchanges []domain.ExchangePort
	for _, exCfg := range cfg.Exchanges {
		exchange := exchange.NewTCPExchange(exCfg.Address, exCfg.Name, log)
		liveExchanges = append(liveExchanges, exchange)
	}

	// Текущие активные биржи (по умолчанию - Live режим)
	var currentExchanges []domain.ExchangePort = liveExchanges

	// Создаём сервис обработки рыночных данных
	marketService := services.NewMarketService(currentExchanges, postgresStorage, redisCache, log)

	// Настраиваем воркеры
	workerPool := concurrency.NewWorkerPool(5, postgresStorage, redisCache, log)

	// Создаем и настраиваем HTTP-обработчики
	webHandler := web.NewHandler(redisCache, postgresStorage, log)
	httpServer := webHandler.Setup(cfg.API.Port)

	// Запускаем HTTP-сервер в отдельной горутине
	go func() {
		log.Info("🌐 Запуск HTTP-сервера на порту", "port", cfg.API.Port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("❌ Ошибка запуска HTTP-сервера", "error", err)
		}
	}()

	// Запускаем обработку данных
	messageCh, errCh := marketService.Start(ctx)

	// Запускаем Worker Pool
	workerPool.Start(ctx, messageCh)

	// Запускаем периодическую агрегацию данных (каждую минуту)
	aggregationCtx, aggregationCancel := context.WithCancel(ctx)
	go startAggregationTask(aggregationCtx, redisCache, postgresStorage, log)

	// Обработка сигналов завершения
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Канал для переключения режимов
	modeCh := webHandler.GetModeChannel()

	log.Info("✅ Система запущена и готова к работе!")

	// Счётчики для статистики
	messageCount := 0
	startTime := time.Now()

	// Основной цикл обработки
	for {
		select {
		// Обработка переключения режимов
		case mode := <-modeCh:
			if mode {
				log.Info("🔄 Переключение в Live режим")

				// Останавливаем текущие процессы агрегации
				aggregationCancel()

				// Создаем новый контекст для обработки данных
				newCtx, newCancel := context.WithCancel(context.Background())

				// Отменяем текущий контекст, останавливая все горутины
				cancel()

				// Обновляем контекст и его отмену
				ctx = newCtx
				cancel = newCancel

				// Используем Live биржи
				currentExchanges = liveExchanges

				// Создаем новый сервис с Live биржами
				marketService = services.NewMarketService(currentExchanges, postgresStorage, redisCache, log)

				// Запускаем обработку данных с новыми биржами
				messageCh, errCh = marketService.Start(ctx)

				// Запускаем новый Worker Pool
				workerPool = concurrency.NewWorkerPool(5, postgresStorage, redisCache, log)
				workerPool.Start(ctx, messageCh)

				// Запускаем новую задачу агрегации
				aggregationCtx, aggregationCancel = context.WithCancel(ctx)
				go startAggregationTask(aggregationCtx, redisCache, postgresStorage, log)

				// Сбрасываем счетчики статистики
				messageCount = 0
				startTime = time.Now()
			} else {
				log.Info("🔄 Переключение в Test режим")

				// Останавливаем текущие процессы агрегации
				aggregationCancel()

				// Создаем новый контекст для обработки данных
				newCtx, newCancel := context.WithCancel(context.Background())

				// Отменяем текущий контекст, останавливая все горутины
				cancel()

				// Обновляем контекст и его отмену
				ctx = newCtx
				cancel = newCancel

				// Создаем тестовые биржи
				testExchanges := testdata.CreateTestExchanges(log)
				currentExchanges = testExchanges

				// Создаем новый сервис с тестовыми биржами
				marketService = services.NewMarketService(currentExchanges, postgresStorage, redisCache, log)

				// Запускаем обработку данных с тестовыми биржами
				messageCh, errCh = marketService.Start(ctx)

				// Запускаем новый Worker Pool
				workerPool = concurrency.NewWorkerPool(5, postgresStorage, redisCache, log)
				workerPool.Start(ctx, messageCh)

				// Запускаем новую задачу агрегации
				aggregationCtx, aggregationCancel = context.WithCancel(ctx)
				go startAggregationTask(aggregationCtx, redisCache, postgresStorage, log)

				// Сбрасываем счетчики статистики
				messageCount = 0
				startTime = time.Now()
			}

		// Обработка сообщений
		case msg, ok := <-messageCh:
			if !ok {
				log.Error("Канал сообщений закрыт неожиданно")
				continue
			}

			messageCount++

			// Выводим каждое 100-е сообщение для экономии логов
			if messageCount%100 == 0 {
				rate := float64(messageCount) / time.Since(startTime).Seconds()
				log.Info("📈 Статистика обработки",
					"total", messageCount,
					"rate", fmt.Sprintf("%.2f msg/sec", rate),
					"last_symbol", msg.Symbol,
					"last_price", msg.Price)
			}

		// Обработка ошибок
		case err, ok := <-errCh:
			if !ok {
				log.Error("Канал ошибок закрыт неожиданно")
				continue
			}
			log.Error("❌ Ошибка обработки", "error", err)

		// Обработка сигналов завершения
		case <-signalCh:
			log.Info("🛑 Получен сигнал завершения, останавливаем систему...")

			// Сначала останавливаем HTTP-сервер
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()

			if err := httpServer.Shutdown(shutdownCtx); err != nil {
				log.Error("Ошибка при остановке HTTP-сервера", "error", err)
			}

			// Останавливаем агрегацию
			aggregationCancel()

			// Затем останавливаем остальные компоненты
			cancel()

			// Даём время на graceful shutdown
			time.Sleep(time.Second)

			log.Info("👋 MarketFlow остановлен. До свидания!")
			return
		}
	}
}

// Функция для периодической агрегации данных
// Изменено определение функции - теперь принимает *slog.Logger вместо slog.Logger
func startAggregationTask(ctx context.Context, cache domain.CachePort, storage domain.StoragePort, logger *slog.Logger) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	logger.Info("⏰ Запуск периодической агрегации данных (каждую минуту)")

	for {
		select {
		case <-ctx.Done():
			logger.Info("Остановка периодической агрегации данных")
			return
		case <-ticker.C:
			// Список всех поддерживаемых пар
			symbols := []string{"BTCUSDT", "DOGEUSDT", "TONUSDT", "SOLUSDT", "ETHUSDT"}

			for _, symbol := range symbols {
				// Получаем данные за последнюю минуту из Redis
				messages, err := cache.GetPricesInRange(symbol, time.Minute)
				if err != nil {
					logger.Error("Ошибка получения данных для агрегации", "error", err, "symbol", symbol)
					continue
				}

				// Группируем данные по бирже
				exchangeData := make(map[string][]domain.Message)
				for _, msg := range messages {
					exchangeData[msg.Exchange] = append(exchangeData[msg.Exchange], msg)
				}

				// Для каждой биржи вычисляем агрегированные данные
				for exchange, msgs := range exchangeData {
					if len(msgs) == 0 {
						continue
					}

					// Вычисляем min, max и avg
					var sum float64
					min := msgs[0].Price
					max := msgs[0].Price

					for _, m := range msgs {
						sum += m.Price
						if m.Price < min {
							min = m.Price
						}
						if m.Price > max {
							max = m.Price
						}
					}

					avg := sum / float64(len(msgs))

					// Создаем объект с агрегированными данными
					aggregated := domain.AggregatedData{
						PairName:     symbol,
						Exchange:     exchange,
						Timestamp:    time.Now(),
						AveragePrice: avg,
						MinPrice:     min,
						MaxPrice:     max,
					}

					// Сохраняем в PostgreSQL
					if err := storage.SaveAggregatedData(aggregated); err != nil {
						logger.Error("Ошибка сохранения агрегированных данных",
							"error", err,
							"symbol", symbol,
							"exchange", exchange)
					} else {
						logger.Info("Сохранены агрегированные данные",
							"symbol", symbol,
							"exchange", exchange,
							"avg", avg,
							"min", min,
							"max", max)
					}
				}
			}
		}
	}
}
