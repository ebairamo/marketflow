package marketflow

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
	"strconv"
	"sync"
	"syscall"
	"time"
)

func Run() {
	// Обработка флагов командной строки
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "--help", "-h":
			printUsage()
			return
		case "--port":
			if len(os.Args) < 3 {
				fmt.Fprintf(os.Stderr, "Error: --port requires a port number\n")
				printUsage()
				os.Exit(1)
			}
			_, err := strconv.Atoi(os.Args[2])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid port number '%s'\n", os.Args[2])
				printUsage()
				os.Exit(1)
			}
			// Порт будет использоваться из конфигурации, здесь просто валидируем
		default:
			if os.Args[1][0] == '-' {
				fmt.Fprintf(os.Stderr, "Error: unknown flag '%s'\n", os.Args[1])
				printUsage()
				os.Exit(1)
			}
		}
	}

	log := logger.SetupLogger()
	log.Info("🚀 Запуск MarketFlow - системы обработки биржевых данных")

	// Загружаем конфигурацию
	cfg, err := config.LoadConfig("config/config.yaml")
	if err != nil {
		log.Error("❌ Ошибка загрузки конфигурации", "error", err)
		os.Exit(1)
	}

	// Переопределяем порт из командной строки, если указан
	if len(os.Args) > 2 && os.Args[1] == "--port" {
		if port, err := strconv.Atoi(os.Args[2]); err == nil {
			cfg.API.Port = port
		}
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
	defer func() {
		if err := postgresStorage.Close(); err != nil {
			log.Error("Ошибка закрытия PostgreSQL", "error", err)
		}
	}()

	// Подключаемся к Redis
	log.Info("💾 Подключаемся к Redis...")
	redisAddr := fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port)
	redisCache, err := cache.NewRedisCache(redisAddr, cfg.Redis.Password, cfg.Redis.DB, log)
	if err != nil {
		log.Error("❌ Ошибка подключения к Redis", "error", err)
		os.Exit(1)
	}
	// Устанавливаем fallback storage
	redisCache.SetStorage(postgresStorage)
	defer func() {
		if err := redisCache.Close(); err != nil {
			log.Error("Ошибка закрытия Redis", "error", err)
		}
	}()

	// Создаём биржи для Live режима
	log.Info("🏛️ Инициализация настройки бирж...")
	var liveExchanges []domain.ExchangePort
	var liveExchangeNames []string
	for _, exCfg := range cfg.Exchanges {
		exchange := exchange.NewTCPExchange(exCfg.Address, exCfg.Name, log)
		liveExchanges = append(liveExchanges, exchange)
		liveExchangeNames = append(liveExchangeNames, exCfg.Name)
	}

	// Имена тестовых бирж
	testExchangeNames := []string{"TestExchange1", "TestExchange2", "TestExchange3"}

	// Переменные для управления режимами
	var (
		currentExchanges  []domain.ExchangePort
		marketService     *services.MarketService
		workerPool        *concurrency.WorkerPool
		aggregationCancel context.CancelFunc
		serviceCtx        context.Context
		serviceCancel     context.CancelFunc
		messageCh         <-chan domain.Message
		errCh             <-chan error
		mu                sync.Mutex // Защита при переключении режимов
	)

	// Функция для запуска сервисов
	startServices := func(exchanges []domain.ExchangePort, exchangeNames []string, webHandler *web.Handler) {
		mu.Lock()
		defer mu.Unlock()

		// Останавливаем предыдущие сервисы если они есть
		if serviceCancel != nil {
			serviceCancel()
			// Даём время на graceful shutdown
			time.Sleep(500 * time.Millisecond)
		}

		if aggregationCancel != nil {
			aggregationCancel()
		}

		// Создаём новый контекст для сервисов
		serviceCtx, serviceCancel = context.WithCancel(ctx)

		// Обновляем список валидных бирж в handler
		webHandler.UpdateValidExchanges(exchangeNames)

		// Создаём сервис обработки рыночных данных
		marketService = services.NewMarketService(exchanges, postgresStorage, redisCache, log)

		// Запускаем обработку данных
		messageCh, errCh = marketService.Start(serviceCtx)

		// Создаём и запускаем Worker Pool
		workerPool = concurrency.NewWorkerPool(5, postgresStorage, redisCache, log)
		workerPool.Start(serviceCtx, messageCh)

		// Запускаем периодическую агрегацию данных
		var aggregationCtx context.Context
		aggregationCtx, aggregationCancel = context.WithCancel(serviceCtx)
		go startAggregationTask(aggregationCtx, redisCache, postgresStorage, log)
	}

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

	// Запускаем начальные сервисы (Live режим по умолчанию)
	log.Info("🔧 Запуск в Live режиме")
	currentExchanges = liveExchanges
	startServices(currentExchanges, liveExchangeNames, webHandler)

	// Обработка сигналов завершения
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Канал для переключения режимов
	modeCh := webHandler.GetModeChannel()

	log.Info("✅ Система запущена и готова к работе!")

	// Счётчики для статистики
	messageCount := 0
	errorCount := 0
	startTime := time.Now()
	lastStatTime := time.Now()

	// Основной цикл обработки
	for {
		select {
		// Обработка переключения режимов
		case mode := <-modeCh:
			if mode {
				log.Info("🔄 Переключение в Live режим")
				currentExchanges = liveExchanges
				startServices(currentExchanges, liveExchangeNames, webHandler)

				// Сбрасываем счетчики статистики
				messageCount = 0
				errorCount = 0
				startTime = time.Now()
			} else {
				log.Info("🔄 Переключение в Test режим")

				// Создаем тестовые биржи
				testExchanges := testdata.CreateTestExchanges(log)
				currentExchanges = testExchanges
				startServices(currentExchanges, testExchangeNames, webHandler)

				// Сбрасываем счетчики статистики
				messageCount = 0
				errorCount = 0
				startTime = time.Now()
			}

		// Обработка сообщений
		case _, ok := <-messageCh:
			if !ok {
				// Канал закрыт - это нормально при переключении режимов
				continue
			}

			messageCount++

			// Выводим статистику каждые 10 секунд
			if time.Since(lastStatTime) > 10*time.Second {
				rate := float64(messageCount) / time.Since(startTime).Seconds()
				log.Info("📈 Статистика обработки",
					"total_messages", messageCount,
					"total_errors", errorCount,
					"rate", fmt.Sprintf("%.2f msg/sec", rate),
					"uptime", time.Since(startTime).Round(time.Second))
				lastStatTime = time.Now()
			}

		// Обработка ошибок
		case err, ok := <-errCh:
			if !ok {
				// Канал закрыт - это нормально при переключении режимов
				continue
			}

			errorCount++

			// Логируем только каждую 10-ю ошибку чтобы не спамить
			if errorCount%10 == 1 {
				log.Error("❌ Ошибка обработки",
					"error", err,
					"total_errors", errorCount)
			}

		// Обработка сигналов завершения
		case sig := <-signalCh:
			log.Info("🛑 Получен сигнал завершения, начинаем graceful shutdown...",
				"signal", sig)

			// Создаём контекст с таймаутом для shutdown
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownCancel()

			// WaitGroup для отслеживания завершения всех компонентов
			var shutdownWg sync.WaitGroup

			// 1. Останавливаем HTTP-сервер
			shutdownWg.Add(1)
			go func() {
				defer shutdownWg.Done()
				log.Info("Останавливаем HTTP-сервер...")
				if err := httpServer.Shutdown(shutdownCtx); err != nil {
					log.Error("Ошибка при остановке HTTP-сервера", "error", err)
				} else {
					log.Info("HTTP-сервер остановлен")
				}
			}()

			// 2. Останавливаем агрегацию
			if aggregationCancel != nil {
				log.Info("Останавливаем агрегацию данных...")
				aggregationCancel()
			}

			// 3. Останавливаем сервисы
			if serviceCancel != nil {
				log.Info("Останавливаем сервисы обработки данных...")
				serviceCancel()
			}

			// 4. Останавливаем MarketService
			if marketService != nil {
				shutdownWg.Add(1)
				go func() {
					defer shutdownWg.Done()
					log.Info("Останавливаем market service...")
					marketService.Stop()
					log.Info("Market service остановлен")
				}()
			}

			// 5. Ждём завершения Worker Pool
			if workerPool != nil {
				shutdownWg.Add(1)
				go func() {
					defer shutdownWg.Done()
					log.Info("Ожидаем завершения worker pool...")
					workerPool.Wait()
					totalProcessed := workerPool.GetTotalProcessed()
					log.Info("Worker pool завершён",
						"total_processed", totalProcessed)
				}()
			}

			// 6. Flush данных в PostgreSQL
			shutdownWg.Add(1)
			go func() {
				defer shutdownWg.Done()
				log.Info("Сохраняем оставшиеся данные в PostgreSQL...")
				if err := postgresStorage.FlushBuffer(); err != nil {
					log.Error("Ошибка при сохранении данных", "error", err)
				} else {
					log.Info("Данные успешно сохранены")
				}
			}()

			// Ждём завершения всех операций или таймаута
			done := make(chan struct{})
			go func() {
				shutdownWg.Wait()
				close(done)
			}()

			select {
			case <-done:
				log.Info("✅ Все компоненты успешно остановлены")
			case <-shutdownCtx.Done():
				log.Warn("⚠️ Таймаут shutdown, принудительное завершение")
			}

			// Отменяем основной контекст
			cancel()

			// Финальная статистика
			uptime := time.Since(startTime)
			log.Info("📊 Финальная статистика",
				"total_messages", messageCount,
				"total_errors", errorCount,
				"uptime", uptime.Round(time.Second),
				"avg_rate", fmt.Sprintf("%.2f msg/sec", float64(messageCount)/uptime.Seconds()))

			log.Info("👋 MarketFlow остановлен. До свидания!")
			return
		}
	}
}

func printUsage() {
	fmt.Print(`Usage:
  marketflow [--port <N>]
  marketflow --help

Options:
  --port N     Port number
`)
}

// Функция для периодической агрегации данных
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

			aggregatedCount := 0
			startTime := time.Now()

			for _, symbol := range symbols {
				// Получаем данные за последнюю минуту из Redis
				messages, err := cache.GetPricesInRange(symbol, time.Minute)
				if err != nil {
					logger.Error("Ошибка получения данных для агрегации",
						"error", err,
						"symbol", symbol)
					continue
				}

				if len(messages) == 0 {
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
						aggregatedCount++
						logger.Debug("Сохранены агрегированные данные",
							"symbol", symbol,
							"exchange", exchange,
							"avg", fmt.Sprintf("%.2f", avg),
							"min", fmt.Sprintf("%.2f", min),
							"max", fmt.Sprintf("%.2f", max),
							"samples", len(msgs))
					}
				}
			}

			if aggregatedCount > 0 {
				logger.Info("Агрегация завершена",
					"aggregated_records", aggregatedCount,
					"duration", time.Since(startTime).Round(time.Millisecond))
			}
		}
	}
}
