package testdata

import (
	"context"
	"log/slog"
	"marketflow/internal/domain"
	"math/rand"
	"sync"
	"time"
)

// Начальные цены для криптовалют (близкие к реальным)
var initialPrices = map[string]float64{
	"BTCUSDT":  120000.0, // Bitcoin
	"ETHUSDT":  3000.0,   // Ethereum
	"SOLUSDT":  230.0,    // Solana
	"TONUSDT":  9.0,      // Toncoin
	"DOGEUSDT": 0.35,     // Dogecoin
}

// Параметры волатильности (мин/макс процентное изменение за тик)
var volatility = map[string]struct {
	min float64
	max float64
}{
	"BTCUSDT":  {-0.5, 0.5}, // Bitcoin менее волатилен
	"ETHUSDT":  {-1.0, 1.0}, // Ethereum более волатилен
	"SOLUSDT":  {-1.5, 1.5}, // Solana еще более волатилен
	"TONUSDT":  {-2.0, 2.0}, // Toncoin очень волатилен
	"DOGEUSDT": {-3.0, 3.0}, // Dogecoin самый волатильный
}

// TestDataGenerator реализует интерфейс ExchangePort для генерации тестовых данных
type TestDataGenerator struct {
	name        string              // Имя биржи
	prices      map[string]float64  // Текущие цены валютных пар
	messageChan chan domain.Message // Канал для отправки сообщений
	errChan     chan error          // Канал для отправки ошибок
	logger      *slog.Logger        // Логгер
	interval    time.Duration       // Интервал генерации данных
	running     bool                // Флаг работы генератора
	mutex       sync.Mutex          // Мьютекс для безопасного доступа к prices
	pairNames   []string            // Список названий пар
}

// NewTestDataGenerator создает новый генератор тестовых данных
func NewTestDataGenerator(name string, interval time.Duration, logger *slog.Logger) *TestDataGenerator {
	prices := make(map[string]float64)
	pairNames := make([]string, 0, len(initialPrices))

	// Инициализация начальных цен
	for pair, price := range initialPrices {
		prices[pair] = price
		pairNames = append(pairNames, pair)
	}

	return &TestDataGenerator{
		name:      name,
		prices:    prices,
		logger:    logger,
		interval:  interval,
		pairNames: pairNames,
	}
}

// Connect - имитация подключения (всегда успешна для генератора)
func (g *TestDataGenerator) Connect() error {
	g.logger.Info("Connecting test data generator", "exchange", g.Name())
	return nil
}

// Close - остановка генератора
func (g *TestDataGenerator) Close() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.running = false
	g.logger.Info("Test data generator closed", "exchange", g.Name())
	return nil
}

// IsConnected - проверка состояния генератора
func (g *TestDataGenerator) IsConnected() bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	return g.running
}

// Name - возвращает имя генератора
func (g *TestDataGenerator) Name() string {
	return g.name
}

// ReadPriceUpdates - начинает генерацию данных и возвращает каналы для сообщений и ошибок
func (g *TestDataGenerator) ReadPriceUpdates(ctx context.Context) (<-chan domain.Message, <-chan error) {
	g.mutex.Lock()

	// Создаем новые каналы для каждого вызова
	g.messageChan = make(chan domain.Message, 100)
	g.errChan = make(chan error, 10)
	g.running = true

	g.mutex.Unlock()

	go g.generateData(ctx)

	return g.messageChan, g.errChan
}

// generateData - горутина для генерации данных
func (g *TestDataGenerator) generateData(ctx context.Context) {
	defer func() {
		close(g.messageChan)
		close(g.errChan)
		g.logger.Info("Data generation stopped", "exchange", g.Name())
	}()

	g.logger.Info("Starting data generation", "exchange", g.Name(), "interval", g.interval)

	// Тикер для генерации данных с указанным интервалом
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			g.logger.Info("Context canceled, stopping data generation", "exchange", g.Name())
			return

		case <-ticker.C:
			// Проверяем, что генератор все еще работает
			g.mutex.Lock()
			if !g.running {
				g.mutex.Unlock()
				return
			}
			g.mutex.Unlock()

			// Генерируем данные для всех пар
			for _, pair := range g.pairNames {
				// Небольшая случайная задержка для имитации асинхронного поступления данных
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

				// Генерируем новую цену
				newPrice := g.generateNewPrice(pair)

				// Создаем сообщение
				msg := domain.Message{
					Symbol:    pair,
					Price:     newPrice,
					Timestamp: time.Now(),
					Exchange:  g.Name(),
				}

				// Отправляем сообщение в канал
				select {
				case <-ctx.Done():
					return
				case g.messageChan <- msg:
					// Сообщение успешно отправлено
				}
			}
		}
	}
}

// generateNewPrice - генерирует новую цену на основе предыдущей с учетом волатильности
func (g *TestDataGenerator) generateNewPrice(pair string) float64 {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	currentPrice := g.prices[pair]
	vol := volatility[pair]

	// Случайное процентное изменение в пределах волатильности
	changePercent := vol.min + rand.Float64()*(vol.max-vol.min)

	// Рассчитываем абсолютное изменение
	change := currentPrice * changePercent / 100.0

	// Новая цена
	newPrice := currentPrice + change

	// Гарантируем, что цена не станет отрицательной
	if newPrice <= 0 {
		newPrice = currentPrice * 0.95 // Уменьшаем цену на 5% если рассчитанное значение отрицательное
	}

	// Иногда генерируем резкие скачки цен для имитации реальности
	if rand.Float64() < 0.02 { // 2% шанс на резкое изменение
		// Скачок от -10% до +10%
		jump := -10.0 + rand.Float64()*20.0
		newPrice = newPrice * (1.0 + jump/100.0)
	}

	// Сохраняем новую цену
	g.prices[pair] = newPrice

	return newPrice
}
