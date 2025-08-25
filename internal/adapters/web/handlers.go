package web

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"marketflow/internal/domain"
	"net/http"
	"strings"
	"time"
)

type Handler struct {
	cache          domain.CachePort
	storage        domain.StoragePort
	logger         *slog.Logger
	liveMode       bool            // флаг режима работы
	modeChan       chan bool       // канал для переключения режимов
	validExchanges map[string]bool // список валидных бирж
}

func NewHandler(cache domain.CachePort, storage domain.StoragePort, logger *slog.Logger) *Handler {
	return &Handler{
		cache:    cache,
		storage:  storage,
		logger:   logger,
		liveMode: true, // По умолчанию в Live Mode
		modeChan: make(chan bool, 1),
		validExchanges: map[string]bool{
			"Exchange1":     true,
			"Exchange2":     true,
			"Exchange3":     true,
			"TestExchange1": true,
			"TestExchange2": true,
			"TestExchange3": true,
		},
	}
}

func (h *Handler) GetModeChannel() <-chan bool {
	return h.modeChan
}

// Эндпоинт для получения последней цены
func (h *Handler) GetLatestPrice(w http.ResponseWriter, r *http.Request) {
	symbol := extractSymbolFromURL(r.URL.Path)
	exchange := extractExchangeFromURL(r.URL.Path)

	if symbol == "" {
		sendErrorResponse(w, "Symbol not specified", http.StatusBadRequest)
		return
	}

	// Валидация символа
	if !isValidSymbol(symbol) {
		sendErrorResponse(w, fmt.Sprintf("Invalid symbol: %s", symbol), http.StatusBadRequest)
		return
	}

	var price domain.Message
	var err error

	if exchange == "" {
		// Если биржа не указана, берем САМУЮ ПОСЛЕДНЮЮ цену со всех бирж
		prices, err := h.getAllLatestPrices(symbol)
		if err != nil {
			h.logger.Error("Ошибка получения последних цен", "error", err, "symbol", symbol)
			sendErrorResponse(w, "Failed to get latest price", http.StatusNotFound)
			return
		}

		if len(prices) == 0 {
			sendErrorResponse(w, fmt.Sprintf("No prices found for symbol %s", symbol), http.StatusNotFound)
			return
		}

		// Находим самую свежую цену по timestamp
		price = prices[0]
		for _, p := range prices {
			if p.Timestamp.After(price.Timestamp) {
				price = p
			}
		}
	} else {
		// Валидация биржи
		if !h.isValidExchange(exchange) {
			sendErrorResponse(w, fmt.Sprintf("Unknown exchange: %s", exchange), http.StatusBadRequest)
			return
		}

		// Если биржа указана, берем последнюю цену с конкретной биржи
		price, err = h.cache.GetCachedPriceByExchange(symbol, exchange)
		if err != nil {
			h.logger.Error("Ошибка получения последней цены", "error", err, "symbol", symbol, "exchange", exchange)
			sendErrorResponse(w, fmt.Sprintf("No price found for %s on %s", symbol, exchange), http.StatusNotFound)
			return
		}
	}

	// Преобразуем в JSON и отправляем ответ
	response := map[string]interface{}{
		"symbol":    price.Symbol,
		"price":     price.Price,
		"exchange":  price.Exchange,
		"timestamp": price.Timestamp,
	}

	sendJSONResponse(w, response)
}

// Вспомогательный метод для получения всех последних цен
func (h *Handler) getAllLatestPrices(symbol string) ([]domain.Message, error) {
	var prices []domain.Message

	// Получаем список всех активных бирж
	for exchange := range h.validExchanges {
		price, err := h.cache.GetCachedPriceByExchange(symbol, exchange)
		if err == nil {
			prices = append(prices, price)
		}
		// Игнорируем ошибки для отдельных бирж
	}

	return prices, nil
}

// Эндпоинт для получения максимальной цены
func (h *Handler) GetHighestPrice(w http.ResponseWriter, r *http.Request) {
	symbol := extractSymbolFromURL(r.URL.Path)
	exchange := extractExchangeFromURL(r.URL.Path)

	if symbol == "" {
		sendErrorResponse(w, "Symbol not specified", http.StatusBadRequest)
		return
	}

	// Валидация символа
	if !isValidSymbol(symbol) {
		sendErrorResponse(w, fmt.Sprintf("Invalid symbol: %s", symbol), http.StatusBadRequest)
		return
	}

	// Валидация биржи если указана
	if exchange != "" && !h.isValidExchange(exchange) {
		sendErrorResponse(w, fmt.Sprintf("Unknown exchange: %s", exchange), http.StatusBadRequest)
		return
	}

	// Получаем период из query параметров
	period := r.URL.Query().Get("period")
	duration, err := parsePeriodStrict(period)
	if err != nil {
		sendErrorResponse(w, fmt.Sprintf("Invalid period format: %s. Valid formats: 1s, 3s, 5s, 10s, 30s, 1m, 3m, 5m", period), http.StatusBadRequest)
		return
	}

	// Получаем все цены за указанный период
	prices, err := h.cache.GetPricesInRange(symbol, duration)
	if err != nil {
		h.logger.Error("Ошибка получения цен", "error", err, "symbol", symbol, "period", period)
		sendErrorResponse(w, "Failed to get price data", http.StatusInternalServerError)
		return
	}

	// Фильтруем по бирже, если она указана
	if exchange != "" {
		var filteredPrices []domain.Message
		for _, p := range prices {
			if p.Exchange == exchange {
				filteredPrices = append(filteredPrices, p)
			}
		}
		prices = filteredPrices
	}

	if len(prices) == 0 {
		sendErrorResponse(w, fmt.Sprintf("No data available for the specified period"), http.StatusNotFound)
		return
	}

	// Находим максимальную цену
	maxPrice := prices[0]
	for _, p := range prices {
		if p.Price > maxPrice.Price {
			maxPrice = p
		}
	}

	response := map[string]interface{}{
		"symbol":    maxPrice.Symbol,
		"price":     maxPrice.Price,
		"exchange":  maxPrice.Exchange,
		"timestamp": maxPrice.Timestamp,
		"period":    duration.String(),
	}

	sendJSONResponse(w, response)
}

// Эндпоинт для получения минимальной цены
func (h *Handler) GetLowestPrice(w http.ResponseWriter, r *http.Request) {
	symbol := extractSymbolFromURL(r.URL.Path)
	exchange := extractExchangeFromURL(r.URL.Path)

	if symbol == "" {
		sendErrorResponse(w, "Symbol not specified", http.StatusBadRequest)
		return
	}

	// Валидация символа
	if !isValidSymbol(symbol) {
		sendErrorResponse(w, fmt.Sprintf("Invalid symbol: %s", symbol), http.StatusBadRequest)
		return
	}

	// Валидация биржи если указана
	if exchange != "" && !h.isValidExchange(exchange) {
		sendErrorResponse(w, fmt.Sprintf("Unknown exchange: %s", exchange), http.StatusBadRequest)
		return
	}

	// Получаем период из query параметров
	period := r.URL.Query().Get("period")
	duration, err := parsePeriodStrict(period)
	if err != nil {
		sendErrorResponse(w, fmt.Sprintf("Invalid period format: %s. Valid formats: 1s, 3s, 5s, 10s, 30s, 1m, 3m, 5m", period), http.StatusBadRequest)
		return
	}

	// Получаем все цены за указанный период
	prices, err := h.cache.GetPricesInRange(symbol, duration)
	if err != nil {
		h.logger.Error("Ошибка получения цен", "error", err, "symbol", symbol, "period", period)
		sendErrorResponse(w, "Failed to get price data", http.StatusInternalServerError)
		return
	}

	// Фильтруем по бирже, если она указана
	if exchange != "" {
		var filteredPrices []domain.Message
		for _, p := range prices {
			if p.Exchange == exchange {
				filteredPrices = append(filteredPrices, p)
			}
		}
		prices = filteredPrices
	}

	if len(prices) == 0 {
		sendErrorResponse(w, fmt.Sprintf("No data available for the specified period"), http.StatusNotFound)
		return
	}

	// Находим минимальную цену
	minPrice := prices[0]
	for _, p := range prices {
		if p.Price < minPrice.Price {
			minPrice = p
		}
	}

	response := map[string]interface{}{
		"symbol":    minPrice.Symbol,
		"price":     minPrice.Price,
		"exchange":  minPrice.Exchange,
		"timestamp": minPrice.Timestamp,
		"period":    duration.String(),
	}

	sendJSONResponse(w, response)
}

// Эндпоинт для получения средней цены
func (h *Handler) GetAveragePrice(w http.ResponseWriter, r *http.Request) {
	symbol := extractSymbolFromURL(r.URL.Path)
	exchange := extractExchangeFromURL(r.URL.Path)

	if symbol == "" {
		sendErrorResponse(w, "Symbol not specified", http.StatusBadRequest)
		return
	}

	// Валидация символа
	if !isValidSymbol(symbol) {
		sendErrorResponse(w, fmt.Sprintf("Invalid symbol: %s", symbol), http.StatusBadRequest)
		return
	}

	// Валидация биржи если указана
	if exchange != "" && !h.isValidExchange(exchange) {
		sendErrorResponse(w, fmt.Sprintf("Unknown exchange: %s", exchange), http.StatusBadRequest)
		return
	}

	// Получаем период из query параметров
	period := r.URL.Query().Get("period")
	duration, err := parsePeriodStrict(period)
	if err != nil {
		sendErrorResponse(w, fmt.Sprintf("Invalid period format: %s. Valid formats: 1s, 3s, 5s, 10s, 30s, 1m, 3m, 5m", period), http.StatusBadRequest)
		return
	}

	// Получаем все цены за указанный период
	prices, err := h.cache.GetPricesInRange(symbol, duration)
	if err != nil {
		h.logger.Error("Ошибка получения цен", "error", err, "symbol", symbol, "period", period)
		sendErrorResponse(w, "Failed to get price data", http.StatusInternalServerError)
		return
	}

	// Фильтруем по бирже, если она указана
	if exchange != "" {
		var filteredPrices []domain.Message
		for _, p := range prices {
			if p.Exchange == exchange {
				filteredPrices = append(filteredPrices, p)
			}
		}
		prices = filteredPrices
	}

	if len(prices) == 0 {
		sendErrorResponse(w, fmt.Sprintf("No data available for the specified period"), http.StatusNotFound)
		return
	}

	// Вычисляем среднюю цену
	var sum float64
	for _, p := range prices {
		sum += p.Price
	}
	avgPrice := sum / float64(len(prices))

	response := map[string]interface{}{
		"symbol":        symbol,
		"average_price": avgPrice,
		"exchange":      exchange,
		"count":         len(prices),
		"period":        duration.String(),
		"timestamp":     time.Now(),
	}

	sendJSONResponse(w, response)
}

// Эндпоинт для переключения в Test Mode
func (h *Handler) SwitchToTestMode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Проверяем текущий режим
	if !h.liveMode {
		response := map[string]interface{}{
			"mode":    "test",
			"message": "Already in test mode",
		}
		sendJSONResponse(w, response)
		return
	}

	h.liveMode = false
	h.modeChan <- false
	h.logger.Info("🔄 Переключение в тестовый режим")

	response := map[string]interface{}{
		"mode":    "test",
		"message": "Switched to test mode",
	}
	sendJSONResponse(w, response)
}

// Эндпоинт для переключения в Live Mode
func (h *Handler) SwitchToLiveMode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Проверяем текущий режим
	if h.liveMode {
		response := map[string]interface{}{
			"mode":    "live",
			"message": "Already in live mode",
		}
		sendJSONResponse(w, response)
		return
	}

	h.liveMode = true
	h.modeChan <- true
	h.logger.Info("🔄 Переключение в live режим")

	response := map[string]interface{}{
		"mode":    "live",
		"message": "Switched to live mode",
	}
	sendJSONResponse(w, response)
}

// Эндпоинт для проверки состояния системы
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":   "ok",
		"mode":     map[bool]string{true: "live", false: "test"}[h.liveMode],
		"time":     time.Now(),
		"redis":    h.checkRedisHealth(),
		"postgres": h.checkPostgresHealth(),
	}
	sendJSONResponse(w, response)
}

// Проверка состояния Redis
func (h *Handler) checkRedisHealth() string {
	// Пробуем получить любое значение
	_, err := h.cache.GetCachedPrice("BTCUSDT")
	if err != nil && strings.Contains(err.Error(), "no cached price") {
		return "connected"
	} else if err != nil {
		return "disconnected"
	}
	return "connected"
}

// Проверка состояния PostgreSQL
func (h *Handler) checkPostgresHealth() string {
	// Пробуем получить любые данные
	_, err := h.storage.GetLatestPrice("BTCUSDT")
	if err != nil && strings.Contains(err.Error(), "no price found") {
		return "connected"
	} else if err != nil {
		return "disconnected"
	}
	return "connected"
}

// Setup настраивает маршрутизацию и возвращает HTTP-сервер
func (h *Handler) Setup(port int) *http.Server {
	mux := http.NewServeMux()

	// Market Data API
	mux.HandleFunc("/prices/latest/", h.GetLatestPrice)
	mux.HandleFunc("/prices/highest/", h.GetHighestPrice)
	mux.HandleFunc("/prices/lowest/", h.GetLowestPrice)
	mux.HandleFunc("/prices/average/", h.GetAveragePrice)

	// Data Mode API
	mux.HandleFunc("/mode/test", h.SwitchToTestMode)
	mux.HandleFunc("/mode/live", h.SwitchToLiveMode)

	// System Health
	mux.HandleFunc("/health", h.HealthCheck)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return server
}

// Метод для обновления списка валидных бирж (вызывается при переключении режимов)
func (h *Handler) UpdateValidExchanges(exchanges []string) {
	h.validExchanges = make(map[string]bool)
	for _, ex := range exchanges {
		h.validExchanges[ex] = true
	}
}

// Проверка валидности биржи
func (h *Handler) isValidExchange(exchange string) bool {
	return h.validExchanges[exchange]
}

// Вспомогательные функции

func sendJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func sendErrorResponse(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	response := map[string]string{
		"error": message,
	}
	json.NewEncoder(w).Encode(response)
}

func extractSymbolFromURL(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) >= 3 {
		symbol := parts[len(parts)-1]
		// Очищаем от query параметров
		if idx := strings.Index(symbol, "?"); idx != -1 {
			symbol = symbol[:idx]
		}
		return symbol
	}
	return ""
}

func extractExchangeFromURL(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) >= 4 && parts[len(parts)-2] != "latest" &&
		parts[len(parts)-2] != "highest" && parts[len(parts)-2] != "lowest" &&
		parts[len(parts)-2] != "average" {
		exchange := parts[len(parts)-2]
		// Очищаем от query параметров
		if idx := strings.Index(exchange, "?"); idx != -1 {
			exchange = exchange[:idx]
		}
		return exchange
	}
	return ""
}

// Валидация символа
func isValidSymbol(symbol string) bool {
	validSymbols := map[string]bool{
		"BTCUSDT":  true,
		"ETHUSDT":  true,
		"SOLUSDT":  true,
		"TONUSDT":  true,
		"DOGEUSDT": true,
	}
	return validSymbols[symbol]
}

// Строгий парсинг периода с валидацией
func parsePeriodStrict(period string) (time.Duration, error) {
	if period == "" {
		// По умолчанию 1 минута
		return time.Minute, nil
	}

	// Список допустимых периодов
	validPeriods := map[string]time.Duration{
		"1s":  1 * time.Second,
		"3s":  3 * time.Second,
		"5s":  5 * time.Second,
		"10s": 10 * time.Second,
		"30s": 30 * time.Second,
		"1m":  1 * time.Minute,
		"3m":  3 * time.Minute,
		"5m":  5 * time.Minute,
	}

	if duration, ok := validPeriods[period]; ok {
		return duration, nil
	}

	return 0, fmt.Errorf("invalid period format")
}

// Старая функция для обратной совместимости
func parsePeriod(period string) time.Duration {
	duration, _ := parsePeriodStrict(period)
	if duration == 0 {
		return time.Minute
	}
	return duration
}
