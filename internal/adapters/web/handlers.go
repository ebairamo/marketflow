package web

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"marketflow/internal/domain"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Handler struct {
	cache    domain.CachePort
	storage  domain.StoragePort
	logger   *slog.Logger
	liveMode bool      // флаг режима работы
	modeChan chan bool // канал для переключения режимов
}

func NewHandler(cache domain.CachePort, storage domain.StoragePort, logger *slog.Logger) *Handler {
	return &Handler{
		cache:    cache,
		storage:  storage,
		logger:   logger,
		liveMode: true, // По умолчанию в Live Mode
		modeChan: make(chan bool, 1),
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
		http.Error(w, "Не указан символ", http.StatusBadRequest)
		return
	}

	var price domain.Message
	var err error

	if exchange == "" {
		// Если биржа не указана, берем последнюю цену с любой биржи
		price, err = h.cache.GetCachedPrice(symbol)
	} else {
		// Если биржа указана, берем последнюю цену с конкретной биржи
		price, err = h.cache.GetCachedPriceByExchange(symbol, exchange)
	}

	if err != nil {
		h.logger.Error("Ошибка получения последней цены", "error", err, "symbol", symbol, "exchange", exchange)
		http.Error(w, "Не удалось получить данные", http.StatusNotFound)
		return
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

// Эндпоинт для получения максимальной цены
func (h *Handler) GetHighestPrice(w http.ResponseWriter, r *http.Request) {
	symbol := extractSymbolFromURL(r.URL.Path)
	exchange := extractExchangeFromURL(r.URL.Path)

	if symbol == "" {
		http.Error(w, "Не указан символ", http.StatusBadRequest)
		return
	}

	// Получаем период из query параметров
	period := r.URL.Query().Get("period")
	duration := parsePeriod(period)

	// Получаем все цены за указанный период
	prices, err := h.cache.GetPricesInRange(symbol, duration)
	if err != nil {
		h.logger.Error("Ошибка получения цен", "error", err, "symbol", symbol, "period", period)
		http.Error(w, "Не удалось получить данные", http.StatusInternalServerError)
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
		http.Error(w, "Нет данных за указанный период", http.StatusNotFound)
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
		http.Error(w, "Не указан символ", http.StatusBadRequest)
		return
	}

	// Получаем период из query параметров
	period := r.URL.Query().Get("period")
	duration := parsePeriod(period)

	// Получаем все цены за указанный период
	prices, err := h.cache.GetPricesInRange(symbol, duration)
	if err != nil {
		h.logger.Error("Ошибка получения цен", "error", err, "symbol", symbol, "period", period)
		http.Error(w, "Не удалось получить данные", http.StatusInternalServerError)
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
		http.Error(w, "Нет данных за указанный период", http.StatusNotFound)
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
		http.Error(w, "Не указан символ", http.StatusBadRequest)
		return
	}

	// Получаем период из query параметров
	period := r.URL.Query().Get("period")
	duration := parsePeriod(period)

	// Получаем все цены за указанный период
	prices, err := h.cache.GetPricesInRange(symbol, duration)
	if err != nil {
		h.logger.Error("Ошибка получения цен", "error", err, "symbol", symbol, "period", period)
		http.Error(w, "Не удалось получить данные", http.StatusInternalServerError)
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
		http.Error(w, "Нет данных за указанный период", http.StatusNotFound)
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
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	h.liveMode = false
	h.modeChan <- false
	h.logger.Info("🔄 Переключение в тестовый режим")

	response := map[string]interface{}{
		"mode":    "test",
		"message": "Переключено в тестовый режим",
	}
	sendJSONResponse(w, response)
}

// Эндпоинт для переключения в Live Mode
func (h *Handler) SwitchToLiveMode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	h.liveMode = true
	h.modeChan <- true
	h.logger.Info("🔄 Переключение в live режим")

	response := map[string]interface{}{
		"mode":    "live",
		"message": "Переключено в live режим",
	}
	sendJSONResponse(w, response)
}

// Эндпоинт для проверки состояния системы
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status": "ok",
		"mode":   map[bool]string{true: "live", false: "test"}[h.liveMode],
		"time":   time.Now(),
	}
	sendJSONResponse(w, response)
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

// Вспомогательные функции

func sendJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func extractSymbolFromURL(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) >= 3 {
		return parts[len(parts)-1]
	}
	return ""
}

func extractExchangeFromURL(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) >= 4 && parts[len(parts)-2] != "latest" &&
		parts[len(parts)-2] != "highest" && parts[len(parts)-2] != "lowest" &&
		parts[len(parts)-2] != "average" {
		return parts[len(parts)-2]
	}
	return ""
}

func parsePeriod(period string) time.Duration {
	if period == "" {
		// По умолчанию 1 минута
		return time.Minute
	}

	// Парсим период (например, 1m, 5s, 30s и т.д.)
	value := period[:len(period)-1]
	unit := period[len(period)-1:]

	val, err := strconv.Atoi(value)
	if err != nil {
		return time.Minute
	}

	switch unit {
	case "s":
		return time.Duration(val) * time.Second
	case "m":
		return time.Duration(val) * time.Minute
	default:
		return time.Minute
	}
}
