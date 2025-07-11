package exchange

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"marketflow/internal/domain"
	"net"
	"strconv"
	"strings"
	"time"
)

type TCPExchange struct {
	address  string
	exchange string // Идентификатор биржи
	conn     net.Conn
	logger   *slog.Logger
}

func parseString(s string) (domain.Message, error) {
	m := domain.Message{}
	cleaned := strings.Trim(s, "{}")
	pairs := strings.Split(cleaned, ",")
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)

		key := strings.Trim(parts[0], `"`)
		value := strings.Trim(parts[1], `"`)
		switch key {
		case "symbol":
			m.Symbol = value

		case "price":
			f, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return m, fmt.Errorf("invalid pair: %s", pair)
			}
			m.Price = f
		case "timestamp":
			clean := strings.Trim(value, "\"}\n")
			ms, err := strconv.ParseInt(clean, 10, 64)
			if err != nil {
				return m, fmt.Errorf("invalid pair: %s", pair)
			}
			t := time.UnixMilli(ms)
			m.Timestamp = t
		}
	}
	return m, nil
}

func NewTCPExchange(address, exchange string, logger *slog.Logger) *TCPExchange {
	return &TCPExchange{
		address:  address,
		exchange: exchange,
		logger:   logger,
	}
}

func (e *TCPExchange) Connect() error {
	e.logger.Info("Connecting to exchange",
		"exchange", e.Name(),
		"address", e.address)

	conn, err := net.Dial("tcp", e.address)
	if err != nil {
		e.logger.Error("Failed to connect to exchange",
			"exchange", e.Name(),
			"address", e.address,
			"error", err)
		return fmt.Errorf("failed to connect to exchange %s: %w", e.Name(), err)
	}

	e.conn = conn
	e.logger.Info("Successfully connected to exchange",
		"exchange", e.Name(),
		"address", e.address)
	return nil
}

// Close закрывает соединение с биржей
func (e *TCPExchange) Close() error {
	if e.conn != nil {
		e.conn.Close()
		e.logger.Info("Соединение закрыто")
		e.conn = nil
	} else {
		return nil
	}
	return nil
}

// ReadPriceUpdates читает обновления цен с биржи
// Возвращает канал с сообщениями и канал с ошибками
func (e *TCPExchange) ReadPriceUpdates(ctx context.Context) (<-chan domain.Message, <-chan error) {
	messageCh := make(chan domain.Message, 100)
	errCh := make(chan error, 10)
	go func() {
		defer func() {
			close(messageCh)
			close(errCh)
		}()
		if e.conn == nil {
			err := fmt.Errorf("соединение не установлено для биржи %s", e.Name())

			errCh <- err
			e.logger.Error("Ошибка соединения", "error", err)

			if err := e.Connect(); err != nil {
				errCh <- fmt.Errorf("не удалось подключиться: %w", err)
				// Закрываем каналы и завершаем горутину, так как без соединения нет смысла продолжать
				close(messageCh)
				close(errCh)
				return
			}
		}
		reader := bufio.NewReader(e.conn)
		e.logger.Info("Подключено к бирже, начинаем чтение данных")
		for {
			select {
			case <-ctx.Done():
				e.logger.Info("Остановка чтения данных по запросу контекста")
				return
			default:

				dataString, err := reader.ReadString('\n')
				if err != nil {
					// Отправить ошибку в канал
					errCh <- err

					// Логировать ошибку
					e.logger.Error("Ошибка чтения", "error", err)

					// Закрыть текущее соединение
					e.conn.Close()
					e.conn = nil

					// Попытка переподключения
					if err := e.Connect(); err != nil {
						// Если не удалось переподключиться
						errCh <- fmt.Errorf("не удалось переподключиться: %w", err)
						continue
					}

					// Обновить reader после переподключения
					reader = bufio.NewReader(e.conn)
					continue
				}

				message, err := parseString(dataString)
				if err != nil {
					errCh <- fmt.Errorf("ошибка парснга: %w, строка: %s", err, dataString)
					e.logger.Error("Ошибка парсинга",
						"error", err,
						"data", dataString)
					continue
				}

				message.Exchange = e.Name() // Добавляем имя биржи к сообщению
				messageCh <- message
			}
		}
	}()
	return messageCh, errCh
}

// IsConnected проверяет, установлено ли соединение
func (e *TCPExchange) IsConnected() bool {
	if e.conn != nil {
		return true
	} else {
		return false
	}
}

// Name возвращает имя биржи
func (e *TCPExchange) Name() string {

	return e.exchange
}
