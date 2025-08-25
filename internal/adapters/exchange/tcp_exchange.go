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
	"sync"
	"time"
)

type TCPExchange struct {
	address   string
	exchange  string
	conn      net.Conn
	logger    *slog.Logger
	mu        sync.RWMutex // Защита conn от конкурентного доступа
	connected bool         // Флаг состояния подключения
}

func parseString(s string) (domain.Message, error) {
	m := domain.Message{}
	cleaned := strings.Trim(s, "{}")
	pairs := strings.Split(cleaned, ",")
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.Trim(parts[0], `"`)
		value := strings.Trim(parts[1], `"`)
		switch key {
		case "symbol":
			m.Symbol = value
		case "price":
			f, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return m, fmt.Errorf("invalid price: %s", value)
			}
			m.Price = f
		case "timestamp":
			clean := strings.Trim(value, "\"}\n")
			ms, err := strconv.ParseInt(clean, 10, 64)
			if err != nil {
				return m, fmt.Errorf("invalid timestamp: %s", clean)
			}
			m.Timestamp = time.UnixMilli(ms)
		}
	}
	return m, nil
}

func NewTCPExchange(address, exchange string, logger *slog.Logger) *TCPExchange {
	return &TCPExchange{
		address:   address,
		exchange:  exchange,
		logger:    logger,
		connected: false,
	}
}

func (e *TCPExchange) Connect() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Если уже подключены, закрываем старое соединение
	if e.conn != nil {
		e.conn.Close()
		e.conn = nil
		e.connected = false
	}

	e.logger.Info("Connecting to exchange",
		"exchange", e.exchange,
		"address", e.address)

	// Устанавливаем таймаут на подключение
	dialer := net.Dialer{
		Timeout: 5 * time.Second,
	}

	conn, err := dialer.Dial("tcp", e.address)
	if err != nil {
		e.logger.Error("Failed to connect to exchange",
			"exchange", e.exchange,
			"address", e.address,
			"error", err)
		return fmt.Errorf("failed to connect to %s at %s: %w", e.exchange, e.address, err)
	}

	e.conn = conn
	e.connected = true

	e.logger.Info("Successfully connected to exchange",
		"exchange", e.exchange,
		"address", e.address)

	return nil
}

func (e *TCPExchange) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.conn != nil {
		err := e.conn.Close()
		e.conn = nil
		e.connected = false
		e.logger.Info("Connection closed", "exchange", e.exchange)
		return err
	}

	e.connected = false
	return nil
}

func (e *TCPExchange) IsConnected() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.connected
}

func (e *TCPExchange) Name() string {
	return e.exchange
}

func (e *TCPExchange) ReadPriceUpdates(ctx context.Context) (<-chan domain.Message, <-chan error) {
	messageCh := make(chan domain.Message, 100)
	errCh := make(chan error, 10)

	go func() {
		defer func() {
			close(messageCh)
			close(errCh)
			e.logger.Info("ReadPriceUpdates goroutine finished", "exchange", e.exchange)
		}()

		for {
			select {
			case <-ctx.Done():
				e.logger.Info("Context cancelled, stopping price updates reader", "exchange", e.exchange)
				return
			default:
			}

			// Получаем текущее соединение
			e.mu.RLock()
			conn := e.conn
			isConnected := e.connected
			e.mu.RUnlock()

			if !isConnected || conn == nil {
				// Соединение не установлено
				select {
				case errCh <- fmt.Errorf("not connected to %s", e.exchange):
				case <-ctx.Done():
					return
				default:
				}

				// Ждём немного перед следующей проверкой
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return
				}
				continue
			}

			// Устанавливаем таймаут чтения
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))

			// Создаём reader для текущего соединения
			reader := bufio.NewReader(conn)

			// Читаем данные
			dataString, err := reader.ReadString('\n')
			if err != nil {
				// Проверяем, не был ли это таймаут
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// Таймаут - это нормально, продолжаем
					continue
				}

				// Другая ошибка - отправляем и помечаем соединение как разорванное
				e.logger.Error("Read error",
					"exchange", e.exchange,
					"error", err)

				select {
				case errCh <- fmt.Errorf("read error from %s: %w", e.exchange, err):
				case <-ctx.Done():
					return
				default:
				}

				// Помечаем соединение как разорванное
				e.mu.Lock()
				e.connected = false
				if e.conn != nil {
					e.conn.Close()
					e.conn = nil
				}
				e.mu.Unlock()

				// Ждём перед следующей попыткой
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return
				}
				continue
			}

			// Парсим сообщение
			message, err := parseString(dataString)
			if err != nil {
				e.logger.Error("Parse error",
					"exchange", e.exchange,
					"error", err,
					"data", dataString)

				select {
				case errCh <- fmt.Errorf("parse error from %s: %w", e.exchange, err):
				case <-ctx.Done():
					return
				default:
				}
				continue
			}

			// Добавляем имя биржи
			message.Exchange = e.exchange

			// Отправляем сообщение
			select {
			case messageCh <- message:
			case <-ctx.Done():
				return
			}
		}
	}()

	return messageCh, errCh
}
