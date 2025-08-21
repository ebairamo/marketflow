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
	exchange string
	conn     net.Conn
	logger   *slog.Logger
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

func (e *TCPExchange) Close() error {
	if e.conn != nil {
		e.conn.Close()
		e.logger.Info("Соединение закрыто", "exchange", e.Name())
		e.conn = nil
	}
	return nil
}

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
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
				e.logger.Error("Ошибка соединения", "error", err)
			}

			if err := e.Connect(); err != nil {
				select {
				case <-ctx.Done():
					return
				case errCh <- fmt.Errorf("не удалось подключиться: %w", err):
				}
				return
			}
		}

		reader := bufio.NewReader(e.conn)
		e.logger.Info("Подключено к бирже, начинаем чтение данных", "exchange", e.Name())

		for {
			select {
			case <-ctx.Done():
				e.logger.Info("Остановка чтения данных по запросу контекста", "exchange", e.Name())
				return
			default:
				dataString, err := reader.ReadString('\n')
				if err != nil {
					select {
					case <-ctx.Done():
						return
					case errCh <- err:
						e.logger.Error("Ошибка чтения", "error", err, "exchange", e.Name())
					}

					e.conn.Close()
					e.conn = nil

					// После e.conn.Close() и e.conn = nil
					// ПЕРЕД попыткой переподключения добавьте:
					select {
					case <-ctx.Done():
						return
					default:
						// Продолжаем с переподключением

						if err := e.Connect(); err != nil {
							select {
							case <-ctx.Done():
								return
							case errCh <- fmt.Errorf("не удалось переподключиться: %w", err):
							}
							return
						}

						if e.conn == nil {
							return
						}

						reader = bufio.NewReader(e.conn)
						continue
					}
				}

				message, err := parseString(dataString)
				if err != nil {
					select {
					case <-ctx.Done():
						return
					case errCh <- fmt.Errorf("ошибка парсинга: %w, строка: %s", err, dataString):
						e.logger.Error("Ошибка парсинга", "error", err, "data", dataString)
					}
					continue
				}

				message.Exchange = e.Name()
				select {
				case <-ctx.Done():
					return
				case messageCh <- message:
				}
			}
		}
	}()

	return messageCh, errCh
}

func (e *TCPExchange) IsConnected() bool {
	return e.conn != nil
}

func (e *TCPExchange) Name() string {
	return e.exchange
}
