package exchange

import (
	"fmt"
	"marketflow/internal/domain"
	"net"
)

type TCPExchange struct {
	address  string
	exchange string // Идентификатор биржи
	conn     net.Conn
}

func NewTCPExchange(address, exchange string) *TCPExchange {
	return &TCPExchange{
		address:  address,
		exchange: exchange,
	}
}

// Connect устанавливает соединение с биржей
func (e *TCPExchange) Connect() error {
	conn, err := net.Dial("tcp", e.address)
	if err != nil {
		fmt.Println("Ошибка подключения:", err)
		return err
	}
	e.conn = conn
	return nil
}

// Close закрывает соединение с биржей
func (e *TCPExchange) Close() error {
	// TODO: Реализовать закрытие соединения
	return nil
}

// ReadPriceUpdates читает обновления цен с биржи
// Возвращает канал с сообщениями и канал с ошибками
func (e *TCPExchange) ReadPriceUpdates() (<-chan domain.Message, <-chan error) {
	// TODO: Реализовать чтение данных и отправку в канал
	return nil, nil
}

// IsConnected проверяет, установлено ли соединение
func (e *TCPExchange) IsConnected() bool {
	// TODO: Реализовать проверку соединения
	return false
}

// Name возвращает имя биржи
func (e *TCPExchange) Name() string {
	// TODO: Реализовать возврат имени биржи
	return ""
}
