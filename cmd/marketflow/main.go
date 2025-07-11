package main

import (
	"bufio"
	"fmt"
	"marketflow/internal/domain"
	"net"
	"strconv"
	"strings"
	"time"
)

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

func main() {
	for {

		conn, err := net.Dial("tcp", "127.0.0.1:40101")
		if err != nil {
			fmt.Println("Ошибка подключения:", err)
			time.Sleep(3 * time.Second)
			continue
		}
		reader := bufio.NewReader(conn)
		fmt.Println("Подключено к бирже, начинаем чтение данных")
		for {

			dataString, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Ошибка dateString: %v\n", err)
				break
			}

			message, err := parseString(dataString)
			if err != nil {
				fmt.Printf("Ошибка парсинга: %v, строка: %s\n", err, dataString)
				continue
			}
			fmt.Println(message)
		}
	}
}
