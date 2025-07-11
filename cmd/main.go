package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type message struct {
	Symbol    string
	Price     float64
	Timestamp time.Time
}

func parseString(s string) (message, error) {
	m := message{}
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
	conn, err := net.Dial("tcp", "127.0.0.1:40101")
	if err != nil {
		fmt.Println("Ошибка подключения:", err)
		return
	}
	defer conn.Close()
	for {
		reader := bufio.NewReader(conn)
		dateString, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Ошибка dateString: %v\n", err)
			return
		}

		message, _ := parseString(dateString)
		fmt.Println(message)
	}
}
