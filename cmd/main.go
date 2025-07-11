package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

type massage struct {
	Symbol    string
	Price     float64
	Timestamp time.Time
}

func parseString(s string) {
	fmt.Printf("Cтрока %s\n", s)
	words := strings.Split(s, ",")
	symbol := strings.Fields(words[0])
	fmt.Println(symbol[0])
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:40101")
	if err != nil {
		fmt.Println("Ошибка подключения:", err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	dateString, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Ошибка dateString: %v\n", err)
		return
	}
	parseString(dateString)
}
