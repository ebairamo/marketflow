package domain

import "time"

type Message struct {
	Symbol    string
	Price     float64
	Timestamp time.Time
}
