package domain

import "time"

type Message struct {
	Symbol    string
	Price     float64
	Timestamp time.Time
	Exchange  string
}

// Новая структура для агрегированных данных
type AggregatedData struct {
	ID           int
	PairName     string
	Exchange     string
	Timestamp    time.Time
	AveragePrice float64
	MinPrice     float64
	MaxPrice     float64
	CreatedAt    time.Time
}
