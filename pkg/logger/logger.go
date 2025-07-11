package logger

import (
	"log/slog"
	"os"
)

// Настройка логгера в pkg/logger/logger.go
func SetupLogger() *slog.Logger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler)
}
