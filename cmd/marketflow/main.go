package main

import (
	"context"
	"marketflow/internal/adapters/exchange"
	"marketflow/internal/application/services"
	"marketflow/internal/domain"
	"marketflow/pkg/logger"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log := logger.SetupLogger()
	log.Info("Начинаем работу криптобиржи...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exchange1 := exchange.NewTCPExchange("127.0.0.1:40101", "Exchange1", log)

	marketService := services.NewMarketService([]domain.ExchangePort{exchange1}, log)

	messageCh, errCh := marketService.Start(ctx)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case msg := <-messageCh:
			log.Info("Получено сообщение",
				"exchange", msg.Exchange,
				"symbol", msg.Symbol,
				"price", msg.Price)

		case err := <-errCh:
			log.Error("Получена ошибка", "error", err)

		case <-signalCh:
			log.Info("Получен сигнал завершения, останавливаем приложение...")
			cancel() // Отменяем контекст

			time.Sleep(500 * time.Millisecond)
			return
		}
	}
}
