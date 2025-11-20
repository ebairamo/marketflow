# MarketFlow

Система обработки рыночных данных в реальном времени для криптовалютных бирж. Проект демонстрирует конкурентную обработку потоковых данных, кеширование и REST API на Go.

## 🚀 Возможности

- **Обработка в реальном времени**: получение и обработка данных о ценах с нескольких источников
- **Два режима работы**:
  - **Live Mode**: получение реальных данных с криптобирж
  - **Test Mode**: генерация синтетических данных для тестирования
- **Конкурентная обработка**: использование паттернов Fan-in, Fan-out, Worker Pool
- **Кеширование в Redis**: быстрый доступ к последним ценам
- **Хранение в PostgreSQL**: агрегированные данные с минутными интервалами
- **REST API**: запросы цен, статистики и управление режимами
- **Graceful Shutdown**: корректное завершение работы с очисткой ресурсов

## 🛠️ Технологии

- **Язык**: Go
- **База данных**: PostgreSQL
- **Кеш**: Redis
- **Архитектура**: Hexagonal Architecture (Ports & Adapters)
- **Конкурентность**: Channels, Goroutines, Worker Pools
- **Логирование**: log/slog

## 📦 Установка

```bash
# Клонировать репозиторий
git clone https://github.com/ebairamo/marketflow.git
cd marketflow

# Собрать проект
go build -o marketflow .
```

### Зависимости

```bash
# PostgreSQL
docker run --name postgres -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres

# Redis
docker run --name redis -p 6379:6379 -d redis
```

## 🎯 Использование

### Запуск сервера

```bash
# Базовый запуск
./marketflow --port 8080

# Справка
./marketflow --help
```

### Конфигурация

Создайте файл `config.yaml`:

```yaml
postgres:
  host: localhost
  port: 5432
  user: postgres
  password: password
  database: marketflow

redis:
  host: localhost
  port: 6379
  password: ""

exchanges:
  - host: localhost
    port: 40101
  - host: localhost
    port: 40102
  - host: localhost
    port: 40103
```

## 🔄 Режимы работы

### Live Mode

Получение реальных данных с криптобирж через предоставленные эмуляторы:

```bash
# Переключить на Live Mode
curl -X POST http://localhost:8080/mode/live
```

**Поддерживаемые пары:**
- BTCUSDT
- ETHUSDT
- SOLUSDT
- DOGEUSDT
- TONUSDT

### Test Mode

Генерация синтетических данных для тестирования:

```bash
# Переключить на Test Mode
curl -X POST http://localhost:8080/mode/test
```

## 📊 API Endpoints

### Получение цен

**Последняя цена**
```bash
# Со всех бирж
curl http://localhost:8080/prices/latest/BTCUSDT

# С конкретной биржи
curl http://localhost:8080/prices/latest/exchange1/BTCUSDT
```

**Максимальная цена**
```bash
# За весь период
curl http://localhost:8080/prices/highest/BTCUSDT

# За последние 5 минут
curl http://localhost:8080/prices/highest/BTCUSDT?period=5m

# С конкретной биржи за 30 секунд
curl http://localhost:8080/prices/highest/exchange1/BTCUSDT?period=30s
```

**Минимальная цена**
```bash
# За весь период
curl http://localhost:8080/prices/lowest/ETHUSDT

# За последнюю минуту
curl http://localhost:8080/prices/lowest/ETHUSDT?period=1m
```

**Средняя цена**
```bash
# За весь период
curl http://localhost:8080/prices/average/SOLUSDT

# За последние 3 минуты с конкретной биржи
curl http://localhost:8080/prices/average/exchange2/SOLUSDT?period=3m
```

### Системные endpoints

**Проверка здоровья системы**
```bash
curl http://localhost:8080/health
```

**Переключение режима**
```bash
# Test Mode
curl -X POST http://localhost:8080/mode/test

# Live Mode
curl -X POST http://localhost:8080/mode/live
```

## 🏗️ Архитектура

### Hexagonal Architecture

```
┌─────────────────────────────────────────┐
│           HTTP Handlers (API)           │
│          (Web Adapter)                  │
└─────────────┬───────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│      Application Layer (Use Cases)      │
│  - Price Processing                     │
│  - Data Aggregation                     │
│  - Mode Switching                       │
└─────────────┬───────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│         Domain Layer (Models)           │
│  - Price, Exchange, Pair                │
└─────────────┬───────────────────────────┘
              │
    ┌─────────┴─────────┐
    │                   │
┌───▼──────┐      ┌─────▼────────┐
│PostgreSQL│      │    Redis     │
│ Adapter  │      │   Adapter    │
└──────────┘      └──────────────┘
```

### Паттерны конкурентности

**Fan-Out Pattern**
- Распределение данных от одного источника между несколькими воркерами

**Fan-In Pattern**
- Объединение данных от нескольких источников в один канал

**Worker Pool**
- Пул из 5 воркеров на каждую биржу для обработки данных

**Generator**
- Генератор синтетических данных для Test Mode

## 💾 Хранение данных

### Redis (Кеш)

Хранение последних цен за последнюю минуту для каждой пары с каждой биржи.

**Структура ключей:**
```
price:{exchange}:{pair}:{timestamp}
```

### PostgreSQL (База данных)

Агрегированные данные за каждую минуту.

**Таблица: market_data**
```sql
CREATE TABLE market_data (
    id SERIAL PRIMARY KEY,
    pair_name VARCHAR(20) NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    average_price DECIMAL(20, 8) NOT NULL,
    min_price DECIMAL(20, 8) NOT NULL,
    max_price DECIMAL(20, 8) NOT NULL
);
```

## 📝 Примеры использования

```bash
# Запуск в Live Mode
./marketflow --port 8080

# Проверка здоровья
curl http://localhost:8080/health

# Получить последнюю цену BTC
curl http://localhost:8080/prices/latest/BTCUSDT

# Получить максимальную цену ETH за последние 5 минут
curl http://localhost:8080/prices/highest/ETHUSDT?period=5m

# Переключиться на Test Mode
curl -X POST http://localhost:8080/mode/test

# Получить среднюю цену SOL с биржи exchange1
curl http://localhost:8080/prices/average/exchange1/SOLUSDT
```

## 🔧 Обработка ошибок

- **Автоматическое переподключение** при потере связи с биржей
- **Fallback механизм**: если Redis недоступен, данные сохраняются в PostgreSQL
- **Graceful degradation**: система продолжает работу при частичных сбоях
- **Батчинг записей**: группировка операций для оптимизации производительности

## 🎓 Цели обучения

Этот проект демонстрирует:
- Конкурентное программирование в Go
- Паттерны конкурентности (Fan-in, Fan-out, Worker Pool, Generator)
- Обработку потоковых данных в реальном времени
- Работу с Redis для кеширования
- Работу с PostgreSQL для хранения данных
- Hexagonal Architecture
- REST API дизайн
- Graceful Shutdown

## 📚 Ссылки

- [Go Concurrency Patterns](https://go.dev/blog/pipelines)
- [Redis Documentation](https://redis.io/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Hexagonal Architecture](https://alistair.cockburn.us/hexagonal-architecture/)

## 🙏 Автор задания

**Savva Savostyanov**
- Email: savvax@savvax.com
- [GitHub](https://github.com/savvax)
- [LinkedIn](https://www.linkedin.com/in/savvax/)

# MarketFlow

Real-time market data processing system for cryptocurrency exchanges. This project demonstrates concurrent stream processing, caching, and REST API design in Go.

## 🚀 Features

- **Real-time Processing**: receive and process price data from multiple sources
- **Dual Operation Modes**:
  - **Live Mode**: fetch real data from crypto exchanges
  - **Test Mode**: generate synthetic data for testing
- **Concurrent Processing**: implements Fan-in, Fan-out, Worker Pool patterns
- **Redis Caching**: fast access to latest prices
- **PostgreSQL Storage**: aggregated data with minute-level intervals
- **REST API**: query prices, statistics, and manage operation modes
- **Graceful Shutdown**: proper cleanup and resource management

## 🛠️ Tech Stack

- **Language**: Go
- **Database**: PostgreSQL
- **Cache**: Redis
- **Architecture**: Hexagonal Architecture (Ports & Adapters)
- **Concurrency**: Channels, Goroutines, Worker Pools
- **Logging**: log/slog

## 📦 Installation

```bash
# Clone repository
git clone https://github.com/ebairamo/marketflow.git
cd marketflow

# Build project
go build -o marketflow .
```

### Dependencies

```bash
# PostgreSQL
docker run --name postgres -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres

# Redis
docker run --name redis -p 6379:6379 -d redis
```

## 🎯 Usage

### Starting the Server

```bash
# Basic usage
./marketflow --port 8080

# Help
./marketflow --help
```

### Configuration

Create `config.yaml` file:

```yaml
postgres:
  host: localhost
  port: 5432
  user: postgres
  password: password
  database: marketflow

redis:
  host: localhost
  port: 6379
  password: ""

exchanges:
  - host: localhost
    port: 40101
  - host: localhost
    port: 40102
  - host: localhost
    port: 40103
```

## 🔄 Operation Modes

### Live Mode

Fetch real data from cryptocurrency exchanges via provided emulators:

```bash
# Switch to Live Mode
curl -X POST http://localhost:8080/mode/live
```

**Supported pairs:**
- BTCUSDT
- ETHUSDT
- SOLUSDT
- DOGEUSDT
- TONUSDT

### Test Mode

Generate synthetic data for testing:

```bash
# Switch to Test Mode
curl -X POST http://localhost:8080/mode/test
```

## 📊 API Endpoints

### Price Queries

**Latest Price**
```bash
# From all exchanges
curl http://localhost:8080/prices/latest/BTCUSDT

# From specific exchange
curl http://localhost:8080/prices/latest/exchange1/BTCUSDT
```

**Highest Price**
```bash
# All time
curl http://localhost:8080/prices/highest/BTCUSDT

# Last 5 minutes
curl http://localhost:8080/prices/highest/BTCUSDT?period=5m

# From specific exchange, last 30 seconds
curl http://localhost:8080/prices/highest/exchange1/BTCUSDT?period=30s
```

**Lowest Price**
```bash
# All time
curl http://localhost:8080/prices/lowest/ETHUSDT

# Last minute
curl http://localhost:8080/prices/lowest/ETHUSDT?period=1m
```

**Average Price**
```bash
# All time
curl http://localhost:8080/prices/average/SOLUSDT

# Last 3 minutes from specific exchange
curl http://localhost:8080/prices/average/exchange2/SOLUSDT?period=3m
```

### System Endpoints

**Health Check**
```bash
curl http://localhost:8080/health
```

**Mode Switching**
```bash
# Test Mode
curl -X POST http://localhost:8080/mode/test

# Live Mode
curl -X POST http://localhost:8080/mode/live
```

## 🏗️ Architecture

### Hexagonal Architecture

```
┌─────────────────────────────────────────┐
│           HTTP Handlers (API)           │
│          (Web Adapter)                  │
└─────────────┬───────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│      Application Layer (Use Cases)      │
│  - Price Processing                     │
│  - Data Aggregation                     │
│  - Mode Switching                       │
└─────────────┬───────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│         Domain Layer (Models)           │
│  - Price, Exchange, Pair                │
└─────────────┬───────────────────────────┘
              │
    ┌─────────┴─────────┐
    │                   │
┌───▼──────┐      ┌─────▼────────┐
│PostgreSQL│      │    Redis     │
│ Adapter  │      │   Adapter    │
└──────────┘      └──────────────┘
```

### Concurrency Patterns

**Fan-Out Pattern**
- Distribute data from one source to multiple workers

**Fan-In Pattern**
- Aggregate data from multiple sources into one channel

**Worker Pool**
- Pool of 5 workers per exchange for data processing

**Generator**
- Synthetic data generator for Test Mode

## 💾 Data Storage

### Redis (Cache)

Store latest prices for the last minute for each pair from each exchange.

**Key structure:**
```
price:{exchange}:{pair}:{timestamp}
```

### PostgreSQL (Database)

Aggregated data per minute.

**Table: market_data**
```sql
CREATE TABLE market_data (
    id SERIAL PRIMARY KEY,
    pair_name VARCHAR(20) NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    average_price DECIMAL(20, 8) NOT NULL,
    min_price DECIMAL(20, 8) NOT NULL,
    max_price DECIMAL(20, 8) NOT NULL
);
```

## 📝 Usage Examples

```bash
# Start in Live Mode
./marketflow --port 8080

# Health check
curl http://localhost:8080/health

# Get latest BTC price
curl http://localhost:8080/prices/latest/BTCUSDT

# Get highest ETH price for last 5 minutes
curl http://localhost:8080/prices/highest/ETHUSDT?period=5m

# Switch to Test Mode
curl -X POST http://localhost:8080/mode/test

# Get average SOL price from exchange1
curl http://localhost:8080/prices/average/exchange1/SOLUSDT
```

## 🔧 Error Handling

- **Automatic reconnection** when connection to exchange is lost
- **Fallback mechanism**: if Redis is unavailable, data is saved to PostgreSQL
- **Graceful degradation**: system continues operation during partial failures
- **Write batching**: group operations for performance optimization

## 🎓 Learning Objectives

This project demonstrates:
- Concurrent programming in Go
- Concurrency patterns (Fan-in, Fan-out, Worker Pool, Generator)
- Real-time stream data processing
- Redis caching
- PostgreSQL data storage
- Hexagonal Architecture
- REST API design
- Graceful Shutdown

## 📚 References

- [Go Concurrency Patterns](https://go.dev/blog/pipelines)
- [Redis Documentation](https://redis.io/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Hexagonal Architecture](https://alistair.cockburn.us/hexagonal-architecture/)

## 🙏 Project Author

**Savva Savostyanov**
- Email: savvax@savvax.com
- [GitHub](https://github.com/savvax)
- [LinkedIn](https://www.linkedin.com/in/savvax/)

---
