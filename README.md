# ML-Service-Inference

Пайплайн инференса ML-моделей на языке Go. Позволяет обслуживать ONNX-модели через gRPC и REST API с поддержкой динамического батчевания, параллельной обработки и полной наблюдаемости системы.

## Архитектура

```
                      ┌───────────────────┐
                      │  Prometheus:9090  │ 
                      │   /metrics        │
                      └──────▲────────────┘
                             │
 ┌──────────┐  ┌──────────┐  │     ┌──────────────────────────────────────────────┐
 │  gRPC    │  │  REST    │  │     │              Пайплайн                        │
 │  :50051  │  │  :8080   │  │     │                                              │
 │          │  │          │  │     │  ┌─────────┐  ┌─────────┐  ┌────────────┐    │
 │  Infer   ├──▶          ├──┼─────▶  │ Бэтчер  │  ├─▶│  Пул    ├─▶│ ONNX    │    │
 │  Stream  │  │ /v1/infer│  │     │   (5-10мс) │  │ воркеров│  │ Runtime    │    │
 └──────────┘  └──────────┘  │     │   ┌────────┘  │ (N=4)   │  │            │    │
                             │     │   │           └────┬────┘  └────────────┘    │
 ┌──────────┐  ┌──────────┐  │     │   │  ┌─────────────┼──────────────┐          │
 │  Kafka   ├──▶  Воркер  ├──┼────▶│   │  │Препроцесс → Infer → Пост  │           │
 │  Topic   │  │  Binary  │  │     │   │  └────────────────────────────┘          │
 └──────────┘  └────┬─────┘  │     └── ┴──────────────────────────────────────────┘
                    │        │
                    ▼        │
              ┌──────────┐   │
              │  Redis   │   │
              │  Кэш     │   │
              └──────────┘   │
                             │
                    ┌────────┴────────┐
                    │  Jaeger / OTLP  │
                    │   Трассировка   │
                    └─────────────────┘
```

## TL;DR

- **ONNX Runtime** — запуск любых моделей PyTorch, экспортированных в формат `.onnx`.
- **Динамическое батчевание** — накапливает запросы в течение N мс или до заполнения лимита, затем выполняет один групповой инференс.
- **Пул воркеров** — ограничение количества горутин с механизмом backpressure через каналы.
- **Двойной API** — поддержка gRPC (unary + bidirectional streaming) и REST (фреймворк Gin).
- **Kafka Consumer** — асинхронная потоковая обработка данных с записью результатов в Redis.
- **Пулы памяти** — использование `sync.Pool` с сегментацией по емкости для снижения нагрузки на Garbage Collector.
- **Observability** — метрики Prometheus, трассировка Jaeger, профилирование pprof и структурированные логи (`slog`).
- **Health Probes** — эндпоинты `/healthz` и `/readyz` для интеграции с Kubernetes.

## Быстрый старт

### Требования

- Go 1.21+
- `protoc` с плагинами для Go (для генерации кода из proto-файлов):
  ```bash
  brew install protobuf
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  ```

### Установка и запуск

```bash
# Загрузка библиотек ONNX Runtime
make setup-onnx

# Сборка бинарных файлов
make build

# Запуск сервера инференса
make run-server
```

Сервис будет доступен по следующим адресам:

| Сервис       | Адрес                                |
|--------------|--------------------------------------|
| REST API     | `http://localhost:8080`              |
| gRPC         | `localhost:50051`                    |
| Prometheus   | `http://localhost:9090/metrics`      |
| pprof        | `http://localhost:6060/debug/pprof/` |

### Тестирование инференса

**Одиночный запрос:**
```bash
curl -X POST http://localhost:8080/v1/infer \
  -H "Content-Type: application/json" \
  -d '{
    "model_id": "test",
    "input": {
      "shape": [1, 4],
      "data": [1.0, 2.0, 3.0, 4.0]
    }
  }'
```

**Батч-запрос (3 примера за раз):**
```bash
curl -X POST http://localhost:8080/v1/infer \
  -H "Content-Type: application/json" \
  -d '{
    "input": {
      "shape": [3, 4],
      "data": [1,2,3,4, 5,6,7,8, 0.1,0.2,0.3,0.4]
    }
  }'
```

## Структура проекта

```
.
├── api/proto/v1/               # Описания Protobuf
│   ├── inference.proto          #   Тензоры, запросы/ответы, сервис инференса
│   └── health.proto             #   Сервис проверки состояния (HealthCheck)
│
├── cmd/
│   ├── server/main.go           # Точка входа для gRPC + REST сервера
│   └── worker/main.go           # Точка входа для Kafka-воркера
│
├── configs/
│   └── config.yaml              # Конфигурация по умолчанию
│
├── internal/
│   ├── config/                  # Загрузчик YAML конфигов
│   ├── server/
│   │   ├── grpc.go              #   gRPC сервер с интерцепторами OTel
│   │   ├── rest.go              #   Gin REST API
│   │   └── handler.go           #   Реализация логики API
│   ├── runtime/
│   │   ├── onnx.go              #   Обертка над ONNX Runtime
│   │   └── validator.go         #   Валидация размерностей тензоров
│   ├── pipeline/
│   │   ├── pipeline.go          #   Оркестратор пайплайна
│   │   ├── stage.go             #   Типы данных и интерфейсы стадий
│   │   ├── stages.go            #   Препроцессинг, инференс, постпроцессинг
│   │   ├── worker.go            #   Пул воркеров (ограниченные горутины)
│   │   └── batcher.go           #   Аккумулятор для динамического батчевания
│   ├── pool/
│   │   └── pool.go              #   sync.Pool с бакетами по емкости
│   ├── stream/
│   │   ├── consumer.go          #   Потребитель Kafka
│   │   └── writer.go            #   Запись результатов в Redis
│   └── observability/
│       ├── logger.go            #   Логгер slog (JSON)
│       ├── metrics.go           #   Метрики Prometheus (счетчики, гистограммы)
│       ├── tracing.go           #   OpenTelemetry / Jaeger трассировка
│       └── health.go            #   Пробы Liveness / Readiness
│
├── pkg/tensor/
│   └── tensor.go                # Generic-структура тензора с конвертацией
│
├── test/testdata/
│   └── dummy.onnx               # Тестовая ML-модель
│
├── Makefile
├── go.mod
└── go.sum
```

## Конфигурация

Все настройки находятся в `configs/config.yaml`:

```yaml
runtime:
  model_path: "models/model.onnx"
  inter_op_threads: 2        # параллелизм между узлами графа ONNX
  intra_op_threads: 4        # параллелизм внутри одного узла (ядра)

pipeline:
  num_workers: 4             # количество горутин для инференса
  batch_size: 32             # макс. количество запросов в одном батче
  batch_timeout_ms: 10       # время ожидания до принудительной отправки батча
```

## Наблюдаемость (Observability)

### Метрики Prometheus

| Метрика | Тип | Описание |
|---------|-----|----------|
| `gopherbrain_requests_total` | counter | Общее кол-во запросов |
| `gopherbrain_request_duration_seconds` | histogram | Полное время обработки запроса |
| `gopherbrain_inference_latency_seconds` | histogram | Время только инференса в ONNX |
| `gopherbrain_batch_size` | histogram | Реальный размер батча |
| `gopherbrain_pipeline_depth` | gauge | Кол-во запросов в очереди |

### Профилирование

Инструмент `pprof` доступен для анализа производительности:

```bash
# Анализ профиля CPU (в течение 30 секунд)
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Анализ аллокаций памяти (Heap)
go tool pprof http://localhost:6060/debug/pprof/heap
```

## Команды Makefile

| Команда | Описание |
|---------|----------|
| `make proto` | Перегенерировать код из `.proto` файлов |
| `make build` | Скомпилировать сервер и воркер |
| `make test` | Запустить тесты с детектором состояния гонки (race detector) |
| `make run-server` | Собрать и запустить сервер инференса |

## Технологический стек

| Компонент | Технология |
|-----------|------------|
| Язык | Go 1.21+ (generics, slog, pprof) |
| ML Runtime | ONNX Runtime 1.22.0 |
| Связь | gRPC + Protobuf / Gin (REST) |
| Брокер сообщений | Kafka (segmentio/kafka-go) |
| Кэш / Хранилище | Redis (go-redis/v9) |
| Мониторинг | Prometheus / Jaeger (OpenTelemetry) |
| Память | sync.Pool (оптимизация GC) |
