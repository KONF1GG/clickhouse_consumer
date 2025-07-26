# Grafana Dashboard Configuration

## Шаги настройки:

### 1. Добавить Prometheus как Data Source
- URL: `http://prometheus:9090` (если Grafana тоже в Docker)  
- URL: `http://localhost:9090` (если Grafana локально)

### 2. Импортировать готовые дашборды или создать свои

## Полезные запросы для дашбордов:

### Производительность сообщений:
```promql
# Количество обработанных сообщений в секунду
rate(radius_messages_total[5m])

# Время обработки сообщений (95-й перцентиль)  
histogram_quantile(0.95, rate(radius_process_duration_seconds_bucket[5m]))

# Ошибки валидации по полям
radius_validation_errors_total
```

### ClickHouse метрики:
```promql
# Операции ClickHouse в секунду
rate(radius_clickhouse_operations_total[5m])

# Время операций ClickHouse
histogram_quantile(0.95, rate(radius_clickhouse_operation_duration_seconds_bucket[5m]))

# Размер батчей
histogram_quantile(0.95, rate(radius_clickhouse_batch_size_bucket[5m]))
```

### Здоровье системы:
```promql
# Здоровье компонентов (1=здоров, 0=проблемы)
radius_system_health

# Использование памяти
radius_memory_usage_bytes

# Активные подключения  
radius_active_connections
```

### RabbitMQ метрики:
```promql
# Подключения к RabbitMQ
radius_rabbitmq_connections_total

# Глубина очереди
radius_queue_depth
```

## Алерты:
- `radius_system_health == 0` - компонент недоступен
- `rate(radius_messages_total{status="critical_error"}[5m]) > 0.1` - много критических ошибок
- `histogram_quantile(0.95, rate(radius_process_duration_seconds_bucket[5m])) > 5` - медленная обработка
