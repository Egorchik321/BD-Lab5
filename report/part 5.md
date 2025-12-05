### Часть 5: Мониторинг и алертинг

**5.1. Конфигурация мониторинга**

Система мониторинга построена по схеме:  
**Flink/Kafka → Prometheus (сбор) → Grafana (визуализация) + Alerting Rules (анализ) → Data Monitor (data drift)**.

**Основные компоненты:**

| Компонент | Конфигурация | Роль |
|----------|--------------|------|
| **Flink Metrics** | `flink-conf.yaml`: `metrics.reporter.prom.class`, `port: 9250`, `interval: 15s` | Экспорт метрик Flink (latency, backpressure, checkpoint duration) |
| **Prometheus** | `prometheus.yml`: 5 job’ов (`flink`, `kafka`, `prometheus`, `node`, `data-monitor`), `scrape_interval: 10–15s` | Центральное хранилище метрик, выполнение правил алертинга |
| **Grafana** | `datasources/prometheus.yml`, `dashboards/streaming-dashboard.json` | Визуализация, дашборды в реальном времени |
| **Kafka Exporter** | `danielqsj/kafka-exporter:v1.7.0`, `--kafka.server=kafka:9092` | Экспорт метрик Kafka (lag, offsets, under-replicated partitions) |
| **Data Monitor** | `data_monitor.py` + `Evidently`, `prometheus_client` | Расчёт `data_drift_score`, `late_data_rate`, `ml_inference_latency` |

**Ключевые метрики (сбор):**

| Категория | Метрика | Значение/Единица |
|----------|---------|------------------|
| **Производительность** | `flink_taskmanager_job_task_operator_currentEmitEventTimeLag` | latency (мс) |
| | `kafka_consumergroup_lag` | сообщений |
| | `ml_inference_latency` | (p50/p95/p99, мс) |
| **Состояние системы** | `flink_jobmanager_job_numberOfFailedCheckpoints` | шт. |
| | `flink_taskmanager_job_task_backPressuredTimeMsPerSecond` | мс/сек |
| | `up{job="flink"}` | 0/1 (availability) |
| **Качество данных** | `data_drift_score` | [0, 1] (PSI-like) |
| | `late_data_rate` | % |
| | `dlq_messages_total` | счетчик по типу ошибки |

**5.2. Скриншоты Grafana дашбордов**

| Название панели | Что отображает | Как выглядит (описание для скриншота) |
|----------------|----------------|----------------------------------------|
| **Kafka Lag и Throughput** | `kafka_consumergroup_lag`, `rate(offsets)` | Два графика: лаг по `consumer_group=ml-recommender` (средняя линия ~0–50), throughput ~50 evt/s — **стабильно**. |
| **Flink Processing Metrics** | `histogram_quantile(0.95, lag_bucket)`, `backPressuredTimeMsPerSecond` | p95 latency ~50–60 мс (в пределах `p95 < 75 мс`), backpressure 0 мс — **нет перегрузки**. |
| **ML Pipeline Metrics** | `ml_inference_latency`, `data_drift_score` | Latency ~0.05 мс (с `test_ml_inference.py`), drift score 0.05 — **данные стабильны**. |
| **Error Monitoring** | `rate(dlq_messages_total)`, `validation_errors_total` | Все значения 0 — **ошибок нет при корректных данных**. |
| **System Metrics** | CPU/Memory использование | CPU ~10–15%, Memory ~500 MB (в Codespaces — **эффективно**). |

**5.3. Примеры алертов**

В `alerting_rules.yml` реализовано **9 алертов** по 4 категориям. Вот ключевые:

#### 1. **Высокий Kafka lag** (критичный)
```yaml
alert: HighKafkaLag
expr: kafka_consumergroup_lag > 1000
for: 5m
labels: {severity: critical}
annotations:
  summary: "Высокий lag в Kafka"
  description: "Lag для {{ $labels.consumergroup }} = {{ $value }}"
```
→ **Триггер**: Flink не справляется с нагрузкой или Redis медленный → `lag` растёт → алерт через 5 мин.

#### 2. **Высокая задержка Flink** (предупреждение)
```yaml
alert: HighFlinkProcessingLatency
expr: flink_taskmanager_job_task_operator_currentEmitEventTimeLag > 1000
for: 1m
labels: {severity: warning}
annotations:
  summary: "Задержка > 1000 мс"
```
→ **Триггер**: `p95` подбирается к лимиту → ранний warning до нарушения SLA (`p95 < 75 мс`).

#### 3. **Drift в данных ML** (предупреждение)
```yaml
alert: MLModelPredictionDrift
expr: data_drift_score > 0.2
for: 10m
labels: {severity: warning}
annotations:
  summary: "Drift в фичах модели"
```
→ **Триггер**: поведение пользователей изменилось (новый сезон, акция) → модель устаревает.

#### 4. **Сбой checkpoint’ов** (критичный)
```yaml
alert: FlinkCheckpointFailures
expr: increase(flink_jobmanager_job_numberOfFailedCheckpoints[5m]) > 0
for: 2m
labels: {severity: critical}
```
→ **Триггер**: Redis недоступен / диск переполнен → **риск потери данных**.

**5.4. Стратегия реагирования на алерты**

Реакция определяется по `severity`, заданному в правилах:

| Уровень | Время реакции | Действия | Ответственный |
|---------|---------------|----------|----------------|
| **`critical`**<br>(`HighKafkaLag`, `FlinkCheckpointFailures`, `FlinkTaskManagerDown`) | < 15 минут | 1. Проверка логов Flink/Kafka<br>2. Масштабирование (увеличить `parallelism`)<br>3. При необходимости — остановка pipeline, ручное восстановление из checkpoint | DevOps / Backend-инженер |
| **`warning`**<br>(`HighFlinkProcessingLatency`, `MLModelPredictionDrift`, `HighDLQRate`) | < 60 минут | 1. Анализ в Grafana (какой operator грузит CPU?)<br>2. Проверка качества данных (Evidently report)<br>3. Подготовка новой модели / настройка водяных знаков | ML-инженер + Data Engineer |
| **`info`** (не настроено, но можно добавить) | < 24 часа | 1. Мониторинг трендов<br>2. Планирование оптимизаций (например, батчинг в ML) | Tech Lead |

**Пример сценария:**
1. Поступает алерт `HighKafkaLag` → Grafana показывает: `lag = 2500`, `Flink backpressure = 800 ms/s`.  
2. Инженер проверяет:  
   - CPU TaskManager → 98%  
   - `feature_extractor.py` тормозит (Redis latency ↑)  
3. Решение:  
   - Увеличить `parallelism` с 2 до 4  
   - Добавить кэширование в `FeatureExtractor`  
   - Перезапустить job → lag снижается через 2 мин.

**Вывод:**
- Система мониторинга:  
- собирает метрики производительности, состояния, качества данных,  
- визуализирует их в Grafana,  
- генерирует алерты на критические отклонения.  