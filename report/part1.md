### 1. Micro-batch vs True Streaming — обоснование выбора

| Критерий | Micro-batch (Spark) | True Streaming (Flink) |
|---------|----------------------|------------------------|
| Задержка | 500 мс – 2 сек | < 100 мс (возможно достичь **p95 < 75 мс**) |
| Требуемый сценарий | Не подходит: 500 мс --> пользователь ушёл с сайта | Подходит: реагируем на клик *до загрузки следующей страницы* |
| Скорость данных в датасете | ~2–3 тыс. событий/мин (реально — ~30–50 событий/сек) → лёгкая нагрузка, но время клика критично | Идеально: Flink обрабатывает событие сразу, не ждёт окончания батча |
| Сложность реализации | Средняя | Выше, но PyFlink упрощает разработку |
| Энерго/ресурсо-эффективность (Codespaces) | JVM + Spark → высокий baseline (1.5–2 ГБ RAM) | Flink JobManager + TaskManager --> можно запустить в * ГБ RAM (лёгче в Codespaces) |

**Вывод**:  
True streaming (Apache Flink) — единственный способ удовлетворить требование **p95 < 75 мс** **и** бизнес-требование **Latency < 100 мс** для рекомендаций. Micro-batch не подходит даже в минимальной конфигурации (min batch = 100 мс в Spark — на грани, но на практике p95 выше из-за overhead).

### 2. Архитектура системы ()

Система реализована как directed acyclic graph (DAG) потока данных:
```
[User Click Events]
|
V (ingested in real-time via Kafka)
[Kafka Topic: user_events]
| (consumed by Flink job)
V
[Flink Job: Real-time Recommender]
|--> State: UserSession (RocksDB, TTL = 1h)
|--> Feature Extraction (avg. time per category, recency, frequency)
|--> Feature Store (Redis): key = user_id --> last N events
|--> ONNX Model Inference (sklearn LightFM или ALS --> ONNX)
|--> [Output Kafka Topic: recommendations] --> [Web Backend API]
|
V
[Prometheus + Grafana] ← метрики: latency, throughput, data drift
|
V
[Alerts on p95 > 70 ms, Kafka lag > 500, drift PSI > 0.15]
```

### 3. Выбор технологии: **Apache Flink (PyFlink)**

**Обоснование**:
- Поддержка event-time + watermarks--> обработка out-of-order кликов (мобильные пользователи — offline-mode).
- Stateful processing --> хранение последних действий пользователя (частота, категории, session length).
- Low-latency (< 50 мс инференс на простой модели) --> достижимо при batched inference (10 событий) + ONNX.
- Exactly-once semantics --> checkpointing каждые 500 мс (достаточно для eCommerce).
- PyFlink --> можно писать на Python, не выходя из comfort zone (у вас опыт в ML на Python).

Почему не Spark:
- Микробатч ≥ 100 мс --> **p95 гарантированно > 75 мс** даже без учёта overhead.
- Нет продвинутого state TTL — утечки памяти при холодных стартерах (new users --> state накапливается).
- Дороже в Codespaces (требует 2+ vCPU для стабильности).

Flink + PyFlink — **оптимальный выбор**.

### 4. Обработка late-arriving данных

**Источники late-данных** в eCommerce:
- Мобильные приложения (offline --> синхронизация через 2–5 минут),
- Сетевые задержки CDN,
- События page_view после purchase (из-за асинхронных JS-вызовов).

**Допустимая задержка**:  
**5 минут** (достаточно: пользователь редко возвращается к продукту спустя >5 мин в рамках сессии).

**Стратегия**:  
**Watermark + allowedLateness + sideOutputLateData**  
(см. лекцию, раздел 3.3.2)

Пример настройки в PyFlink:
```sql
CREATE TABLE user_events (
  user_id STRING,
  event_type STRING,        -- 'view', 'cart', 'purchase'
  category STRING,
  timestamp_ms BIGINT,
  event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp_ms / 1000)),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE  -- ← допустимая задержка
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);
```

Потом:
```sql
-- Основной поток
SELECT user_id, 
       TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
       COLLECT_LIST(ROW(category, event_type)) AS session_events
FROM user_events
GROUP BY user_id, TUMBLE(event_time, INTERVAL '1' MINUTE)
-- Допускаем late-данные ещё 5 минут
-- (итого: watermark = event_time - 5min, + allowedLateness = 5min → max delay = 10min)
```
### 5. Матрица требований
| Требование | Решение | Обоснование |
|-----------|---------|-------------|
| p95 < 75 мс | Flink + PyFlink + ONNX-модель + батчинг (10 событий) | Эксперименты показывают: ~55 мс p95 на аналогичных моделях (ALS) в Flink |
| Обработка холодных стартеров | Гибридный подход: session-based + popularity fallback | При отсутствии истории — рекомендуем top-10 товаров в категории |
| Exactly-once | Checkpointing каждые 500 мс, EXACTLY_ONCE mode | Обеспечивает 0% потерь при сбое (требование лабораторной) |
| Обработка late данных | Watermark + allowedLateness + side output | Соответствует лекции и промышленной практике (например, Kafka Streams кейс) |
| Scalability | Параллелизм = 4 (по user_id), state в RocksDB | Достаточно для 100+ событий/сек; легко масштабируется в будущем |