# monitoring/generate_test_metrics.py
import time
import random
from prometheus_client import start_http_server, Gauge, Counter, Histogram, Summary
import threading

def generate_flink_metrics():
    """Генерация тестовых метрик Flink"""
    # Метрики Flink
    cpu_load = Gauge('flink_taskmanager_Status_JVM_CPU_Load', 'CPU Load')
    memory_used = Gauge('flink_taskmanager_Status_JVM_Memory_Heap_Used', 'Memory Used', ['component'])
    records_in = Gauge('flink_taskmanager_job_task_numRecordsInPerSecond', 'Records In Per Second', ['operator'])
    latency = Histogram('flink_taskmanager_job_task_operator_currentEmitEventTimeLag', 'Processing Latency', ['operator'])
    backpressure = Gauge('flink_taskmanager_job_task_backPressuredTimeMsPerSecond', 'Backpressure Time')
    
    # Метрики Kafka
    kafka_lag = Gauge('kafka_consumergroup_lag', 'Kafka Lag', ['consumergroup', 'topic', 'partition'])
    kafka_offset = Gauge('kafka_topic_partition_current_offset', 'Current Offset', ['topic', 'partition'])
    
    # ML метрики
    ml_latency = Summary('ml_inference_latency', 'ML Inference Latency')
    drift_score = Gauge('data_drift_score', 'Data Drift Score')
    
    # Метрики ошибок
    dlq_messages = Counter('dlq_messages_total', 'DLQ Messages', ['error_type'])
    validation_errors = Counter('validation_errors_total', 'Validation Errors', ['field'])
    
    operators = ['kafka_source', 'ml_inference', 'redis_sink', 'kafka_sink']
    consumergroups = ['flink-group-1', 'flink-group-2']
    topics = ['user_events', 'ml_predictions', 'validation_errors']
    error_types = ['validation_error', 'ml_error', 'redis_error']
    fields = ['user_id', 'product_id', 'timestamp_ms', 'event_type']
    
    while True:
        # Flink метрики
        cpu_load.set(random.uniform(10, 80))
        memory_used.labels(component='heap').set(random.uniform(500, 2000) * 1024 * 1024)
        
        for op in operators:
            records_in.labels(operator=op).set(random.uniform(100, 1000))
            latency.labels(operator=op).observe(random.uniform(5, 50))
        
        backpressure.set(random.uniform(0, 200))
        
        # Kafka метрики
        for cg in consumergroups:
            for topic in topics[:2]:
                for partition in range(3):
                    kafka_lag.labels(
                        consumergroup=cg,
                        topic=topic,
                        partition=partition
                    ).set(random.uniform(0, 1500))
                    
                    kafka_offset.labels(
                        topic=topic,
                        partition=partition
                    ).set(random.randint(10000, 50000))
        
        # ML метрики
        for _ in range(random.randint(5, 20)):
            ml_latency.observe(random.uniform(10, 100))
        
        drift_score.set(random.uniform(0.05, 0.25))
        
        # Метрики ошибок
        if random.random() < 0.3:
            dlq_messages.labels(error_type=random.choice(error_types)).inc()
        
        if random.random() < 0.2:
            validation_errors.labels(field=random.choice(fields)).inc()
        
        time.sleep(5)

def generate_system_metrics():
    """Генерация системных метрик"""
    cpu_usage = Gauge('node_cpu_seconds_total', 'CPU seconds total', ['mode', 'cpu'])
    memory_total = Gauge('node_memory_MemTotal_bytes', 'Total memory')
    memory_available = Gauge('node_memory_MemAvailable_bytes', 'Available memory')
    
    cpu_modes = ['idle', 'user', 'system', 'iowait']
    cpus = ['0', '1', '2', '3']
    
    # Инициализация
    memory_total.set(16 * 1024 * 1024 * 1024)  # 16 GB
    
    while True:
        # CPU метрики
        total_cpu = 0
        for cpu in cpus:
            for mode in cpu_modes:
                value = random.uniform(100, 1000)
                cpu_usage.labels(mode=mode, cpu=cpu).set(value)
                if mode != 'idle':
                    total_cpu += value
        
        # Memory метрики
        available = random.uniform(4, 12) * 1024 * 1024 * 1024
        memory_available.set(available)
        
        time.sleep(10)

if __name__ == '__main__':
    # Запуск сервера метрик
    start_http_server(9100)
    print("Metrics server started on port 9100")
    
    # Запуск генераторов метрик в отдельных потоках
    threading.Thread(target=generate_flink_metrics, daemon=True).start()
    threading.Thread(target=generate_system_metrics, daemon=True).start()
    
    # Бесконечный цикл
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping metric generation...")