# monitoring/data-monitor/data_monitor.py
import time
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from evidently.report import Report
from evidently.metrics import DataDriftTable, DatasetDriftMetric
from evidently.metric_preset import DataDriftPreset
from prometheus_client import start_http_server, Gauge, Counter, Summary
import threading
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus метрики
DATA_DRIFT_SCORE = Gauge('data_drift_score', 'Data drift score between reference and current data')
LATE_DATA_RATE = Gauge('late_data_rate', 'Percentage of late arriving data')
DLQ_MESSAGES_TOTAL = Counter('dlq_messages_total', 'Total number of messages in DLQ', ['error_type'])
ML_INFERENCE_LATENCY = Summary('ml_inference_latency', 'ML inference latency in milliseconds')
VALIDATION_ERRORS = Counter('validation_errors_total', 'Total validation errors', ['field', 'rule'])

class DataMonitor:
    def __init__(self, kafka_bootstrap='kafka:9092'):
        self.kafka_bootstrap = kafka_bootstrap
        self.reference_data = None
        self.window_size = 1000  # Размер окна для анализа
        self.data_buffer = []
        
        # Инициализация Kafka consumer для мониторинга
        self.consumer = KafkaConsumer(
            'user_events',
            'ml_predictions_success',
            'validation_errors',
            'dlq_fatal_errors',
            bootstrap_servers=kafka_bootstrap,
            auto_offset_reset='earliest',
            group_id='data-monitor',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Загрузка референсных данных (можно из файла или базы)
        self.load_reference_data()
    
    def load_reference_data(self):
        """Загрузка референсных данных для сравнения"""
        try:
            # Генерация синтетических референсных данных
            np.random.seed(42)
            n_samples = 1000
            
            self.reference_data = pd.DataFrame({
                'user_id': [f'user_{i}' for i in range(n_samples)],
                'avg_time_category': np.random.uniform(0, 10, n_samples),
                'click_count': np.random.randint(1, 20, n_samples),
                'purchase_count': np.random.randint(0, 5, n_samples),
                'recency': np.random.uniform(0, 100, n_samples),
                'prediction': np.random.uniform(0, 1, n_samples)
            })
            
            logger.info(f"Загружено {len(self.reference_data)} референсных записей")
        except Exception as e:
            logger.error(f"Ошибка загрузки референсных данных: {e}")
            self.reference_data = None
    
    def calculate_data_drift(self, current_data):
        """Расчет data drift между референсными и текущими данными"""
        if self.reference_data is None or current_data.empty:
            return 0.0
        
        try:
            # Подготовка данных для Evidently
            ref_data = self.reference_data[['avg_time_category', 'click_count', 'purchase_count', 'recency']]
            curr_data = current_data[['avg_time_category', 'click_count', 'purchase_count', 'recency']]
            
            # Создание отчета по drift
            drift_report = Report(metrics=[DataDriftPreset()])
            drift_report.run(reference_data=ref_data, current_data=curr_data)
            
            # Получение результатов
            result = drift_report.as_dict()
            
            # Извлечение общего drift score
            if 'metrics' in result and len(result['metrics']) > 0:
                drift_score = result['metrics'][0]['result']['dataset_drift']
                
                # Если drift обнаружен, получаем детальный score
                if drift_score:
                    # Используем максимальный drift среди фич как общий score
                    drift_scores = []
                    for metric in result['metrics']:
                        if 'result' in metric and 'drift_by_columns' in metric['result']:
                            for col, col_drift in metric['result']['drift_by_columns'].items():
                                if 'drift_score' in col_drift:
                                    drift_scores.append(col_drift['drift_score'])
                    
                    if drift_scores:
                        final_drift_score = max(drift_scores)
                        DATA_DRIFT_SCORE.set(final_drift_score)
                        return final_drift_score
            
            DATA_DRIFT_SCORE.set(0.0)
            return 0.0
            
        except Exception as e:
            logger.error(f"Ошибка расчета data drift: {e}")
            return 0.0
    
    def monitor_late_data(self, messages):
        """Мониторинг late-arriving данных"""
        late_count = 0
        total_count = 0
        
        for msg in messages:
            if 'timestamp_ms' in msg:
                event_time = datetime.fromtimestamp(msg['timestamp_ms'] / 1000)
                processing_delay = (datetime.now() - event_time).total_seconds()
                
                if processing_delay > 300:  # Более 5 минут задержки
                    late_count += 1
                total_count += 1
        
        if total_count > 0:
            late_rate = (late_count / total_count) * 100
            LATE_DATA_RATE.set(late_rate)
            return late_rate
        
        return 0.0
    
    def process_messages(self):
        """Обработка сообщений из Kafka для мониторинга"""
        current_window = []
        
        for message in self.consumer:
            try:
                msg_value = message.value
                topic = message.topic
                
                # Добавление в буфер для анализа
                if topic == 'user_events':
                    self.data_buffer.append(msg_value)
                    
                    # Ограничение размера буфера
                    if len(self.data_buffer) > self.window_size:
                        self.data_buffer = self.data_buffer[-self.window_size:]
                    
                    # Мониторинг late data
                    self.monitor_late_data([msg_value])
                
                elif topic == 'ml_predictions_success':
                    # Мониторинг латенси ML инференса
                    if 'processing_time' in msg_value and 'event_time' in msg_value:
                        latency = msg_value['processing_time'] - msg_value['event_time']
                        ML_INFERENCE_LATENCY.observe(latency)
                
                elif topic == 'validation_errors':
                    # Счетчик ошибок валидации
                    field = msg_value.get('field', 'unknown')
                    rule = msg_value.get('validation_rule', 'unknown')
                    VALIDATION_ERRORS.labels(field=field, rule=rule).inc()
                
                elif topic == 'dlq_fatal_errors':
                    # Счетчик DLQ сообщений
                    error_type = msg_value.get('error_type', 'unknown')
                    DLQ_MESSAGES_TOTAL.labels(error_type=error_type).inc()
                
                # Периодический анализ data drift
                if len(self.data_buffer) >= 100:
                    current_df = pd.DataFrame(self.data_buffer)
                    drift_score = self.calculate_data_drift(current_df)
                    
                    if drift_score > 0.2:
                        logger.warning(f"Обнаружен data drift: {drift_score:.3f}")
                    
                    # Очистка буфера после анализа
                    self.data_buffer = []
                    
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения: {e}")
    
    def run(self):
        """Запуск мониторинга"""
        logger.info("Запуск data monitor...")
        
        # Запуск Prometheus метрик сервера
        start_http_server(8000)
        logger.info("Prometheus metrics server started on port 8000")
        
        # Запуск обработки сообщений
        self.process_messages()

if __name__ == "__main__":
    monitor = DataMonitor()
    monitor.run()