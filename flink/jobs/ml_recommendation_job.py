# flink/jobs/ml_recommendation_job.py
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy, Duration
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common import Row
import json
import numpy as np
import onnxruntime as ort
import sys
import os

# Добавляем путь к нашим модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'feature_store'))
from feature_extractor import FeatureExtractor

# 1. Класс для ML-инференса с батчингом
class MLInferenceFunction(KeyedProcessFunction):
    def __init__(self, batch_size=10):
        self.batch_size = batch_size
        self.redis_client = None
        self.model_session = None
        self.feature_extractor = None
        self.feature_cache = {}  # кэш для батчинга
        
    def open(self, runtime_context: RuntimeContext):
        print("Инициализация ML функции...")
        
        # Инициализация Redis
        try:
            self.feature_extractor = FeatureExtractor()
            print("Redis подключен успешно")
        except Exception as e:
            print(f"Ошибка подключения к Redis: {e}")
            self.feature_extractor = None
        
        # Загрузка ONNX модели
        try:
            model_path = os.path.join(os.path.dirname(__file__), '..', '..', 'model', 'model.onnx')
            if not os.path.exists(model_path):
                model_path = "/opt/flink/usrlib/model.onnx"
            
            self.model_session = ort.InferenceSession(model_path)
            print(f"ONNX модель загружена: {self.model_session.get_inputs()[0].name}")
        except Exception as e:
            print(f"Ошибка загрузки модели: {e}")
            self.model_session = None
    
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context', out: 'Collector'):
        try:
            user_id = value[0]  # user_id в Row
            
            # 1. Сохраняем событие в Feature Store
            if self.feature_extractor:
                event_data = {
                    'user_id': user_id,
                    'product_id': value[1],
                    'category': value[2],
                    'event_type': value[3],
                    'timestamp_ms': value[4]
                }
                self.feature_extractor.store_user_event(user_id, event_data)
            
            # 2. Получаем фичи для предсказания
            features = self.get_features(user_id)
            
            # 3. Добавляем в кэш для батчинга
            if user_id not in self.feature_cache:
                self.feature_cache[user_id] = {
                    'features': features,
                    'timestamp': ctx.timestamp(),
                    'original_value': value
                }
            
            # 4. Если накопилось достаточно - делаем батч-инференс
            if len(self.feature_cache) >= self.batch_size:
                self.process_batch(out)
                self.feature_cache.clear()
            
            # 5. Одиночный инференс (если нужно немедленно)
            prediction = self.single_predict(features)
            
            # 6. Формируем результат
            result = Row(
                user_id=user_id,
                product_id=value[1],
                prediction=float(prediction),
                event_time=ctx.timestamp(),
                processing_time=ctx.timer_service().current_processing_time()
            )
            out.collect(result)
            
        except Exception as e:
            print(f"Ошибка в process_element: {e}")
            # Можно добавить логирование ошибок или DLQ
    
    def get_features(self, user_id):
        """Получение фич из Feature Store"""
        if self.feature_extractor:
            return self.feature_extractor.get_user_features(user_id)
        else:
            # Заглушка если Redis недоступен
            return np.array([[0, 0, 0, 1440]], dtype=np.float32)
    
    def single_predict(self, features):
        """Одиночный инференс"""
        if self.model_session is None:
            return 0.5  # значение по умолчанию
        
        try:
            input_name = self.model_session.get_inputs()[0].name
            output_name = self.model_session.get_outputs()[0].name
            result = self.model_session.run([output_name], {input_name: features})
            return result[0][0][0]
        except Exception as e:
            print(f"Ошибка инференса: {e}")
            return 0.5
    
    def process_batch(self, out):
        """Батч-инференс для оптимизации"""
        if not self.model_session or not self.feature_cache:
            return
        
        try:
            # Подготавливаем батч
            user_ids = []
            features_list = []
            for user_id, data in self.feature_cache.items():
                user_ids.append(user_id)
                features_list.append(data['features'])
            
            features_batch = np.vstack(features_list)
            
            # Батч-инференс
            input_name = self.model_session.get_inputs()[0].name
            output_name = self.model_session.get_outputs()[0].name
            predictions = self.model_session.run([output_name], {input_name: features_batch})[0]
            
            # Отправляем результаты
            for i, user_id in enumerate(user_ids):
                result = Row(
                    user_id=user_id,
                    prediction=float(predictions[i][0]),
                    batch_processed=True,
                    timestamp=self.feature_cache[user_id]['timestamp']
                )
                out.collect(result)
                
        except Exception as e:
            print(f"Ошибка батч-инференса: {e}")

# 2. Main функция Flink Job
def main():
    # Инициализация
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # Checkpointing для exactly-once
    env.enable_checkpointing(1000)
    env.get_checkpoint_config().set_checkpointing_mode("EXACTLY_ONCE")
    
    # Kafka Source
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'ml-recommender'
    }
    
    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(Types.ROW_NAMED(
            ['user_id', 'product_id', 'category', 'event_type', 'timestamp_ms'],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.BIG_INT()]
        )).build()
    
    kafka_consumer = FlinkKafkaConsumer(
        topics='user_events',
        deserialization_schema=deserialization_schema,
        properties=kafka_props
    )
    
    # Watermark стратегия
    class EventTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return value[4]  # timestamp_ms
    
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_minutes(5)
    ).with_timestamp_assigner(EventTimestampAssigner())
    
    # Создаем поток
    source_stream = env.add_source(kafka_consumer) \
        .assign_timestamps_and_watermarks(watermark_strategy)
    
    print("Kafka source configured")
    
    # Применяем ML-инференс
    processed_stream = source_stream \
        .key_by(lambda x: x[0]) \
        .process(MLInferenceFunction(batch_size=10))
    
    # Kafka Sink для результатов
    serialization_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(Types.ROW_NAMED(
            ['user_id', 'product_id', 'prediction', 'event_time', 'processing_time'],
            [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.BIG_INT(), Types.BIG_INT()]
        )).build()
    
    kafka_producer = FlinkKafkaProducer(
        topic='ml_predictions',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )
    
    processed_stream.add_sink(kafka_producer)
    
    print("ML Pipeline configured. Starting execution...")
    
    # Запуск
    env.execute("ML Recommendation Pipeline")

if __name__ == '__main__':
    main()