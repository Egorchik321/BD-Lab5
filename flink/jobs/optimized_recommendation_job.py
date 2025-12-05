# flink/jobs/optimized_recommendation_job.py
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy, Duration
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common import Row
from pyflink.datastream import OutputTag
import json
import numpy as np
import onnxruntime as ort
import redis
import time
import logging
from collections import defaultdict

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OptimizedMLInferenceFunction(KeyedProcessFunction):
    def __init__(self, batch_size=32, cache_size=1000):
        self.batch_size = batch_size
        self.cache_size = cache_size
        self.redis_client = None
        self.model_session = None
        self.feature_cache = {}
        self.prediction_cache = {}
        self.batch_buffer = defaultdict(list)
        
    def open(self, runtime_context: RuntimeContext):
        logger.info("Инициализация оптимизированной ML функции...")
        
        # Оптимизация: lazy loading Redis
        self.redis_client = self._init_redis_lazy()
        
        # Оптимизация: предзагрузка модели
        self.model_session = self._load_model_optimized()
        
        # Инициализация метрик
        self.metrics = runtime_context.get_metric_group()
        self.latency_metric = self.metrics.histogram("ml_inference_latency")
        self.cache_hit_metric = self.metrics.counter("cache_hits")
        self.batch_size_metric = self.metrics.gauge("batch_size", lambda: len(self.batch_buffer))
        
    def _init_redis_lazy(self):
        """Lazy initialization Redis с connection pooling"""
        try:
            pool = redis.ConnectionPool(
                host='redis',
                port=6379,
                max_connections=10,
                decode_responses=False,
                socket_timeout=2,
                socket_connect_timeout=2
            )
            return redis.Redis(connection_pool=pool)
        except Exception as e:
            logger.warning(f"Redis недоступен: {e}. Используем in-memory cache.")
            return None
    
    def _load_model_optimized(self):
        """Оптимизированная загрузка модели с кэшированием"""
        try:
            # Оптимизация: кэширование сессии модели
            import hashlib
            model_path = "/opt/flink/usrlib/model.onnx"
            
            # Создание оптимизированной сессии
            options = ort.SessionOptions()
            options.intra_op_num_threads = 2  # Оптимизация для многоядерных систем
            options.inter_op_num_threads = 2
            options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
            options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
            
            session = ort.InferenceSession(model_path, options)
            logger.info(f"Модель загружена с оптимизациями")
            return session
        except Exception as e:
            logger.error(f"Ошибка загрузки модели: {e}")
            return None
    
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context', out: 'Collector'):
        start_time = time.perf_counter()
        user_id = value[0]
        
        try:
            # Оптимизация: проверка кэша предсказаний
            if user_id in self.prediction_cache:
                cached_pred, timestamp = self.prediction_cache[user_id]
                if time.time() - timestamp < 30:  # TTL кэша: 30 секунд
                    self.cache_hit_metric.inc()
                    prediction = cached_pred
                    logger.debug(f"Cache hit for user {user_id}")
                else:
                    # Кэш устарел
                    del self.prediction_cache[user_id]
                    prediction = self._get_prediction(user_id, value)
            else:
                prediction = self._get_prediction(user_id, value)
            
            # Формирование результата
            result = Row(
                user_id=user_id,
                product_id=value[1],
                prediction=float(prediction),
                event_time=ctx.timestamp(),
                processing_time=int(time.time() * 1000),
                latency_ms=int((time.perf_counter() - start_time) * 1000),
                cache_hit=(user_id in self.prediction_cache)
            )
            
            out.collect(result)
            
            # Замер латенси
            latency_ms = (time.perf_counter() - start_time) * 1000
            self.latency_metric.update(int(latency_ms))
            
        except Exception as e:
            logger.error(f"Ошибка обработки для user {user_id}: {e}")
            # Fallback prediction
            result = Row(
                user_id=user_id,
                product_id=value[1],
                prediction=0.5,
                event_time=ctx.timestamp(),
                processing_time=int(time.time() * 1000),
                latency_ms=-1,
                cache_hit=False,
                error=str(e)[:100]
            )
            out.collect(result)
    
    def _get_prediction(self, user_id, event_data):
        """Получение предсказания с батчингом"""
        # Добавление в батч-буфер
        self.batch_buffer[user_id].append({
            'user_id': user_id,
            'event_data': event_data,
            'timestamp': time.time()
        })
        
        # Если накопился достаточный батч - выполняем инференс
        if len(self.batch_buffer) >= self.batch_size:
            predictions = self._batch_predict()
            
            # Обновление кэша предсказаний
            for user_id, pred in predictions.items():
                self.prediction_cache[user_id] = (pred, time.time())
                
                # Ограничение размера кэша
                if len(self.prediction_cache) > self.cache_size:
                    # LRU eviction
                    oldest_key = min(self.prediction_cache.items(), 
                                    key=lambda x: x[1][1])[0]
                    del self.prediction_cache[oldest_key]
            
            # Очистка буфера
            self.batch_buffer.clear()
            
            return predictions.get(user_id, 0.5)
        else:
            # Одиночный инференс для immediate response
            return self._single_predict(user_id, event_data)
    
    def _batch_predict(self):
        """Батч-инференс для оптимизации"""
        if not self.model_session or not self.batch_buffer:
            return {}
        
        try:
            # Подготовка батча
            user_ids = list(self.batch_buffer.keys())
            features_list = []
            
            for user_id in user_ids:
                features = self._extract_features(user_id, self.batch_buffer[user_id][0]['event_data'])
                features_list.append(features)
            
            features_batch = np.vstack(features_list)
            
            # Батч-инференс
            start_inference = time.perf_counter()
            input_name = self.model_session.get_inputs()[0].name
            output_name = self.model_session.get_outputs()[0].name
            predictions = self.model_session.run([output_name], {input_name: features_batch})[0]
            
            inference_time = (time.perf_counter() - start_inference) * 1000
            logger.debug(f"Batch inference for {len(user_ids)} users: {inference_time:.2f} мс")
            
            # Сопоставление предсказаний с user_id
            result = {}
            for i, user_id in enumerate(user_ids):
                result[user_id] = float(predictions[i][0])
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка батч-инференса: {e}")
            return {}
    
    def _single_predict(self, user_id, event_data):
        """Одиночный инференс (fallback)"""
        try:
            features = self._extract_features(user_id, event_data)
            
            if self.model_session:
                input_name = self.model_session.get_inputs()[0].name
                output_name = self.model_session.get_outputs()[0].name
                result = self.model_session.run([output_name], {input_name: features})[0]
                return float(result[0][0])
            else:
                return 0.5
        except Exception as e:
            logger.error(f"Ошибка одиночного инференса: {e}")
            return 0.5
    
    def _extract_features(self, user_id, event_data):
        """Извлечение фич с кэшированием"""
        # Простая реализация для примера
        return np.array([[0.5, 10, 2, 30]], dtype=np.float32)

def create_optimized_pipeline():
    """Создание оптимизированного пайплайна"""
    # Инициализация с оптимизированными настройками
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # === КРИТИЧЕСКИЕ ОПТИМИЗАЦИИ ДЛЯ ЗАДЕРЖКИ ===
    
    # 1. Оптимизация параллелизма
    env.set_parallelism(8)  # Увеличение с 2 до 8
    env.set_max_parallelism(32)
    
    # 2. Оптимизация checkpointing для low latency
    env.enable_checkpointing(1000)  # 1 секунда (быстрее восстановление)
    env.get_checkpoint_config().set_checkpointing_mode("EXACTLY_ONCE")
    env.get_checkpoint_config().set_min_pause_between_checkpoints(500)  # Минимум 500 мс
    env.get_checkpoint_config().set_checkpoint_timeout(30000)  # 30 секунд
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    
    # 3. Оптимизация буферов и таймаутов
    env.set_buffer_timeout(5)  # 5 мс timeout для буферов
    env.get_config().set_auto_watermark_interval(200)  # Watermark каждые 200 мс
    
    # 4. Оптимизация состояния
    env.get_config().set_global_job_parameters({
        "state.backend": "rocksdb",
        "state.backend.incremental": "true",
        "state.backend.rocksdb.ttl.compaction.filter.enabled": "true",
        "state.checkpoints.dir": "file:///tmp/flink-checkpoints",
        "state.savepoints.dir": "file:///tmp/flink-savepoints"
    })
    
    # Kafka source с оптимизированными настройками
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'optimized-recommender',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': 'false',
        'fetch.min.bytes': '1024',  # Оптимизация для низкой задержки
        'fetch.max.wait.ms': '100',  # Максимум 100 мс ожидания
        'max.partition.fetch.bytes': '1048576'
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
    
    # Оптимизированная watermark стратегия
    class OptimizedTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return value[4]
    
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(2)  # Уменьшено с 5 минут до 2 секунд для low latency
    ).with_timestamp_assigner(OptimizedTimestampAssigner())
    
    # Создание потока
    source_stream = env.add_source(kafka_consumer) \
        .assign_timestamps_and_watermarks(watermark_strategy)
    
    # Применение оптимизированной ML функции
    processed_stream = source_stream \
        .key_by(lambda x: x[0]) \
        .process(OptimizedMLInferenceFunction(batch_size=32, cache_size=1000))
    
    # Оптимизированный sink
    serialization_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(Types.ROW_NAMED(
            ['user_id', 'product_id', 'prediction', 'event_time', 'processing_time', 'latency_ms', 'cache_hit'],
            [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.BIG_INT(), Types.BIG_INT(), Types.INT(), Types.BOOLEAN()]
        )).build()
    
    kafka_producer = FlinkKafkaProducer(
        topic='optimized_predictions',
        serialization_schema=serialization_schema,
        producer_config={
            'bootstrap.servers': 'kafka:9092',
            'batch.size': '16384',  # Увеличенный batch
            'linger.ms': '5',       # Минимальная задержка
            'compression.type': 'lz4',  # Сжатие для экономии bandwidth
            'acks': '1'            # Быстрое подтверждение
        }
    )
    
    processed_stream.add_sink(kafka_producer)
    
    logger.info("Оптимизированный пайплайн настроен")
    return env

def main():
    """Основная функция"""
    logger.info("Запуск оптимизированного ML Recommendation Pipeline")
    
    env = create_optimized_pipeline()
    env.execute("Optimized ML Recommendation Pipeline")

if __name__ == '__main__':
    main()