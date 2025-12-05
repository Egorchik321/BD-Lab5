# flink/jobs/fault_tolerant_job.py
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy, Duration
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common import Row
from pyflink.datastream import OutputTag
from pyflink.common.restart_strategy import RestartStrategies
import json
import numpy as np
import onnxruntime as ort
import sys
import os
import logging
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Определяем выходные теги для DLQ
ERROR_OUTPUT_TAG = OutputTag("error_events", Types.ROW_NAMED(
    ['user_id', 'original_event', 'error_message', 'error_type', 'timestamp', 'retry_count'],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.BIG_INT(), Types.INT()]
))

VALIDATION_ERROR_TAG = OutputTag("validation_errors", Types.ROW_NAMED(
    ['user_id', 'field', 'value', 'validation_rule', 'timestamp'],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.BIG_INT()]
))

# 1. Класс для валидации событий
class EventValidator:
    @staticmethod
    def validate_event(event):
        """Валидация входящих событий"""
        errors = []
        
        # Проверка обязательных полей
        required_fields = ['user_id', 'product_id', 'event_type', 'timestamp_ms']
        for field in required_fields:
            if not event.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Валидация типов данных
        if event.get('timestamp_ms'):
            try:
                ts = int(event['timestamp_ms'])
                if ts < 0 or ts > 4102444800000:  # До 2100 года
                    errors.append(f"Invalid timestamp: {event['timestamp_ms']}")
            except:
                errors.append(f"Timestamp must be integer: {event['timestamp_ms']}")
        
        # Валидация event_type
        valid_event_types = ['view', 'click', 'add_to_cart', 'purchase']
        if event.get('event_type') and event['event_type'] not in valid_event_types:
            errors.append(f"Invalid event_type: {event['event_type']}")
        
        return errors

# 2. Улучшенный класс ML-инференса с обработкой ошибок
class FaultTolerantMLInferenceFunction(KeyedProcessFunction):
    def __init__(self, max_retries=3):
        self.max_retries = max_retries
        self.redis_client = None
        self.model_session = None
        self.retry_count_state = None  # State для отслеживания повторных попыток
        
    def open(self, runtime_context: RuntimeContext):
        logger.info("Инициализация FaultTolerantMLInferenceFunction...")
        
        # Инициализация State для отслеживания retry count
        self.retry_count_state = runtime_context.get_state(
            runtime_context.get_state_descriptor(
                "retry_count",
                Types.INT()
            )
        )
        
        # Инициализация ML модели с retry логикой
        self.model_session = self._initialize_model_with_retry(max_attempts=3)
        
        # Инициализация Redis
        self.redis_client = self._initialize_redis()
    
    def _initialize_model_with_retry(self, max_attempts=3, delay_seconds=2):
        """Инициализация модели с экспоненциальной задержкой при ошибках"""
        for attempt in range(max_attempts):
            try:
                model_path = os.path.join(os.path.dirname(__file__), '..', '..', 'model', 'model.onnx')
                if not os.path.exists(model_path):
                    model_path = "/opt/flink/usrlib/model.onnx"
                
                session = ort.InferenceSession(model_path)
                logger.info(f"ONNX модель загружена (попытка {attempt + 1})")
                return session
                
            except Exception as e:
                logger.warning(f"⚠️Ошибка загрузки модели (попытка {attempt + 1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    sleep_time = delay_seconds * (2 ** attempt)  # Экспоненциальная задержка
                    logger.info(f"⏳ Повтор через {sleep_time} сек...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Не удалось загрузить модель после {max_attempts} попыток")
                    return None
    
    def _initialize_redis(self):
        """Инициализация Redis с обработкой ошибок"""
        try:
            import redis
            client = redis.Redis(
                host='redis',
                port=6379,
                decode_responses=False,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                max_connections=10
            )
            client.ping()
            logger.info("Redis подключен")
            return client
        except Exception as e:
            logger.warning(f"Redis недоступен: {e}")
            return None
    
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context', out: 'Collector'):
        """Основной метод обработки с обработкой ошибок"""
        user_id = value[0]
        original_event = json.dumps({
            'user_id': value[0],
            'product_id': value[1],
            'category': value[2],
            'event_type': value[3],
            'timestamp_ms': value[4]
        })
        
        try:
            # 1. Валидация входящего события
            event_dict = {
                'user_id': value[0],
                'product_id': value[1],
                'event_type': value[3],
                'timestamp_ms': value[4]
            }
            
            validation_errors = EventValidator.validate_event(event_dict)
            if validation_errors:
                for error in validation_errors:
                    error_row = Row(
                        user_id=user_id,
                        field=self._get_field_from_error(error),
                        value=str(event_dict.get(self._get_field_from_error(error), 'N/A')),
                        validation_rule=error,
                        timestamp=ctx.timestamp()
                    )
                    ctx.output(VALIDATION_ERROR_TAG, error_row)
                return  # Не обрабатываем невалидные события
            
            # 2. Получение текущего счетчика повторных попыток
            current_retry = self.retry_count_state.value() or 0
            
            # 3. Основная логика обработки
            prediction = self._safe_predict(user_id, ctx)
            
            # 4. Сброс счетчика повторных попыток при успехе
            if current_retry > 0:
                self.retry_count_state.update(0)
                logger.info(f"✅ Восстановление после {current_retry} ошибок для user: {user_id}")
            
            # 5. Формирование успешного результата
            result = Row(
                user_id=user_id,
                product_id=value[1],
                prediction=float(prediction),
                event_time=ctx.timestamp(),
                processing_time=ctx.timer_service().current_processing_time(),
                status="SUCCESS"
            )
            out.collect(result)
            
        except Exception as e:
            # Обработка ошибок
            current_retry = self.retry_count_state.value() or 0
            new_retry_count = current_retry + 1
            
            if new_retry_count <= self.max_retries:
                # Повторная попытка
                self.retry_count_state.update(new_retry_count)
                logger.warning(f"Ошибка для user {user_id} (попытка {new_retry_count}/{self.max_retries}): {e}")
                
                # Регистрация таймера для повторной обработки
                retry_time = ctx.timer_service().current_processing_time() + (1000 * (2 ** current_retry))  # Экспоненциальная задержка
                ctx.timer_service().register_processing_time_timer(retry_time)
                
            else:
                # Превышен лимит попыток - отправка в DLQ
                logger.error(f"Превышен лимит повторных попыток для user {user_id}")
                
                error_row = Row(
                    user_id=user_id,
                    original_event=original_event,
                    error_message=str(e),
                    error_type=type(e).__name__,
                    timestamp=ctx.timestamp(),
                    retry_count=new_retry_count
                )
                ctx.output(ERROR_OUTPUT_TAG, error_row)
                
                # Сброс счетчика
                self.retry_count_state.update(0)
    
    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext', out: 'Collector'):
        """Обработка таймера для повторных попыток"""
        # Можно реализовать повторную обработку события
        pass
    
    def _safe_predict(self, user_id, ctx):
        """Безопасный инференс с обработкой ошибок"""
        try:
            # Упрощенная логика для примепа
            if self.model_session is None:
                raise Exception("ML модель не загружена")
            
            # Генерация фич
            features = self._get_features_safely(user_id)
            
            # Инференс
            input_name = self.model_session.get_inputs()[0].name
            output_name = self.model_session.get_outputs()[0].name
            result = self.model_session.run([output_name], {input_name: features})
            
            return result[0][0][0]
            
        except Exception as e:
            # Fallback: возвращаем предсказание по умолчанию
            logger.warning(f"Fallback для user {user_id}: {e}")
            return 0.5  # Значение по умолчанию
    
    def _get_features_safely(self, user_id):
        """Безопасное получение фич с fallback"""
        try:
            if self.redis_client:
                # Реальная логика получения фич из Redis
                pass
        except Exception:
            pass
        
        # Fallback фичи
        return np.array([[0, 0, 0, 1440]], dtype=np.float32)
    
    def _get_field_from_error(self, error_message):
        """Извлечение имени поля из сообщения об ошибке"""
        if "user_id" in error_message:
            return "user_id"
        elif "product_id" in error_message:
            return "product_id"
        elif "event_type" in error_message:
            return "event_type"
        elif "timestamp" in error_message:
            return "timestamp_ms"
        return "unknown"

# 3. Main функция с полной настройкой fault tolerance
def main():
    logger.info("Запуск Fault Tolerant ML Pipeline")
    
    # Инициализация среды
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # === КОНФИГУРАЦИЯ FAULT TOLERANCE ===
    
    # 1. Настройка checkpointing для exactly-once
    env.enable_checkpointing(500)  # Каждые 500 мс
    env.get_checkpoint_config().set_checkpointing_mode("EXACTLY_ONCE")
    env.get_checkpoint_config().set_min_pause_between_checkpoints(300)  # Минимум 300 мс между checkpoint
    env.get_checkpoint_config().set_checkpoint_timeout(60000)  # Таймаут 60 сек
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)  # Только 1 concurrent checkpoint
    env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(3)  # Допустимо 3 сбоя подряд
    
    # 2. Настройка стратегии перезапуска
    env.set_restart_strategy(
        RestartStrategies.exponential_delay_restart(
            restart_attempts=3,  # Максимум 3 попытки перезапуска
            initial_backoff=Duration.of_seconds(1),  # Начальная задержка 1 сек
            max_backoff=Duration.of_seconds(30),  # Максимальная задержка 30 сек
            backoff_multiplier=2.0,  # Множитель задержки
            reset_backoff_threshold=Duration.of_minutes(5),  # Сброс через 5 минут
            jitter_factor=0.1  # Случайный разброс 10%
        )
    )
    
    # 3. Параллелизм и состояние
    env.set_parallelism(4)
    env.set_max_parallelism(16)
    
    # === НАСТРОЙКА ИСТОЧНИКОВ И СТОКОВ ===
    
    # Kafka Source
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'fault-tolerant-recommender',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': 'false'
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
    
    # Создаем основной поток
    source_stream = env.add_source(kafka_consumer) \
        .assign_timestamps_and_watermarks(watermark_strategy)
    
    logger.info("Kafka source configured")
    
    # === ОБРАБОТКА С ОБРАБОТКОЙ ОШИБОК ===
    
    # Основной поток обработки
    processed_stream = source_stream \
        .key_by(lambda x: x[0]) \
        .process(FaultTolerantMLInferenceFunction(max_retries=3))
    
    # === ПОЛУЧЕНИЕ ПОТОКОВ С ОШИБКАМИ ===
    
    # Поток ошибок валидации
    validation_error_stream = processed_stream.get_side_output(VALIDATION_ERROR_TAG)
    
    # Поток фатальных ошибок (DLQ)
    error_stream = processed_stream.get_side_output(ERROR_OUTPUT_TAG)
    
    # === НАСТРОЙКА СТОКОВ ===
    
    # 1. Сток для успешных предсказаний
    success_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(Types.ROW_NAMED(
            ['user_id', 'product_id', 'prediction', 'event_time', 'processing_time', 'status'],
            [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.BIG_INT(), Types.BIG_INT(), Types.STRING()]
        )).build()
    
    success_producer = FlinkKafkaProducer(
        topic='ml_predictions_success',
        serialization_schema=success_schema,
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )
    
    # 2. Сток для ошибок валидации
    validation_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(Types.ROW_NAMED(
            ['user_id', 'field', 'value', 'validation_rule', 'timestamp'],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.BIG_INT()]
        )).build()
    
    validation_producer = FlinkKafkaProducer(
        topic='validation_errors',
        serialization_schema=validation_schema,
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )
    
    # 3. Сток для фатальных ошибок (DLQ)
    error_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(Types.ROW_NAMED(
            ['user_id', 'original_event', 'error_message', 'error_type', 'timestamp', 'retry_count'],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.BIG_INT(), Types.INT()]
        )).build()
    
    error_producer = FlinkKafkaProducer(
        topic='dlq_fatal_errors',
        serialization_schema=error_schema,
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )
    
    # === ПОДКЛЮЧЕНИЕ СТОКОВ ===
    
    processed_stream.add_sink(success_producer).name("success_predictions_sink")
    validation_error_stream.add_sink(validation_producer).name("validation_errors_sink")
    error_stream.add_sink(error_producer).name("fatal_errors_dlq_sink")
    
    logger.info("Fault tolerant pipeline configured. Starting execution...")
    
    # Запуск
    env.execute("Fault Tolerant ML Recommendation Pipeline")

if __name__ == '__main__':
    main()