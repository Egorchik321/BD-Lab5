from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy, Duration
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.watermark_strategy import TimestampAssigner

# 1. Инициализация
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)

# 2. Checkpointing для exactly-once
env.enable_checkpointing(1000)
env.get_checkpoint_config().set_checkpointing_mode("EXACTLY_ONCE")

print("Flink environment initialized")

# 3. Kafka Source
kafka_props = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'flink-test'
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

# 4. Watermark стратегия
class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[4]

watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
    Duration.of_minutes(5)
).with_timestamp_assigner(EventTimestampAssigner())

# 5. Получение потока
source_stream = env.add_source(kafka_consumer) \
    .assign_timestamps_and_watermarks(watermark_strategy)

print("Kafka source configured")

# 6. Простая обработка: подсчет событий по пользователю
processed_stream = source_stream \
    .map(lambda x: (x[0], 1)) \
    .key_by(lambda x: x[0]) \
    .reduce(lambda a, b: (a[0], a[1] + b[1]))

# 7. Kafka Sink для результатов
serialization_schema = JsonRowSerializationSchema.builder() \
    .with_type_info(Types.ROW_NAMED(
        ['user_id', 'event_count'],
        [Types.STRING(), Types.INT()]
    )).build()

kafka_producer = FlinkKafkaProducer(
    topic='user_recommendations',
    serialization_schema=serialization_schema,
    producer_config={'bootstrap.servers': 'kafka:9092'}
)

processed_stream.add_sink(kafka_producer)

print("Pipeline configured. Starting execution...")

# 8. Запуск
if __name__ == '__main__':
    env.execute("Simple Recommendation Pipeline")