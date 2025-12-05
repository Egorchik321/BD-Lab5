import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import sys

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
TOPIC = 'user_events'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

categories = ['electronics', 'books', 'clothing', 'home', 'sports']
event_types = ['view', 'add_to_cart', 'purchase']

def generate_event(event_id):
    user_id = f"user_{random.randint(1, 50)}"
    category = random.choice(categories)
    product_id = f"product_{random.randint(1, 100)}"
    event_type = random.choice(event_types)
    
    # 10% late events
    is_late = random.random() < 0.1
    delay_seconds = random.randint(300, 600) if is_late else 0
    
    event_time = datetime.now() - timedelta(seconds=delay_seconds)
    timestamp_ms = int(event_time.timestamp() * 1000)
    
    return {
        'event_id': event_id,
        'user_id': user_id,
        'product_id': product_id,
        'category': category,
        'event_type': event_type,
        'timestamp_ms': timestamp_ms,
        'is_late': is_late
    }

def main():
    print("Starting test data generator...")
    print(f"Target: {KAFKA_BOOTSTRAP_SERVERS}")
    
    event_count = 0
    
    try:
        while event_count < 100:  # Генерируем 100 событий для теста
            event = generate_event(event_count)
            
            producer.send(TOPIC, value=event)
            
            event_count += 1
            
            if event_count % 10 == 0:
                print(f"Generated {event_count} events")
                sys.stdout.flush()
            
            time.sleep(0.1)  # 10 событий в секунду
            
        producer.flush()
        print(f"\nTotal events generated: {event_count}")
        print("Generator finished successfully")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()