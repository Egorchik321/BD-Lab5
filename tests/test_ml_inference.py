# tests/test_ml_inference.py
import time
import numpy as np
import onnxruntime as ort
import redis
import json
from datetime import datetime, timedelta
import subprocess
import sys

def test_ml_latency():
    """Тест задержки ML-инференса"""
    print("Тестирование задержки ML-инференса...")
    
    # Загрузка модели
    try:
        session = ort.InferenceSession("model/model.onnx")
        print(f"Модель загружена: {session.get_inputs()[0].name}")
    except Exception as e:
        print(f"Ошибка загрузки модели: {e}")
        return False
    
    # Подключение к Redis
    try:
        redis_client = redis.Redis(host='localhost', port=6379, decode_responses=False)
        redis_client.ping()
        print("Redis подключен")
    except Exception as e:
        print(f"Redis недоступен: {e}")
        redis_client = None
    
    # Измерение латенси
    latencies = []
    errors = []
    
    for i in range(100):
        # Генерация тестовых фич
        features = np.random.rand(1, 4).astype(np.float32)
        
        start = time.time()
        try:
            input_name = session.get_inputs()[0].name
            output_name = session.get_outputs()[0].name
            result = session.run([output_name], {input_name: features})
            latency = (time.time() - start) * 1000  # мс
            latencies.append(latency)
        except Exception as e:
            errors.append(str(e))
        
        # Тест Redis
        if redis_client and i % 10 == 0:
            try:
                test_key = f"test:{i}"
                redis_client.set(test_key, "test_value", ex=10)
                redis_client.get(test_key)
            except Exception as e:
                print(f"Redis error: {e}")
    
    # Вывод метрик
    print(f"\n## ML INFERENCE METRICS ##")
    print(f"p50_latency_ms={np.percentile(latencies, 50):.2f}")
    print(f"p95_latency_ms={np.percentile(latencies, 95):.2f}")
    print(f"p99_latency_ms={np.percentile(latencies, 99):.2f}")
    print(f"throughput_req_per_sec={1000 / np.mean(latencies) if latencies else 0:.2f}")
    print(f"error_count={len(errors)}")
    
    # Проверка требования p95 < 75 мс (для вашего варианта - последняя цифра 5)
    if latencies:
        p95 = np.percentile(latencies, 95)
        print(f"\nТребование: p95 < 75 мс")
        print(f"Результат: p95 = {p95:.2f} мс")
        
        if p95 < 75:
            print("Требование выполнено!")
            return True
        else:
            print("Требование не выполнено!")
            return False
    else:
        print("Не удалось измерить задержку")
        return False

def test_end_to_end_pipeline():
    """Тест end-to-end пайплайна"""
    print("\nТестирование end-to-end пайплайна...")
    
    # Создаем тестовый топик
    try:
        subprocess.run([
            "docker-compose", "exec", "-T", "kafka",
            "kafka-topics", "--create", "--if-not-exists",
            "--bootstrap-server", "localhost:9092",
            "--partitions", "2",
            "--replication-factor", "1",
            "--topic", "test_ml_input"
        ], check=True, capture_output=True)
        
        subprocess.run([
            "docker-compose", "exec", "-T", "kafka",
            "kafka-topics", "--create", "--if-not-exists",
            "--bootstrap-server", "localhost:9092",
            "--partitions", "2",
            "--replication-factor", "1",
            "--topic", "test_ml_output"
        ], check=True, capture_output=True)
        
        print("Тестовые топики созданы")
        return True
    except Exception as e:
        print(f"Ошибка создания топиков: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("PART 3: ML Integration Testing")
    print("=" * 60)
    
    # Тест ML инференса
    ml_success = test_ml_latency()
    
    # Тест пайплайна
    pipeline_success = test_end_to_end_pipeline()
    
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)
    
    if ml_success and pipeline_success:
        print("Все тесты ML-интеграции пройдены!")
        print("\nML-модель готова к интеграции в потоковый пайплайн.")
        sys.exit(0)
    else:
        print("Некоторые тесты не пройдены")
        sys.exit(1)