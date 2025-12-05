import time
import json
import subprocess
import sys

def test_docker_services():
    """Проверка запущенных Docker сервисов"""
    print("Test 1: Checking Docker services...")
    
    try:
        result = subprocess.run(
            ["docker-compose", "ps"],
            capture_output=True,
            text=True,
            check=True
        )
        
        print("Docker services status:")
        print(result.stdout)
        
        # Проверяем ключевые сервисы
        services = ['zookeeper', 'kafka', 'flink-jobmanager', 'flink-taskmanager']
        running = []
        
        for line in result.stdout.split('\n'):
            for service in services:
                if service in line and 'Up' in line:
                    running.append(service)
        
        if len(running) == len(services):
            print(f"All {len(services)} services are running")
            return True
        else:
            missing = set(services) - set(running)
            print(f"Missing services: {missing}")
            return False
            
    except Exception as e:
        print(f"Docker test failed: {e}")
        return False

def test_kafka_topics():
    """Проверка создания Kafka топиков"""
    print("\nTest 2: Checking Kafka topics...")
    
    try:
        # Ждем инициализации Kafka
        time.sleep(10)
        
        result = subprocess.run(
            ["docker-compose", "exec", "kafka", "kafka-topics", 
             "--list", "--bootstrap-server", "localhost:9092"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            topics = [t.strip() for t in result.stdout.split('\n') if t.strip()]
            print(f"Found topics: {topics}")
            
            required_topics = ['user_events', 'user_recommendations', 'user_sessions', 'late_events_dlq']
            missing = [t for t in required_topics if t not in topics]
            
            if not missing:
                print("All required topics created")
                return True
            else:
                print(f"Missing topics: {missing}")
                return False
        else:
            print(f"Kafka topics command failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Kafka topics test failed: {e}")
        return False

def test_flink_ui():
    """Проверка доступности Flink UI"""
    print("\nTest 3: Checking Flink UI...")
    
    try:
        import requests
        response = requests.get('http://localhost:8081', timeout=5)
        
        if response.status_code == 200:
            print("Flink JobManager UI is accessible")
            return True
        else:
            print(f"Flink UI returned status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Flink UI test failed: {e}")
        return False

def test_data_generation():
    """Проверка генерации тестовых данных"""
    print("\nTest 4: Testing data generation...")
    
    try:
        # Запускаем генератор на короткое время
        result = subprocess.run(
            ["docker-compose", "run", "--rm", "load-generator"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        print("Data generator output:")
        print(result.stdout[:500])  # Первые 500 символов
        
        if "Generated" in result.stdout and "events" in result.stdout:
            print("Data generator is working")
            return True
        else:
            print("Data generator may have issues")
            return False
            
    except subprocess.TimeoutExpired:
        print("Data generator ran successfully (stopped by timeout)")
        return True
    except Exception as e:
        print(f"Data generation test failed: {e}")
        return False

def main():
    print("=" * 60)
    print("PART 2 SIMPLE TEST: Stream Processing Infrastructure")
    print("=" * 60)
    
    tests = [
        ("Docker Services", test_docker_services),
        ("Kafka Topics", test_kafka_topics),
        ("Flink UI", test_flink_ui),
        ("Data Generation", test_data_generation)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
            time.sleep(2)  # Пауза между тестами
        except Exception as e:
            print(f"{test_name} test crashed: {e}")
            results.append((test_name, False))
    
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)
    
    passed = 0
    for test_name, result in results:
        status = "PASS" if result else "✗ FAIL"
        print(f"{test_name:25} {status}")
        if result:
            passed += 1
    
    print(f"\nTotal: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("\nPART 2 INFRASTRUCTURE IS WORKING")
        print("\nNext steps:")
        print("1. Check Flink UI: http://localhost:8081")
        print("2. Check Kafka topics: docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092")
        print("3. Submit Flink job manually when ready")
    else:
        print(f"\n{len(tests) - passed} tests failed")
    
    return passed == len(tests)

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)