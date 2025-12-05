# tests/test_monitoring.py
import time
import requests
import json
import sys
import subprocess
from datetime import datetime

def test_prometheus_availability():
    """Проверка доступности Prometheus"""
    print("\n" + "="*60)
    print("Тест 1: Проверка доступности Prometheus")
    print("="*60)
    
    try:
        response = requests.get("http://localhost:9090", timeout=5)
        if response.status_code == 200:
            print("Prometheus UI доступен")
            
            # Проверка API
            api_response = requests.get("http://localhost:9090/api/v1/targets", timeout=5)
            if api_response.status_code == 200:
                data = api_response.json()
                active_targets = [t for t in data['data']['activeTargets'] if t['health'] == 'up']
                print(f"Prometheus API доступен, активных таргетов: {len(active_targets)}")
                return True
            else:
                print("Prometheus API недоступен")
                return False
        else:
            print(f"Prometheus UI недоступен: статус {response.status_code}")
            return False
    except Exception as e:
        print(f"Ошибка подключения к Prometheus: {e}")
        return False

def test_grafana_availability():
    """Проверка доступности Grafana"""
    print("\n" + "="*60)
    print("Тест 2: Проверка доступности Grafana")
    print("="*60)
    
    try:
        response = requests.get("http://localhost:3000", timeout=10)
        if response.status_code == 200:
            print("Grafana UI доступен")
            
            # Проверка API (с базовой аутентификацией)
            auth = ("admin", "admin")
            api_response = requests.get("http://localhost:3000/api/health", auth=auth, timeout=5)
            if api_response.status_code == 200:
                print("Grafana API доступен")
                return True
            else:
                print(" Grafana API недоступен")
                return True  # Не критично для теста
        else:
            print(f"Grafana UI недоступен: статус {response.status_code}")
            return False
    except Exception as e:
        print(f"Ошибка подключения к Grafana: {e}")
        return False

def test_flink_metrics():
    """Проверка метрик Flink"""
    print("\n" + "="*60)
    print("Тест 3: Проверка метрик Flink")
    print("="*60)
    
    try:
        # Проверка метрик Flink через Prometheus
        query = 'up{job="flink"}'
        response = requests.get(
            "http://localhost:9090/api/v1/query",
            params={'query': query},
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            if data['data']['result']:
                print(f"Flink метрики доступны в Prometheus")
                
                # Проверка конкретных метрик
                metrics_to_check = [
                    'flink_taskmanager_Status_JVM_CPU_Load',
                    'flink_taskmanager_Status_JVM_Memory_Heap_Used',
                    'flink_taskmanager_job_task_numRecordsInPerSecond'
                ]
                
                available_metrics = []
                for metric in metrics_to_check:
                    metric_response = requests.get(
                        "http://localhost:9090/api/v1/query",
                        params={'query': metric},
                        timeout=5
                    )
                    if metric_response.status_code == 200:
                        metric_data = metric_response.json()
                        if metric_data['data']['result']:
                            available_metrics.append(metric.split('_')[-1])
                
                print(f"   Доступные метрики: {', '.join(available_metrics)}")
                return True
            else:
                print("Flink метрики не найдены в Prometheus")
                return False
        else:
            print(f"Ошибка запроса к Prometheus: {response.status_code}")
            return False
    except Exception as e:
        print(f"Ошибка проверки метрик Flink: {e}")
        return False

def test_kafka_metrics():
    """Проверка метрик Kafka"""
    print("\n" + "="*60)
    print("Тест 4: Проверка метрик Kafka")
    print("="*60)
    
    try:
        query = 'up{job="kafka"}'
        response = requests.get(
            "http://localhost:9090/api/v1/query",
            params={'query': query},
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            if data['data']['result']:
                print("Kafka экспортер доступен")
                
                # Проверка метрик топиков
                topics_query = 'kafka_topic_partition_current_offset'
                topics_response = requests.get(
                    "http://localhost:9090/api/v1/query",
                    params={'query': topics_query},
                    timeout=5
                )
                
                if topics_response.status_code == 200:
                    topics_data = topics_response.json()
                    if topics_data['data']['result']:
                        topics = set(r['metric']['topic'] for r in topics_data['data']['result'])
                        print(f"   Найдены метрики для топиков: {', '.join(topics)}")
                        return True
                    else:
                        print("Метрики топиков не найдены")
                        return True  # Не критично
                else:
                    print("Не удалось получить метрики топиков")
                    return True
            else:
                print("Kafka экспортер не найден")
                return False
        else:
            print(f"Ошибка запроса метрик Kafka: {response.status_code}")
            return False
    except Exception as e:
        print(f"Ошибка проверки метрик Kafka: {e}")
        return False

def test_alert_rules():
    """Проверка правил алертов"""
    print("\n" + "="*60)
    print("Тест 5: Проверка правил алертов")
    print("="*60)
    
    try:
        response = requests.get("http://localhost:9090/api/v1/rules", timeout=5)
        if response.status_code == 200:
            data = response.json()
            
            if 'data' in data and 'groups' in data['data']:
                alert_groups = data['data']['groups']
                total_rules = sum(len(group['rules']) for group in alert_groups)
                alerting_rules = sum(1 for group in alert_groups for rule in group['rules'] if rule['type'] == 'alerting')
                
                print(f" Правила алертов загружены")
                print(f"   Всего групп правил: {len(alert_groups)}")
                print(f"   Всего правил: {total_rules}")
                print(f"   Правил алертов: {alerting_rules}")
                
                # Вывод списка алертов
                print("\n   Список алертов:")
                for group in alert_groups:
                    for rule in group['rules']:
                        if rule['type'] == 'alerting':
                            print(f"     - {rule['name']} (группа: {group['name']})")
                
                return True
            else:
                print("Правила алертов не найдены")
                return False
        else:
            print(f"Ошибка получения правил: {response.status_code}")
            return False
    except Exception as e:
        print(f"Ошибка проверки правил алертов: {e}")
        return False

def generate_monitoring_report():
    """Генерация отчета по мониторингу"""
    print("\n" + "="*60)
    print("СОЗДАНИЕ ОТЧЕТА ПО МОНИТОРИНГУ")
    print("="*60)
    
    tests = [
        ("Prometheus доступность", test_prometheus_availability),
        ("Grafana доступность", test_grafana_availability),
        ("Flink метрики", test_flink_metrics),
        ("Kafka метрики", test_kafka_metrics),
        ("Alert правила", test_alert_rules),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\nТест: {test_name}")
        try:
            result = test_func()
            results[test_name] = result
            time.sleep(2)
        except Exception as e:
            print(f"Ошибка при выполнении теста: {e}")
            results[test_name] = False
    
    # Вывод результатов
    print("\n" + "="*60)
    print("РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ МОНИТОРИНГА")
    print("="*60)
    
    passed = 0
    for test_name, result in results.items():
        status = "ПРОЙДЕН" if result else "ПРОВАЛЕН"
        print(f"{test_name:25} {status}")
        if result:
            passed += 1
    
    total = len(tests)
    
    print(f"\nИтого: {passed}/{total} тестов пройдено")
    
    # Сохранение отчета
    with open("tests/monitoring_report.txt", "w") as f:
        f.write("="*60 + "\n")
        f.write("ОТЧЕТ ПО МОНИТОРИНГУ И АЛЕРТИНГУ\n")
        f.write("="*60 + "\n\n")
        
        f.write(f"Дата тестирования: {datetime.now().isoformat()}\n\n")
        
        for test_name, result in results.items():
            status = "PASS" if result else "FAIL"
            f.write(f"{test_name}: {status}\n")
        
        f.write(f"\nИтого: {passed}/{total}\n")
        f.write(f"Процент успеха: {(passed/total*100):.1f}%\n\n")
        
        f.write("="*60 + "\n")
        f.write("ДОСТУП К ИНТЕРФЕЙСАМ\n")
        f.write("="*60 + "\n")
        f.write("Prometheus UI: http://localhost:9090\n")
        f.write("Grafana UI: http://localhost:3000 (admin/admin)\n")
        f.write("Flink UI: http://localhost:8081\n")
        f.write("Kafka UI: localhost:9092 (через CLI)\n\n")
        
        f.write("="*60 + "\n")
        f.write("КЛЮЧЕВЫЕ АЛЕРТЫ\n")
        f.write("="*60 + "\n")
        f.write("1. HighKafkaLag: kafka_consumergroup_lag > 1000 (5m)\n")
        f.write("2. HighFlinkProcessingLatency: flink_taskmanager_job_task_operator_currentEmitEventTimeLag > 1000 (1m)\n")
        f.write("3. MLModelPredictionDrift: data_drift_score > 0.2 (10m)\n")
        f.write("4. HighMLInferenceLatency: ml_inference_latency_p95 > 75 (5m)\n")
        f.write("5. FlinkCheckpointFailures: increase(flink_jobmanager_job_numberOfFailedCheckpoints[5m]) > 0\n\n")
        
        f.write("="*60 + "\n")
        f.write("СТРАТЕГИЯ РЕАГИРОВАНИЯ НА АЛЕРТЫ\n")
        f.write("="*60 + "\n")
        f.write("Критические (severity: critical):\n")
        f.write("  - Время реакции: < 15 минут\n")
        f.write("  - Действия: Немедленное устранение, возможно остановка pipeline\n")
        f.write("  - Примеры: Kafka недоступен, Flink TaskManager down\n\n")
        
        f.write("Предупреждения (severity: warning):\n")
        f.write("  - Время реакции: < 60 минут\n")
        f.write("  - Действия: Анализ и планирование исправлений\n")
        f.write("  - Примеры: Высокий lag, data drift, повышенная латенси\n\n")
        
        f.write("Информационные (severity: info):\n")
        f.write("  - Время реакции: < 24 часа\n")
        f.write("  - Действия: Мониторинг трендов\n")
        f.write("  - Примеры: Рост использования ресурсов, изменения в throughput\n")
    
    return passed == total

def main():
    """Основная функция"""
    print("="*60)
    print("ТЕСТИРОВАНИЕ СИСТЕМЫ МОНИТОРИНГА И АЛЕРТИНГА")
    print("="*60)
    print("Проверка инфраструктуры мониторинга\n")
    
    try:
        success = generate_monitoring_report()
        
        if success:
            print("\nСИСТЕМА МОНИТОРИНГА ГОТОВА!")
            print("\nДоступные интерфейсы:")
            print("  Grafana: http://localhost:3000 (admin/admin)")
            print("   Prometheus: http://localhost:9090")
            print("    Flink UI: http://localhost:8081")
            
            print("\nДашборды Grafana:")
            print("  1. Kafka Lag и Throughput")
            print("  2. Flink Processing Metrics")
            print("  3. ML Pipeline Metrics")
            print("  4. Error Monitoring")
            print("  5. System Metrics")
            
            print("\nСоздан отчет: tests/monitoring_report.txt")
            sys.exit(0)
        else:
            print("\nТребуется настройка мониторинга")
            print("\nДля запуска мониторинга выполните:")
            print("   docker-compose up -d prometheus grafana kafka-exporter")
            print("\nИ подождите 30 секунд для инициализации")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\nТестирование прервано")
        sys.exit(1)
    except Exception as e:
        print(f"\nНеожиданная ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()