# feature_store/feature_extractor.py
import redis
import json
import numpy as np
from datetime import datetime

class FeatureExtractor:
    def __init__(self, redis_host='redis', redis_port=6379):
        self.redis_client = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=5
        )
    
    def store_user_event(self, user_id, event_data):
        """Сохраняем событие пользователя в Redis"""
        key = f"user:{user_id}:events"
        
        # Храним последние 100 событий
        event_data['timestamp'] = datetime.now().isoformat()
        event_json = json.dumps(event_data)
        
        # Добавляем в начало списка
        self.redis_client.lpush(key, event_json)
        
        # Обрезаем список до 100 элементов
        self.redis_client.ltrim(key, 0, 99)
        
        # Устанавливаем TTL 24 часа
        self.redis_client.expire(key, 24 * 3600)
    
    def get_user_features(self, user_id):
        """Извлекаем фичи для пользователя"""
        key = f"user:{user_id}:events"
        events_json = self.redis_client.lrange(key, 0, 99)  # последние 100 событий
        
        if not events_json:
            # Нет истории - возвращаем фичи по умолчанию
            return self._default_features()
        
        events = []
        for event_json in events_json:
            try:
                events.append(json.loads(event_json))
            except:
                continue
        
        # Извлекаем фичи
        features = self._extract_features(events)
        return features
    
    def _extract_features(self, events):
        """Извлекаем фичи из событий"""
        if not events:
            return self._default_features()
        
        # Пример фич для рекомендательной системы
        click_events = [e for e in events if e.get('event_type') == 'click' or e.get('event_type') == 'view']
        purchase_events = [e for e in events if e.get('event_type') == 'purchase']
        cart_events = [e for e in events if e.get('event_type') == 'add_to_cart']
        
        # Время в категориях (упрощенно)
        categories = {}
        for event in events:
            cat = event.get('category', 'unknown')
            if cat not in categories:
                categories[cat] = 0
            categories[cat] += 1
        
        avg_time_category = np.mean(list(categories.values())) if categories else 0
        click_count = len(click_events)
        purchase_count = len(purchase_events)
        
        # Реценси (время с последнего события в минутах)
        if events:
            last_event_time = datetime.fromisoformat(events[0].get('timestamp', datetime.now().isoformat()))
            recency = (datetime.now() - last_event_time).total_seconds() / 60
        else:
            recency = 1440  # 24 часа по умолчанию
        
        return np.array([[avg_time_category, click_count, purchase_count, recency]], dtype=np.float32)
    
    def _default_features(self):
        """Фичи по умолчанию для холодного старта"""
        return np.array([[0, 0, 0, 1440]], dtype=np.float32)