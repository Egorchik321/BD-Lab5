# model/train_model.py
import sys
import subprocess

# Проверка и установка зависимостей
def install_dependencies():
    required = ['scikit-learn', 'skl2onnx', 'onnxruntime', 'pandas', 'joblib']
    for package in required:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            print(f"Установка {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install_dependencies()

# Теперь импортируем
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import joblib
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

# Генерация синтетических данных для обучения
def generate_training_data(n_samples=10000):
    print("Генерация тренировочных данных...")
    data = []
    for i in range(n_samples):
        if i % 1000 == 0:
            print(f"Сгенерировано {i}/{n_samples} записей...")
        
        user_id = f"user_{np.random.randint(1, 100)}"
        avg_time_category = np.random.uniform(0, 10)
        click_count = np.random.randint(1, 20)
        purchase_count = np.random.randint(0, 5)
        recency = np.random.uniform(0, 100)
        target = 1 if np.random.random() > 0.7 else 0
        
        data.append([avg_time_category, click_count, purchase_count, recency, target])
    
    df = pd.DataFrame(data, columns=['avg_time_category', 'click_count', 'purchase_count', 'recency', 'target'])
    return df

# Обучение модели
def train_model():
    print("Обучение модели...")
    df = generate_training_data()
    X = df.drop('target', axis=1).values
    y = df['target'].values
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = RandomForestClassifier(n_estimators=50, random_state=42, max_depth=5)
    model.fit(X_train, y_train)
    
    accuracy = model.score(X_test, y_test)
    print(f"Точность модели: {accuracy:.2f}")
    
    # Сохранение в pickle
    joblib.dump(model, "model/model.pkl")
    print("Модель сохранена как model.pkl")
    
    # Экспорт в ONNX - ИСПРАВЛЕНО: initial_types вместо initial_type
    initial_type = [('float_input', FloatTensorType([None, 4]))]
    onnx_model = convert_sklearn(model, initial_types=initial_type)
    
    with open("model/model.onnx", "wb") as f:
        f.write(onnx_model.SerializeToString())
    
    print("ONNX модель сохранена как model.onnx")
    return accuracy

if __name__ == "__main__":
    # Создаем директорию model, если её нет
    import os
    os.makedirs("model", exist_ok=True)
    
    try:
        accuracy = train_model()
        print(f"\nМодель успешно обучена и экспортирована!")
        print(f"Точность: {accuracy:.2%}")
        print(f"Файлы: model.pkl, model.onnx")
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)