FROM python:3.9-slim

WORKDIR /app

# Установка зависимостей
RUN pip install \
    evidently==0.2.8 \
    prometheus-client==0.17.0 \
    kafka-python==2.0.2 \
    pandas==1.5.0 \
    numpy==1.23.5 \
    scikit-learn==1.3.0

COPY . .

CMD ["python", "data_monitor.py"]