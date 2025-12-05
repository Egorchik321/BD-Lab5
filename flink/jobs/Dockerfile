FROM flink:1.15-scala_2.12

# Установка Python 3 и pip
RUN apt-get update && apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python

# Установка необходимых Python пакетов
RUN pip3 install \
    apache-flink==1.15.0 \
    kafka-python==2.0.2 \
    redis==4.5.0 \
    onnxruntime==1.14.0 \
    numpy==1.23.5 \
    pandas==1.5.0

# Скачивание Kafka connector JAR
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.15.0/flink-sql-connector-kafka-1.15.0.jar

# Копирование Python job и модели
COPY recommendation_job.py /opt/flink/usrlib/
COPY ml_recommendation_job.py /opt/flink/usrlib/
COPY ../../model/model.onnx /opt/flink/usrlib/model.onnx

# Установка прав
RUN chmod +x /opt/flink/usrlib/recommendation_job.py
RUN chmod +x /opt/flink/usrlib/ml_recommendation_job.py

WORKDIR /opt/flink