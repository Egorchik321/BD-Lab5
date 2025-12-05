FROM python:3.9-slim

WORKDIR /app

RUN pip install kafka-python==2.0.2

COPY generator.py .

CMD ["python", "generator.py"]