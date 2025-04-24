FROM python:3.10-slim
WORKDIR /app
COPY producer.py .

COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

RUN pip install confluent_kafka
CMD ["python", "producer.py"]
