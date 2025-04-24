FROM python:3.10-slim
WORKDIR /app
COPY consumer.py .

COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

RUN pip install confluent_kafka snowflake-connector-python
CMD ["python", "consumer.py"]
