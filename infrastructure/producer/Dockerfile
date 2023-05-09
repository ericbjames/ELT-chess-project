FROM python:3.9.16-slim

WORKDIR /producer

COPY requirements.txt .
RUN apt-get update && apt-get install -y gcc librdkafka-dev
RUN apt-get update && apt-get install -y gcc
RUN pip install -r requirements.txt

COPY run_files .

ENV PYTHONPATH=/producer/run_files

CMD ["python", "kafka_producer.py"]
