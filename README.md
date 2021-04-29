# Twitter Sentiment Analysis

## IF4044 Big Data Technology Assignment
- 13517024 - Ferdian Ifkarsyah
- 13517077 - Dandi Agus Maulana
- 13517080 - Mgs. Muhammad Riandi Ramadhan
- 13517104 - Muhammad Fikri Hizbullah
- 13517109 - Fata Nugraha
- 13517149 - Fajar Muslim

### Requirement
1. Apache Spark 2.4.7
2. Python 3.7.x
3. Apache Kafka 2.4.0
4. spark-streaming-kafka-0-8-assembly_2.11-2.4.7.jar
5. PostgreSQL 13.x
6. Node.js 12.18.0

### How to run Kafka producer and Twitter stream listener
1. Add Twitter credential to `producer.py`.
2. Run `producer.py` by this command.
```
python3 producer.py
```

### How to run PySpark streaming processor
1. Configure Kafka topic and server in `main.py`.
2. Add PostgreSQL configurations in `main.py`.
3. Run `main.py` by this command.
```
spark-submit main.py
```
