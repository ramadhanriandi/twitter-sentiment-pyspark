import json
import psycopg2
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from types import SimpleNamespace
from textblob import TextBlob

def update_function(new_values, running_sum):
  if running_sum is None:
    running_sum = 0
  return sum(new_values, running_sum)

# text classification
def polarity_detection(text):
  polarity = TextBlob(text).sentiment.polarity

  if polarity < -0.1:
    return -1
  elif polarity >= -0.1 and polarity <= 0.1:
    return 0
  elif polarity > 0.1:
    return 1

def get_sentiment(text):
  positive = 0
  negative = 0
  neutral = 0

  sentiment_result = polarity_detection(text)

  if sentiment_result == 1:
    positive += 1
    return ("positive", positive)

  elif sentiment_result == -1:
    negative += 1
    return ("negative", negative)

  elif sentiment_result == 0:
    neutral += 1
    return ("neutral", neutral)
  
def process_lines(lines, window_length = 2, sliding_interval = 2):
    """
    Function to process "text" from tweet object in each window operation
    Params:
        lines: Spark DStream defined above
        window_length: length of window in windowing operation
        sliding_interval: sliding interval for the window operation
    Return:
        result: DStream (RDD) of variance result with 
                format --> ('positive:', num_positive), ('negative:', num_negative), ('neutral:', num_neutral)
                Example:   ('positive:', 1), ('negative:', 3), ('neutral:', 2)
    """
    tweets = lines.flatMap(lambda line: line[1].split("\n\n"))
    objects = tweets.map(lambda tweet: json.loads(tweet, object_hook=lambda d: SimpleNamespace(**d)))

    texts = objects.map(lambda obj: obj.text) # if using OAuth1

    ### if using OAuth2
    # texts = objects.map(lambda obj: obj.data.text) 

    sentiments = texts.map(get_sentiment) # apply sentiment analysis library here

    result = sentiments.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, window_length, sliding_interval)

    ### use this if you want to have accumulative sentiments, not windowed sentiments
    # accumulative = result.updateStateByKey(update_function) 
    # return accumulative

    return result

def prepare_query_value(data):
  global positive, negative, neutral

  if data[0] == "positive":
    positive.add(int(data[1]))
  elif data[0] == "negative":
    negative.add(int(data[1]))
  elif data[0] == "neutral":
    neutral.add(int(data[1]))

def insert_to_table(rdd):
  connection = psycopg2.connect(
    user = 'postgres',
    password = '1234',
    host = '127.0.0.1',
    port = '5432',
    database = 'twitter'
  )
  cursor = connection.cursor()

  rdd.foreach(lambda data: prepare_query_value(data))

  query = """INSERT INTO sentiments(positive, negative, neutral) VALUES (%s, %s, %s)"""
  cursor.execute(query, (positive.value, negative.value, neutral.value))

  connection.commit()
  cursor.close()
  connection.close()

  positive.add(positive.value * (-1))
  negative.add(negative.value * (-1))
  neutral.add(neutral.value * (-1))

# Environment variables
APP_NAME = "PySpark PostgreSQL - via JDBC"
MASTER = "local"

KAFKA_TOPIC = "twitter-topic"
BOOTSTRAP_SERVER = "localhost:9092"

# Spark configurations
conf = SparkConf() \
    .setAppName(APP_NAME) \
    .setMaster(MASTER)
sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 1) # stream each one second
ssc.checkpoint("./checkpoint")

positive = sc.accumulator(0)
negative = sc.accumulator(0)
neutral = sc.accumulator(0)

# Consume Kafka topic
lines = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC], {"metadata.broker.list": BOOTSTRAP_SERVER})

# Process lines retrieved from Kafka topic
result = process_lines(lines, window_length=10, sliding_interval=10)

# Print the result
result.pprint()

# Insert to table
result.foreachRDD(insert_to_table)

ssc.start()
ssc.awaitTermination()