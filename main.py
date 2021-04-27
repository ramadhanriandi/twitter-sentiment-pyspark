import pyspark
import json
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from types import SimpleNamespace

def update_function(new_values, running_sum):
    if running_sum is None:
        running_sum = 0
    return sum(new_values, running_sum)

def get_sentiment(text):
  positive = 0
  negative = 0
  neutral = 0

  # apply sentiment analysis library here, this is just dummy function 
  if len(text) % 3 == 0:
    positive += 1
    return ("positive", positive)

  elif len(text) % 3 == 1:
    negative += 1
    return ("negative", negative)

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
    # texts = objects.map(lambda obj: obj.data.text) # if using OAuth2

    sentiments = texts.map(get_sentiment) # apply sentiment analysis library here

    result = sentiments.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, window_length, sliding_interval)

    accumulative = result.updateStateByKey(update_function) # use this if you want to have accumulative sentiments, not windowed sentiments

    return accumulative

# Environment variables
APP_NAME = "PySpark PostgreSQL - via JDBC"
MASTER = "local"

KAFKA_TOPIC = "twitter-topic"
BOOTSTRAP_SERVER = "localhost:9092"

# TABLE = "sentiments"
# USER = "postgres"
# PASSWORD  = ""

# Spark configurations
conf = SparkConf() \
    .setAppName(APP_NAME) \
    .setMaster(MASTER)
    # .conf("spark.driver.extraClassPath","sqljdbc_7.2/enu/mssql-jdbc-7.2.2.jre8.jar")
sc = SparkContext(conf=conf)

# sqlContext = SQLContext(sc)
# sql = sqlContext.sparkSession

ssc = StreamingContext(sc, 1) # stream each one second
ssc.checkpoint("./checkpoint")

# jdbcDF = sql.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/twitter") \
#     .option("dbtable", TABLE) \
#     .option("user", USER) \
#     .option("password", PASSWORD) \
#     .option("driver", "org.postgresql.Driver") \
#     .load()

# Consume Kafka topic
lines = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC], {"metadata.broker.list": BOOTSTRAP_SERVER})

# Process lines retrieved from Kafka topic
result = process_lines(lines, window_length=2, sliding_interval=2)

# Print the result
result.pprint()

ssc.start()
ssc.awaitTermination()