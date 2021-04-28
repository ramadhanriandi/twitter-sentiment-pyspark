import sys
import json
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaProducer

CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""

auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = API(auth, wait_on_rate_limit=True,
          wait_on_rate_limit_notify=True)


class Listener(StreamListener):
    def on_data(self, data):
        try:
            msg = json.loads(data)
            out = ""
            if msg['text'][:2] == 'RT':
                return
            if "extended_tweet" in msg:
                out = msg['extended_tweet']['full_text']
            else:
                out = msg['text']
            print(out)
            producer.send("america", out.encode('utf-8'))
        except Exception as e:
            print(e)

    def on_error(self, status_code):
        print(status_code)
        return False


listener = Listener()
producer = KafkaProducer(bootstrap_servers="localhost:9092")

stream = Stream(auth=api.auth, listener=listener, tweet_mode='extended')
try:
    print('Start streaming.')
    stream.filter(track=['america'])
except KeyboardInterrupt:
    print("Stopped.")
finally:
    print('Done.')
    stream.disconnect()
    output.close()
