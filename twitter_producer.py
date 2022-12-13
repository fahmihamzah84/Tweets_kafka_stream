import json
from confluent_kafka import Producer
import logging
import tweepy
# from tweepy import OAuthHandler
# from tweepy import Stream
import time
from unidecode import unidecode


def get_keys(path):
    with open(path) as key:
        return json.load(key)

keys = get_keys('.secret/key.json')
access_token = keys[0]['access_token']
access_token_secret = keys[0]['access_token_secret']
consumer_key = keys[0]['consumer_key']
consumer_secret = keys[0]['consumer_secret']
bearer_token = keys[0]['bearer_token']

###################### LOGGER ###########################
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
##########################################################

################## Log Message ###########################
def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)
###########################################################

################## INITIATE PRODUCER #####################
producer = Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka Producer has been initiated...')
##########################################################

#################### Get Twitter Data #####################
topic_name = "get_tweet"

search_terms = ["Elon", "Tate", "Adin"]

class StreamListener(tweepy.StreamingClient):
    def on_tweet(self, tweet):
                data = {
                    "id" : tweet.id,
                    "created_at" : tweet.created_at,
                    "tweets" : tweet.text,
                    "lang": tweet.lang,
                    "source": tweet.source
                }
                tweets = json.dumps(data, sort_keys=True, default=str)
                producer.poll(1)
                producer.produce(topic_name, tweets,callback=receipt)
                producer.flush()
                time.sleep(3)
                
    def on_error(self, status):
        if status == 420:
            return False

def streaming_tweets():
    stream = StreamListener(
        bearer_token
    )
    for terms in search_terms:
        stream.add_rules(tweepy.StreamRule(terms))
    stream.filter(tweet_fields=["id", "text", "created_at", "lang", "source"])

###########################################################

if __name__ == "__main__":
    streaming_tweets()