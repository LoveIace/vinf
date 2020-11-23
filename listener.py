import socket
import json
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

auth = OAuthHandler('NgNB0rS6QZOzRZWtrQJp8gCFT', 'K74o7d5uzKIKWasqKc4bfoLXxSfvNyxQsWj1Vz58MdXmF2e4LC')
auth.set_access_token(
    '1311008687192969216-GuNk52h8DJl5C5qqy2sttOK8YDojOB',
    '3k1eritJL1Gn5nZ3LFfDuRTdK8TlgDtbD1RoE0X6BaiYg'
)

class Listener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            if ('retweeted_status' in msg):
                if ('extended_tweet' in msg['retweeted_status']):
                    self.client_socket.send((str(msg['retweeted_status']['extended_tweet']['full_text']) + "\n").encode('utf-8'))
            elif ('extended_status' in msg):
                self.client_socket.send((str(msg['extended_status']['full_text']) + "\n").encode('utf-8'))
            else:
                self.client_socket.send((str(msg['text']) + "\n").encode('utf-8'))
        except BaseException:
            pass
        return True


    def on_error(self, status):
        print(status)
        return True

address = ('localhost', 5050)

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(address)
server_socket.listen(5)

print("Listening for client...")
connection, address = server_socket.accept()

print("Connected to Client at " + str(address))
twitter_stream = Stream(auth, Listener(connection), tweet_mode="extended_tweet")
# twitter_stream.filter(track=['Trump'])
twitter_stream.sample()