import os
import json
import time
from langdetect import detect
import nltk
from nltk.stem import WordNetLemmatizer 
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SQLContext
import emoji
from collections import namedtuple

lemmatizer = WordNetLemmatizer()
sentiment_dictionary = {}
emoji_sentiment_dictionary = {}
with open('emoji_sentiment.json') as j:
    emoji_sentiment_dictionary = json.load(j)

def auth():
    return os.environ.get("BEARER_TOKEN")

def create_url():
    return "https://api.twitter.com/2/tweets/sample/stream"

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def is_url(text):
    if re.search(r'(https?:\/\/[^\s]+)', text):
        return True
    return False

def lemmatize(text):
    return [lemmatizer.lemmatize(token.lower()) for token in text]

def tokenize(text):
    text = " ".join([word for word in text.split() if not is_url(word)])
    return re.findall(r'[a-zA-Z0-9@#-\'\\/_]+', text)

def get_emojis(text):
    return [fix_emoji(c) for c in text if c in emoji.UNICODE_EMOJI]

stopwords = [
    "RT",
    "i",
    "me",
    "my",
    "myself",
    "we",
    "our",
    "ours",
    "ourselves",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
    "he",
    "him",
    "his",
    "himself",
    "she",
    "her",
    "hers",
    "herself",
    "it",
    "its",
    "itself",
    "they",
    "them",
    "their",
    "theirs",
    "themselves",
    "what",
    "which",
    "who",
    "whom",
    "this",
    "that",
    "these",
    "those",
    "am",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "have",
    "has",
    "had",
    "having",
    "do",
    "does",
    "did",
    "doing",
    "a",
    "an",
    "the",
    "and",
    "but",
    "if",
    "or",
    "because",
    "as",
    "until",
    "while",
    "of",
    "at",
    "by",
    "for",
    "with",
    "about",
    "against",
    "between",
    "into",
    "through",
    "during",
    "before",
    "after",
    "above",
    "below",
    "to",
    "from",
    "up",
    "down",
    "in",
    "out",
    "on",
    "off",
    "over",
    "under",
    "again",
    "further",
    "then",
    "once",
    "here",
    "there",
    "when",
    "where",
    "why",
    "how",
    "all",
    "any",
    "both",
    "each",
    "few",
    "more",
    "most",
    "other",
    "some",
    "such",
    "no",
    "nor",
    "not",
    "only",
    "own",
    "same",
    "so",
    "than",
    "too",
    "very",
    "s",
    "t",
    "can",
    "will",
    "just",
    "don",
    "should",
    "now"
]

def remove_stopwords(text):
    to_remove = '|'.join(stopwords)
    regex = re.compile(r'\b('+to_remove+r')\b', flags=re.IGNORECASE)
    return regex.sub("", text)
    
def language_filter(text, lang):
    try:
        if detect(text) == lang:
            return True
    except:
        return False
    return False

def process_tweet(text):
    sentiment_value = 0
    words = 0

    text = remove_stopwords(text)

    # lemmatize each token
    for token in tokenize(text):
        # print(token)
        token = token.lower()
        token = lemmatizer.lemmatize(token)

        if token in sentiment_dictionary:
            sentiment_value+=sentiment_dictionary[token]
        if not re.search(r'[^a-zA-Z]', token):
            words+=1

    for emoji in get_emojis(text):
        if emoji in emoji_sentiment_dictionary:
            emoji_sentiment = emoji_sentiment_dictionary[emoji]["positive-emotion"] - emoji_sentiment_dictionary[emoji]["negative-emotion"]
            sentiment_value += emoji_sentiment
            words+=1
        

    if words == 0:
        words = 1

    senteval = sentiment_value/words
    return senteval

def fix_emoji(emoji):
    ret = re.sub(br".*(\\[^\\]*)$", br'\1' ,emoji.encode('unicode-escape')).decode('unicode-escape')
    return ret

def main():
    with open('dict.tff','r') as f:
        lines = f.readlines()
    
    translate = {
        "weak":0.5,
        "strong":1,
        "positive":1,
        "neutral":0,
        "both":0,
        "negative":-1
    }

    for line in lines:
        word = re.sub(r'.*word1=([a-z]+)\spos.*\n', r'\1', line)
        subjectivity = re.sub(r'.*type=([a-z]+)subj.*\n', r'\1', line)
        polarity = re.sub(r'.*priorpolarity=([a-z]+)\n', r'\1', line)
        sentiment_dictionary[word] = translate[polarity] * translate[subjectivity]

    spark = SparkContext("local[4]", "sentimento")
    spark.setLogLevel("ERROR")
    sqlc = SQLContext(spark)
    ssc = StreamingContext(spark, 10)
    ssc.checkpoint("checkpoint")

    socketStream = ssc.socketTextStream("localhost", 5050)

    fields = ('text', 'sentiment')
    Tweet = namedtuple('Tweet', fields)

    evaluated = socketStream.filter(lambda tweet: language_filter(tweet, 'en'))\
        .map(lambda tweet: (tweet, process_tweet(tweet)))\
        .map(lambda tweet: Tweet(tweet[0], tweet[1]))

    evaluated.window(30,20).foreachRDD(lambda rdd: rdd.toDF().registerTempTable("tweets"))

    print("Starting spark")
    ssc.start()

    print("Processing tweets...")
    time.sleep(30)
    while True:
        window = 0
        time.sleep(20)
        df = sqlc.sql('Select * from tweets').toPandas()
        df.to_csv("data_{}.csv".format(window))
        print("5 most positive comments:")
        print(df.nlargest(5, 'sentiment'))
        print("\n5 most negative comments:")
        print(df.nsmallest(5, 'sentiment'))


        print('Average sentiment of tweets: {}'.format(df['sentiment'].mean()))

        window+=1
        
    ssc.awaitTermination()


    # print("Average sentiment for given track {}".format(total_sentiment/tweet_count))

if __name__ == "__main__":
    main()