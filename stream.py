import os
import json
import requests
from langdetect import detect
import nltk
from nltk.stem import WordNetLemmatizer 
import re

lemmatizer = WordNetLemmatizer()

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

def tokenize(text):
    no_urls = " ".join([word for word in text.split() if not is_url(word)])
    return re.split(r'[^a-zA-Z0-9@#-\'\\/_]+', no_urls)

def get_symbols(text):
    return [fix_emoji(char) for char in text if ord(char)>127]

def get_hashtags(text):
    return re.findall('#[^ ]+', text)

def get_ats(text):
    return re.findall(r'RT @[^:]+|@[^ ]+', text)

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

sentiment_dictionary = {}
symbol_sentiment_dictionary = {}
with open('emoji_sentiment.json') as j:
    symbol_sentiment_dictionary = json.load(j)

def process_tweet(tweet):
    text = tweet["data"]["text"]
    
    # filter tweets in english
    try:
        if detect(text) != 'en':
            return
    except:
        return

    # print(json.dumps(tweet, indent=4, sort_keys=True))
    # print the tweet
    print(text)

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

    for symbol in get_symbols(text):
        if symbol in symbol_sentiment_dictionary:
            symbol_sentiment = symbol_sentiment_dictionary[symbol]["positive-emotion"] - symbol_sentiment_dictionary[symbol]["negative-emotion"]
            sentiment_value += symbol_sentiment
            words+=1
        

    if words == 0:
        words = 1
    print("SENTIMENT EVALUATION: {}".format(sentiment_value/words))



    # print("symbols: {}".format(get_symbols(text)))
    # print("hashtags: {}".format(get_hashtags(text)))
    # print("mentions: {}".format(get_ats(text)))
    print("\n\n")

def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers, stream=True)
    print(response.status_code)
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            process_tweet(json_response)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )

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

    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)
    timeout = 0
    while True:
        connect_to_endpoint(url, headers)
        timeout += 1


if __name__ == "__main__":
    main()