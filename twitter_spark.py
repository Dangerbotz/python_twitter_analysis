from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import io
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import threading

#consumer key, consumer secret, access token, access secret.
#DO NOT SHARE!
ckey    = ""
csecret = ""
atoken  = ""
asecret = ""

class listener(StreamListener):
    def on_data(self, data):
        with io.open("tweets.txt", "w") as file:
            file.write(data)
        return(True)

    def on_error(self, status):
        print (status)

def doTwitterStreaming():
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)

    twitterStream = Stream(auth, listener())
    twitterStream.filter(track=["POTUS"]) 

t = threading.Thread(target=doTwitterStreaming)

try:
    
    t.start()

    sc = SparkContext(appName="SparkStreaming")
    ssc = StreamingContext(sc, 10)
    lines = ssc.textFileStream("input.txt")
    words = lines.flatMap(lambda line: json.loads(line)["text"].split(" "))\
                  .map(lambda x: (x, 1))\
                  .reduceByKey(lambda a, b: a+b).map(lambda x: (x[1], x[0]))\
                  .transform(lambda rdd: rdd.sortByKey(False)).map(lambda x: (x[1], x[0]))
    words.pprint()

    ssc.start() 
    ssc.awaitTermination()

finally:
    print("end")
