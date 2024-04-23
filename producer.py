import numpy as np
import pandas as pd
from kafka import KafkaProducer
import time
import json

KAFKA_TOPIC_1 = "positive"
KAFKA_TOPIC_2 = "negative"
KAFKA_TOPIC_3 = "neutral" 
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

def send_three_topics(Instagram_Comment_list):
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"))

    for message in Instagram_Comment_list:
        kafka_Instagram_Comment= message["Instagram_Comment"]
        kafka_sentiment= str(message["sentiment"])

        if kafka_sentiment == 'positive':
            kafka_producer.send(KAFKA_TOPIC_1, value=kafka_Instagram_Comment)

        elif kafka_sentiment == 'negative':
            kafka_producer.send(KAFKA_TOPIC_2, value=kafka_Instagram_Comment)

        else:
            kafka_producer.send(KAFKA_TOPIC_3, value=kafka_Instagram_Comment)

        print("Instagram_Comment: ", kafka_Instagram_Comment)
        print("Sentiment: ", kafka_sentiment)
        time.sleep(2)
    kafka_producer.flush()
    kafka_producer.close()

if __name__ == "__main__":
    print("Kafka Producer has Started ... ")
    filepath = "dataset.xlsx"
    Instagram_Comment_df = pd.read_excel(filepath, usecols=['Instagram_Comment', 'sentiment'],header=0)
    Instagram_Comment_list = Instagram_Comment_df.to_dict(orient="records")
    send_three_topics(Instagram_Comment_list)
    print("Kafka Producer has Completed!. ")
