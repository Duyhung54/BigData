#!/usr/bin/env python
import json, random, time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def random_rating():
    return {
        "userId": random.randint(1, 6040),
        "movieId": random.randint(1, 3952),
        "rating": round(random.uniform(0.5, 5.0), 1),
        "timestamp": int(time.time())
    }

while True:
    rating = random_rating()
    producer.send("ratings", rating)
    producer.flush()
    print(f"Sent: {rating}")
    time.sleep(1)            # 1 rating/second (tùy chỉnh)