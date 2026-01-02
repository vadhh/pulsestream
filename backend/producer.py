import json
import time
import random
import os
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

# Configuration: Read from Env or default to Localhost
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:19092')

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'python-producer'
}

producer = Producer(conf)
topic = 'market-news'

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_news():
    sources = ['Bloomberg', 'Reuters', 'CoinDesk', 'CNBC', 'Financial Times']
    headline_structure = [
        f"{fake.company()} stock {random.choice(['soars', 'plummets', 'stabilizes'])} after earnings report.",
        f"CEO of {fake.company()} announces surprise resignation.",
        f"Bitcoin breaks {random.randint(20000, 90000)} support level.",
        f"New regulations hit the {random.choice(['Tech', 'Energy', 'Crypto'])} sector.",
        f"Market rally continues as inflation data {random.choice(['improves', 'worsens'])}."
    ]

    return {
        'id': fake.uuid4(),
        'headline': random.choice(headline_structure),
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'source': random.choice(sources)
    }

print(f"🚀 Starting Producer connected to: {KAFKA_BROKER}")

try:
    while True:
        data = generate_news()
        producer.produce(topic, key=data['id'], value=json.dumps(data), on_delivery=delivery_report)
        producer.poll(0)
        time.sleep(5) 

except KeyboardInterrupt:
    print("Aborted by user")
finally:
    producer.flush()