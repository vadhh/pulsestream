import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

# Initialize Faker for realistic-looking headlines
fake = Faker()

# Configuration
conf = {
    'bootstrap.servers': 'localhost:19092', # Connects to Redpanda external port
    'client.id': 'python-producer'
}

producer = Producer(conf)
topic = 'market-news'

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_news():
    sources = ['Bloomberg', 'Reuters', 'CoinDesk', 'CNBC', 'Financial Times']
    
    # Simulate some chaos
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

print("🚀 Starting Producer... Press Ctrl+C to stop.")

try:
    while True:
        data = generate_news()
        
        # Asynchronous produce
        producer.produce(
            topic, 
            key=data['id'], 
            value=json.dumps(data), 
            on_delivery=delivery_report
        )
        
        # Wait up to 1 second for events. Callbacks will be invoked during this call
        producer.poll(0)
        
        print(f"Sent: {data['headline'][:50]}...")
        time.sleep(5) # 5 second delay as requested

except KeyboardInterrupt:
    print("Aborted by user")
finally:
    producer.flush() # Ensure all messages are sent before exiting