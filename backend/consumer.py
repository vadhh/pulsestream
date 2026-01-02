import json
import sys
import os
import redis
import ollama
from confluent_kafka import Consumer, KafkaError
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance

# --- CONFIGURATION ---
# Docker services use service names (redis, redpanda, qdrant)
# Local runs use localhost
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:19092')
QDRANT_HOST = os.getenv('QDRANT_HOST', 'localhost')
OLLAMA_HOST = os.getenv('OLLAMA_HOST', 'http://localhost:11434')

TOPIC = 'market-news'
COLLECTION_NAME = "market_headlines"

print(f"🔧 Config: Kafka={KAFKA_BROKER}, Redis={REDIS_HOST}, Qdrant={QDRANT_HOST}, Ollama={OLLAMA_HOST}")

# 1. Connect to Infrastructure
r = redis.Redis(host=REDIS_HOST, port=6379, db=0)
q_client = QdrantClient(host=QDRANT_HOST, port=6333)

# Configure Ollama Client explicitly
o_client = ollama.Client(host=OLLAMA_HOST)

# 2. Setup Qdrant
try:
    q_client.get_collection(COLLECTION_NAME)
except Exception:
    print(f"Creating Qdrant collection: {COLLECTION_NAME}")
    q_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=4096, distance=Distance.COSINE)
    )

# 3. Kafka Consumer
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'market-news-analyst-docker',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(conf)

def get_embedding(text):
    try:
        response = o_client.embeddings(model='llama3', prompt=text)
        return response['embedding']
    except Exception as e:
        print(f"⚠️ Embedding Error: {e}")
        return []

def analyze_sentiment(headline):
    prompt = f"""
    Analyze the following financial headline. 
    Classify it strictly as one of these three: "BULLISH", "BEARISH", or "NEUTRAL".
    Then provide a very brief (1 sentence) explanation why.
    Headline: "{headline}"
    Output JSON format: {{ "sentiment": "...", "reason": "..." }}
    """
    try:
        response = o_client.chat(model='llama3', messages=[{'role': 'user', 'content': prompt}])
        return response['message']['content']
    except Exception as e:
        print(f"⚠️ AI Error: {e}")
        return None

def start_consumer():
    try:
        consumer.subscribe([TOPIC])
        print(f"🧠 AI Analyst listening...")

        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            raw_data = msg.value().decode('utf-8')
            data = json.loads(raw_data)
            print(f"\n📥 Processing: {data['headline'][:40]}...")

            ai_analysis_text = analyze_sentiment(data['headline'])
            vector = get_embedding(data['headline'])

            if vector:
                q_client.upsert(
                    collection_name=COLLECTION_NAME,
                    points=[PointStruct(
                        id=data['id'], 
                        vector=vector, 
                        payload={
                            "headline": data['headline'], 
                            "source": data['source'],
                            "timestamp": data['timestamp'],
                            "analysis": ai_analysis_text
                        }
                    )]
                )

            dashboard_data = {
                'id': data['id'],
                'headline': data['headline'],
                'source': data['source'],
                'timestamp': data['timestamp'],
                'analysis': ai_analysis_text
            }
            r.lpush('news_stream', json.dumps(dashboard_data))
            r.ltrim('news_stream', 0, 49)
            r.publish('news_updates', json.dumps(dashboard_data))

    except KeyboardInterrupt:
        print("Aborted")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_consumer()