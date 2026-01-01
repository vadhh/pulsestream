import json
import sys
import redis
import ollama
from confluent_kafka import Consumer, KafkaError
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance

# --- CONFIGURATION ---
REDIS_HOST = 'localhost'
KAFKA_HOST = 'localhost:19092'
QDRANT_HOST = 'localhost'
TOPIC = 'market-news'

# 1. Connect to Infrastructure
r = redis.Redis(host=REDIS_HOST, port=6379, db=0)
q_client = QdrantClient(host=QDRANT_HOST, port=6333)

# 2. Setup Qdrant Collection (Run once check)
COLLECTION_NAME = "market_headlines"
try:
    q_client.get_collection(COLLECTION_NAME)
except Exception:
    print(f"Creating Qdrant collection: {COLLECTION_NAME}")
    q_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=4096, distance=Distance.COSINE) # Llama3 embeddings are usually size 4096
    )

# 3. Kafka Consumer Setup
conf = {
    'bootstrap.servers': KAFKA_HOST,
    'group.id': 'market-news-analyst-v2',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(conf)

def get_embedding(text):
    """Ask Ollama for the vector representation of the text"""
    try:
        # We use the same model 'llama3' for embedding
        response = ollama.embeddings(model='llama3', prompt=text)
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
        response = ollama.chat(model='llama3', messages=[{'role': 'user', 'content': prompt}])
        return response['message']['content']
    except Exception:
        return None

def start_consumer():
    try:
        consumer.subscribe([TOPIC])
        print(f"🧠 AI Analyst + Memory listening on '{TOPIC}'...")

        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            # 1. Parse
            raw_data = msg.value().decode('utf-8')
            data = json.loads(raw_data)
            print(f"\n📥 Processing: {data['headline'][:40]}...")

            # 2. Parallel Tasks (conceptually): Analyze AND Embed
            ai_analysis_text = analyze_sentiment(data['headline'])
            vector = get_embedding(data['headline'])

            # 3. Store in Qdrant (Long-term Memory)
            if vector:
                q_client.upsert(
                    collection_name=COLLECTION_NAME,
                    points=[
                        PointStruct(
                            id=data['id'],
                            vector=vector,
                            payload={
                                "headline": data['headline'],
                                "source": data['source'],
                                "timestamp": data['timestamp'],
                                "analysis": ai_analysis_text
                            }
                        )
                    ]
                )
                print("   ↳ 💾 Saved to Vector DB")

            # 4. Update Dashboard (Short-term Memory)
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