from confluent_kafka import Consumer
import json
from datetime import datetime

latencies = []

# Configure the Kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'latency-checker',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['final_rfq_output'])

print("Consuming messages to calculate latency...\n")

while len(latencies) < 1000:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue

    try:
        kafka_time = msg.timestamp()[1]  # Timestamp in milliseconds
        event = json.loads(msg.value())

        rfq_id = event.get("rfq_id")
        rfq_state = event.get("rfq_state")
        receive_time_str = event.get("receive_time")  # ISO format expected

        if receive_time_str:
            receive_dt = datetime.fromisoformat(receive_time_str)
            receive_epoch = receive_dt.timestamp() * 1000  # convert to ms
            latency = kafka_time - receive_epoch
            latencies.append(latency)
            print(f"RFQ ID: {rfq_id} | State: {rfq_state} | Latency (ms): {latency:.2f}")

    except Exception as e:
        print("Failed to process message:", e)

consumer.close()
print(f"\nFinished consuming {len(latencies)} messages.")
