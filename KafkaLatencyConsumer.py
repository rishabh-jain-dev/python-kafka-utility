from confluent_kafka import Consumer
import json
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import signal
import sys

class KafkaLatencyConsumer:
    def __init__(self, brokers, topic, group_id='latency-checker'):
        self.brokers = brokers
        self.topic = topic
        self.group_id = group_id
        self.latencies = []
        self.running = True

        self.consumer = Consumer({
            'bootstrap.servers': self.brokers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        })

    def signal_handler(self, sig, frame):
        print("\nGracefully shutting down...")
        self.running = False
        self.consumer.close()
        self.plot_histogram()
        sys.exit(0)

    def start(self, max_events=1000):
        signal.signal(signal.SIGINT, self.signal_handler)
        self.consumer.subscribe([self.topic])
        print(f"Consuming messages from topic: {self.topic}")

        while self.running and len(self.latencies) < max_events:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Error:", msg.error())
                continue

            try:
                kafka_time = msg.timestamp()[1]  # Timestamp in ms
                event = json.loads(msg.value())
                receive_time_str = event.get("receive_time")  # ISO format expected
                if receive_time_str:
                    receive_dt = datetime.fromisoformat(receive_time_str)
                    receive_epoch = receive_dt.timestamp() * 1000
                    latency = kafka_time - receive_epoch
                    self.latencies.append(latency)
                    print(f"Latency (ms): {latency}")
            except Exception as e:
                print("Failed to process message:", e)

        self.consumer.close()
        self.plot_histogram()

    def plot_histogram(self):
        if not self.latencies:
            print("No latency data collected.")
            return

        lat_array = np.array(self.latencies)
        mean_latency = np.mean(lat_array)
        median_latency = np.median(lat_array)
        p95_latency = np.percentile(lat_array, 95)

        print("\nLatency Summary:")
        print(f"Mean: {mean_latency:.2f} ms")
        print(f"Median: {median_latency:.2f} ms")
        print(f"95th Percentile: {p95_latency:.2f} ms")

        plt.figure(figsize=(10, 6))
        plt.hist(self.latencies, bins=50, edgecolor='black', alpha=0.7)
        plt.axvline(mean_latency, color='red', linestyle='--', label='Mean')
        plt.axvline(median_latency, color='green', linestyle='--', label='Median')
        plt.axvline(p95_latency, color='orange', linestyle='--', label='95th Percentile')
        plt.title("Latency Histogram (ms)")
        plt.xlabel("Latency (ms)")
        plt.ylabel("Frequency")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

# --- Run the consumer ---
if __name__ == "__main__":
    brokers = "localhost:9092"
    topic = "final_rfq_output"

    consumer = KafkaLatencyConsumer(brokers=brokers, topic=topic)
    consumer.start(max_events=1000)
