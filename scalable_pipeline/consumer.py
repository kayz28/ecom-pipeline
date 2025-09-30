import os
import json
import argparse
import threading
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError
from cleaner import clean_chunk
import time
import pandas as pd

COLUMNS = [
    "order_id", "product_name", "category", "quantity", "unit_price",
    "discount_percent", "region", "sale_date", "customer_email"
]

def write_partitioned(df: pd.DataFrame, OUTPUT_DIR):
    for (month, region), group in df.groupby(["month", "region"]):
        dir_path = os.path.join(OUTPUT_DIR, f"month={month}", f"region={region}")
        os.makedirs(dir_path, exist_ok=True)

        file_path = os.path.join(dir_path, f"batch_{int(time.time())}.parquet")
        group.to_parquet(file_path, index=False)
        print(f"[INFO] Written {len(group)} rows -> {file_path}")


def consumer_worker(thread_id, args):
    consumer_conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([args.topic])

    print(f"[Thread-{thread_id}] Started consuming from {args.topic}")

    buffer = []
    last_flush_time = datetime.now()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # Check flush interval
                if buffer and (datetime.now() - last_flush_time).seconds >= args.flush_interval:
                    df = pd.DataFrame(buffer)
                    clean_chunk(df)
                    write_partitioned(df, args.output_dir)
                    print(f"[Thread-{thread_id}] Flushed {len(df)} records (interval)")
                    buffer = []
                    last_flush_time = datetime.now()
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(msg.error())
                continue
                
            
            value = json.loads(msg.value().decode("utf-8"))
            record = {col: value.get(col, None) for col in COLUMNS}
            if record["order_id"] == "order_id":
                continue
            buffer.append(record)

            # Flush when batch is full
            if len(buffer) >= args.batch_size:
                pdf = pd.DataFrame(buffer)
                df = clean_chunk(pdf)
                write_partitioned(df, args.output_dir)
                print(f"[Thread-{thread_id}] Flushed {len(buffer)} records")
                buffer = []
                last_flush_time = datetime.now()

    except KeyboardInterrupt:
        print(f"[Thread-{thread_id}] Stopping...")
    finally:
        # Final flush
        if buffer:
            pdf = pd.DataFrame(buffer)
            df = clean_chunk(pdf)
            write_partitioned(df, args.output_dir)
            print(f"[Thread-{thread_id}] Final flush {len(buffer)} records")
        consumer.close()


# -------------------------------
# Main
# -------------------------------
def main():
    parser = argparse.ArgumentParser(description="Multi-threaded Kafka consumer with partitioning")
    parser.add_argument("--bootstrap-servers", type=str,help="Kafka bootstrap servers", default="localhost:9092")
    parser.add_argument("--topic", type=str,help="Kafka topic name", default="sales_topic2")
    parser.add_argument("--group-id", type=str, default="consumer-group-2", help="Kafka consumer group id")
    parser.add_argument("--batch-size", type=int, default=50000, help="Number of messages per batch")
    parser.add_argument("--flush-interval", type=int, default=30, help="Flush interval in seconds")
    parser.add_argument("--threads", type=int, default=5, help="Number of consumer threads")
    parser.add_argument("--output-dir", type=str, default="/Users/kartikanand/pipeline-project/data/scalable_data", help="Directory for partitioned output")
    args = parser.parse_args()

    threads = []
    for tid in range(args.threads):
        t = threading.Thread(target=consumer_worker, args=(tid, args))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
