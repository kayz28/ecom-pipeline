import os
import threading
import argparse
import pandas as pd
from confluent_kafka import Producer
import json


# -------------------
# Thread Locks
# -------------------
offset_lock = threading.Lock()
file_lock = threading.Lock()

# -------------------
# Kafka Producer
# -------------------
producer_conf = {"bootstrap.servers": "localhost:9092" }
producer = Producer(producer_conf)

def flush_producer():
    """Flush Kafka producer safely."""
    producer.flush()

# -------------------
# Offset Functions
# -------------------
def read_committed_offset(OFFSET_FILE) -> int:
    if not os.path.exists(OFFSET_FILE):
        return 0
    with open(OFFSET_FILE, "r") as f:
        return int(f.read().strip() or 0)

def commit_offset(offset: int, OFFSET_FILE):
    tmp_file = OFFSET_FILE + ".tmp"
    with open(tmp_file, "w") as f:
        f.write(str(offset))
    os.replace(tmp_file, OFFSET_FILE)


# -------------------
# Worker Thread
# -------------------
def worker(thread_id: int, args, header):
    CSV_FILE = args.csv_file
    OFFSET_FILE = args.offset_file
    BATCH_SIZE = args.batch_size
    KAFKA_TOPIC = args.kafka_topic

    while True:
        # Claim a batch atomically
        with offset_lock:
            start_offset = read_committed_offset(OFFSET_FILE)

        # Read the batch safely
            batch_lines = []
            with open(CSV_FILE, "r", encoding="utf-8") as f:
                if start_offset == 0:
                    f.seek(0)
                else:
                    # Subsequent reads - seek to the saved offset
                    f.seek(start_offset)
                
                lines_read = 0
                while lines_read < BATCH_SIZE:
                    line = f.readline()
                    if not line:
                        break
                    batch_lines.append(line.strip())
                    lines_read += 1
                
                end_offset = f.tell()
                
                # If we read any lines, commit the new offset
                if batch_lines:
                    commit_offset(end_offset, OFFSET_FILE)
                else:
                    break  # EOF

        # Send batch to Kafka
        if not batch_lines:
            break

        # Commit the new offset

        # Send batch to Kafka
        print("batch_lines",len(batch_lines))
        for i in range(0, len(batch_lines), 5000):
            sub_batch = batch_lines[i:i + 5000]
            for line in sub_batch:
                record = dict(zip(header, line.split(",")))
                producer.produce(KAFKA_TOPIC, value=json.dumps(record))
            flush_producer()
        print(f"[Thread-{thread_id}] Sent {len(batch_lines)} records from byte offset {start_offset} to {end_offset}")


def main():
    parser = argparse.ArgumentParser(description="Multi-threaded Kafka Producer")
    parser.add_argument("--csv_file", type=str, default="/Users/kartikanand/pipeline-project/sales_raw.csv", help="Input CSV file")
    parser.add_argument("--offset_file", type=str, default="/Users/kartikanand/pipeline-project/offset.txt", help="Offset tracking file")
    parser.add_argument("--batch_size", type=int, default=100000, help="Number of rows per batch")
    parser.add_argument("--num_threads", type=int, default=10, help="Number of producer threads")
    parser.add_argument("--kafka_topic", type=str, default="sales_topic3", help="Kafka topic name")
    args = parser.parse_args()
    with open(args.csv_file, "r", encoding="utf-8") as f:
        header = f.readline().strip().split(",")

    threads = []
    for tid in range(args.num_threads):
        t = threading.Thread(target=worker, args=(tid, args, header))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
    print("[INFO] All data processed.")
