from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import os
import time
from datetime import datetime

# Configuration
KAFKA_TOPIC = "traffic-events"
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
HDFS_URL = "http://namenode:9870"
HDFS_USER = "hadoop"

def main():
    print("Starting HDFS Ingestion Service... waiting 30s for services")
    
    # Wait for services to start
    time.sleep(30)
    
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        print("Connected to HDFS.")
    except Exception as e:
        print(f"Failed to connect to HDFS: {e}")
        return

    # Create base directory
    try:
        client.makedirs("/data/raw/traffic")
    except:
        pass

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(2, 5, 0),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='hdfs-ingest-group',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    print(f"Listening on {KAFKA_TOPIC}...")

    buffer = []
    FLUSH_SIZE = 10 
    
    for message in consumer:
        try:
            event = json.loads(message.value)
        except json.JSONDecodeError:
            print(f"Skipping invalid JSON: {message.value}")
            continue
            
        buffer.append(event)
        
        if len(buffer) >= FLUSH_SIZE:
            flush_to_hdfs(client, buffer)
            buffer = []

def flush_to_hdfs(client, data):
    
    # Grouping by day and zone
    grouped = {}
    for event in data:
        dt = datetime.fromisoformat(event['event_time'])
        date_str = dt.strftime('%Y-%m-%d')
        zone = event['zone']
        key = (date_str, zone)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(event)
    
    for (date_str, zone), events in grouped.items():
        path = f"/data/raw/traffic/date={date_str}/zone={zone}"
        filename = f"events_{int(time.time()*1000)}.json"
        full_path = f"{path}/{filename}"
        
        content = ""
        for e in events:
            content += json.dumps(e) + "\n"
            
        try:
             try:
                 client.makedirs(path)
             except:
                 pass
             
             client.write(full_path, data=content, encoding='utf-8')
             print(f"Wrote {len(events)} events to {full_path}")
        except Exception as e:
            print(f"Error writing to HDFS: {e}")

if __name__ == "__main__":
    main()
