from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='anomaly_detector'
)

user_time = defaultdict(list)

print("Nasłuchuję na anomalie prędkości (> 3 transakcje w ciągu 60 sekund)...")

for msg in consumer:
    event = msg.value
    user_id = event['user_id']

    current_time = datetime.fromisoformat(event['timestamp'])

    user_time[user_id].append(current_time)
    
    time_prog = current_time - timedelta(seconds=60)
    user_time[user_id] = [t for t in user_time[user_id] if t >= time_prog]
    
    tx_count = len(user_time[user_id])
    if tx_count > 3:
        print(f"Anomalia; użytkownik: {user_id} | "
              f"Transakcje w ostatnich 60s: {tx_count}")
