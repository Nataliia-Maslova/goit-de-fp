from kafka import KafkaProducer
import json
import time
import random

# --- Kafka конфіг ---
KAFKA_TOPIC = "athlete_event_results"
KAFKA_SERVER = "localhost:9092"  # заміни, якщо у тебе інший

# --- Тестові дані  ---
athletes = [
    {"athlete_id": 1, "sport": "Swimming", "event": "100m Freestyle", "country_noc": "USA", "medal": "Gold", "gender": "M", "pos": "1"},
    {"athlete_id": 2, "sport": "Athletics", "event": "Marathon", "country_noc": "KEN", "medal": "Silver", "gender": "M", "pos": "2"},
    {"athlete_id": 3, "sport": "Gymnastics", "event": "Vault", "country_noc": "CHN", "medal": "Bronze", "gender": "F", "pos": "3"},
    {"athlete_id": 4, "sport": "Swimming", "event": "200m Butterfly", "country_noc": "AUS", "medal": None, "gender": "F", "pos": "4"},
    {"athlete_id": 5, "sport": "Athletics", "event": "100m Sprint", "country_noc": "JAM", "medal": "Gold", "gender": "M", "pos": "1"}
]

# --- Ініціалізація продюсера ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Надсилання повідомлень ---
for _ in range(50):  # можна змінити кількість ітерацій
    athlete = random.choice(athletes)
    producer.send(KAFKA_TOPIC, value=athlete)
    print(f"Sent: {athlete}")
    time.sleep(1)

producer.flush()
producer.close()