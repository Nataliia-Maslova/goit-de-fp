from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Налаштування з'єднання з Redpanda (Kafka-compatible)
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="athlete-topic-admin"
)

# Оголошення топіків
topics = [
    NewTopic(name="athlete_event_results", num_partitions=1, replication_factor=1),
    NewTopic(name="athlete_aggregated_metrics", num_partitions=1, replication_factor=1)
]

try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Топіки створено!")
except TopicAlreadyExistsError:
    print("Один або більше топіків вже існує.")
finally:
    admin_client.close()