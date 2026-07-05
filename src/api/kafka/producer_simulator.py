from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

producer.send("iot", {"content": "Mohammed salah is egyptian."})
producer.flush()
print("Message envoyé !")