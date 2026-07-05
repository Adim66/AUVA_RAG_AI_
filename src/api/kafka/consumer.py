import os
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer

# Executor pour ne pas bloquer la boucle asyncio avec les appels sync
_executor = ThreadPoolExecutor(max_workers=2)


def _ingest_sync(pipeline, doc_id: str, content: str):
    """Wrapper synchrone — tourne dans le ThreadPoolExecutor"""
    summary = pipeline.ingest_document(doc_id, content)
    print(
        f"[Kafka] Ingested doc_id='{doc_id}' | "
        f"added={summary['added']} deleted={summary['deleted']} unchanged={summary['unchanged']}"
    )
    return summary


async def kafka_consumer_loop(pipeline):
    """
    Coroutine qui tourne en arrière-plan pendant toute la vie du service.
    - S'abonne au topic défini dans .env
    - Pour chaque message : déclenche l'ingestion CDC
    - doc_id = nom du topic (comme convenu)
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic             = os.getenv("KAFKA_TOPIC", "my-topic")
    group_id          = os.getenv("KAFKA_GROUP_ID", "rag-consumer-group")

    print(f"[Kafka] Connecting to {bootstrap_servers}, topic='{topic}', group='{group_id}'")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: b.decode("utf-8"),
    )

    print(f"[Kafka] Consumer ready. Listening on topic '{topic}'...")

    loop = asyncio.get_event_loop()

    try:
        for raw_msg in consumer:
            try:
                # Désérialiser le message JSON
                payload = json.loads(raw_msg.value)
                content = payload.get("content", "")

                if not content:
                    print(f"[Kafka] Empty content in message, skipping.")
                    continue

                # doc_id = nom du topic
                doc_id = raw_msg.topic

                # Lancer l'ingestion dans le thread pool (non-bloquant)
                await loop.run_in_executor(
                    _executor,
                    _ingest_sync,
                    pipeline,
                    doc_id,
                    content,
                )

            except json.JSONDecodeError as e:
                print(f"[Kafka] JSON decode error: {e} | raw={raw_msg.value[:100]}")
            except Exception as e:
                print(f"[Kafka] Ingestion error: {e}")

    except Exception as e:
        print(f"[Kafka] Consumer loop crashed: {e}")
    finally:
        consumer.close()
        print("[Kafka] Consumer closed.")