import sys
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

# Assure que src/ est dans le path AVANT tout import interne
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    from api.dependencies import init_singletons, get_cdc_pipeline
    from api.kafka.consumer import kafka_consumer_loop

    init_singletons()
    print("[main] Singletons ready, starting server...")

    consumer_task = None

    yield  # FastAPI commence à accepter les requêtes ICI

    # Kafka démarre après que le serveur est up
    pipeline = get_cdc_pipeline()
    consumer_task = asyncio.create_task(kafka_consumer_loop(pipeline))
    print("[main] Kafka consumer task started.")

    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="LiveVectorLake — RAG Microservice",
    version="1.0.0",
    lifespan=lifespan,
)

# Imports des routers ici aussi, après app créé
from api.routers import query, health  # noqa: E402
app.include_router(health.router, tags=["Health"])
app.include_router(query.router,  tags=["Query"])