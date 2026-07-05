import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# src/ dans le path
sys.path.insert(0, str(Path(__file__).parent.parent))

from query_engine_generation import RAGQueryEngine
from pipeline.cdc_ingest_simple import CDCIngestionPipeline

_rag_engine: RAGQueryEngine = None
_cdc_pipeline: CDCIngestionPipeline = None


def get_rag_engine() -> RAGQueryEngine:
    return _rag_engine


def get_cdc_pipeline() -> CDCIngestionPipeline:
    return _cdc_pipeline


def init_singletons():
    global _rag_engine, _cdc_pipeline

    print("[dependencies] Loading RAGQueryEngine...")
    _rag_engine = RAGQueryEngine()

    print("[dependencies] Loading CDCIngestionPipeline...")
    _cdc_pipeline = CDCIngestionPipeline(reset_milvus=False)

    print("[dependencies] All singletons ready.")