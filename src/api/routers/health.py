from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
def health_check():
    from api.dependencies import get_rag_engine, get_cdc_pipeline
    engine   = get_rag_engine()
    pipeline = get_cdc_pipeline()
    return {
        "status": "ok",
        "rag_engine_loaded": engine is not None,
        "cdc_pipeline_loaded": pipeline is not None,
        "llm_model": engine.llm_model_name if engine else None,
    }