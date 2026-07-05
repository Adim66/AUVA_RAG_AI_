from fastapi import APIRouter, HTTPException
from api.kafka.schemas import QueryRequest, QueryResponse

router = APIRouter()


@router.post("/query", response_model=QueryResponse)
def query_llm(request: QueryRequest):
    from api.dependencies import get_rag_engine
    engine = get_rag_engine()

    if engine is None:
        raise HTTPException(status_code=503, detail="RAGQueryEngine not initialized")

    try:
        result = engine.PassLLMGenerationHot(
            query_text=request.query,
            top_k=request.top_k,
            verbose=False,
        )
        return QueryResponse(
            query=result["query"],
            answer=result["answer"],
            model=result["model"],
            sources=result["sources"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))