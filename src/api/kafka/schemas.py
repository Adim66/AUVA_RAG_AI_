from pydantic import BaseModel

class KafkaMessage(BaseModel):
    """Message attendu depuis le topic Kafka"""
    content: str

class QueryRequest(BaseModel):
    """Request body pour POST /query"""
    query: str
    top_k: int = 3

class QueryResponse(BaseModel):
    """Response body de POST /query"""
    query: str
    answer: str
    model: str
    sources: list