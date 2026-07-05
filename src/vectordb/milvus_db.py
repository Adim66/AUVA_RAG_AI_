from typing import List, Dict, Any
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType, utility

class MilvusDB:
    def __init__(self, host: str = "localhost", port: str = "19530", collection_name: str = "doc_chunks"):
        self.host = host
        self.port = port
        self.collection_name = collection_name
        self.collection = None
        self._connected = False
        
    def connect(self):
        """Connect to Milvus (persistent connection)"""
        if not self._connected:
            connections.connect("default", host=self.host, port=self.port)
            self._connected = True
        
    def create_collection(self, dim: int = 384):
        """Create collection with schema including temporal fields, content, and position"""
        if utility.has_collection(self.collection_name):
            utility.drop_collection(self.collection_name)
            
        fields = [
            FieldSchema(name="chunk_id", dtype=DataType.VARCHAR, max_length=64, is_primary=True, auto_id=False),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="status", dtype=DataType.VARCHAR, max_length=16),
            FieldSchema(name="doc_id", dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name="position", dtype=DataType.INT64),
            FieldSchema(name="valid_from", dtype=DataType.INT64),
            FieldSchema(name="valid_to", dtype=DataType.INT64),
            FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=2000)
        ]
        schema = CollectionSchema(fields, "Document chunk collection with versioning")
        self.collection = Collection(self.collection_name, schema)
        
        # Optimized index for low latency
        index_params = {"metric_type": "IP", "index_type": "HNSW", "params": {"M": 8, "efConstruction": 100}}
        self.collection.create_index(field_name="vector", index_params=index_params)
        self.collection.load()
        
    def insert(self, chunk_ids: List[str], vectors: List[List[float]], statuses: List[str], doc_ids: List[str], positions: List[int], valid_from: List[int], valid_to: List[int], contents: List[str] = None):
        """Insert vectors into collection with temporal metadata, position, and content"""
        if not self.collection:
            self.collection = Collection(self.collection_name)
            self.collection.load()
        
        if contents is None:
            contents = [""] * len(chunk_ids)
        
        data = [chunk_ids, vectors, statuses, doc_ids, positions, valid_from, valid_to, contents]
        self.collection.insert(data)
        self.collection.flush()
        
    def search(self, query_vector: List[float], limit: int = 5, filter_expr: str = "status == 'active'") -> List[Dict]:
        """Search for similar vectors (optimized)"""
        if not self.collection:
            self.collection = Collection(self.collection_name)
            self.collection.load()
        
        search_params = {"metric_type": "IP", "params": {"ef": 64}}
        results = self.collection.search([query_vector], "vector", search_params, limit=limit, expr=filter_expr, output_fields=["chunk_id", "status", "doc_id", "position", "valid_from", "valid_to", "content"])
        print(f"[MilvusDB] Search returned {len(results[0])} results.")
        return [{"chunk_id": hit.id, "score": hit.score, "status": hit.entity.get("status"), "doc_id": hit.entity.get("doc_id"), "position": hit.entity.get("position"), "valid_from": hit.entity.get("valid_from"), "valid_to": hit.entity.get("valid_to"), "content": hit.entity.get("content")} for hit in list(results[0])]
    
    def update_status(self, chunk_ids: List[str], new_status: str = "inactive"):
        """Mark chunks as inactive (for CDC)"""
        # Milvus doesn't support direct updates, so delete and reinsert
        self.collection.delete(f"chunk_id in {chunk_ids}")
    
    def get_all_chunk_ids(self, filter_expr: str = "status == 'active'") -> List[str]:
        """Get all chunk IDs matching filter"""
        if not self.collection:
            self.collection = Collection(self.collection_name)
        self.collection.load()
        # Milvus query has default limit of 16384, should be enough for most cases
        results = self.collection.query(expr=filter_expr, output_fields=["chunk_id"], limit=16384)
        return [r["chunk_id"] for r in results]
