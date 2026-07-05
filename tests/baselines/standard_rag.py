"""Standard RAG baseline - no versioning, full re-index on updates"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from typing import List, Dict
from sentence_transformers import SentenceTransformer
from pymilvus import connections, Collection, utility
from vectordb.milvus_db import MilvusDB
from cdc.chunker import chunk_text

class StandardRAG:
    """Baseline: Standard RAG without versioning"""
    
    def __init__(self, reset: bool = False):
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.milvus = MilvusDB(collection_name="standard_rag")
        self.documents = {}
        
        if reset:
            self.milvus.connect()
            self.milvus.create_collection()
        
    def ingest_document(self, doc_id: str, content: str):
        """Ingest/update document - full re-index"""
        if doc_id in self.documents:
            self._delete_document(doc_id)
        
        chunks = chunk_text(content)
        vectors = self.model.encode(chunks).tolist()
        
        self.milvus.connect()
        chunk_ids = [f"{doc_id}_{i}" for i in range(len(chunks))]
        doc_ids = [doc_id] * len(chunks)
        statuses = ['active'] * len(chunks)
        positions = list(range(len(chunks)))
        valid_from = [0] * len(chunks)
        valid_to = [0] * len(chunks)
        contents = chunks
        
        self.milvus.insert(chunk_ids, vectors, statuses, doc_ids, positions, valid_from, valid_to, contents)
        self.documents[doc_id] = {'chunks': len(chunks)}
        
    def _delete_document(self, doc_id: str):
        """Delete all chunks for a document"""
        if not self.milvus.collection:
            self.milvus.collection = Collection(self.milvus.collection_name)
        self.milvus.collection.delete(f"doc_id == '{doc_id}'")
        
    def query(self, query_text: str, top_k: int = 5) -> List[Dict]:
        """Query current documents"""
        query_vector = self.model.encode(query_text).tolist()
        self.milvus.connect()
        results = self.milvus.search(query_vector, limit=top_k, filter_expr="")
        return results
