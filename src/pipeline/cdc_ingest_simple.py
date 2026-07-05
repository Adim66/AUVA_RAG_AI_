"""CDC-aware ingestion pipeline with Delta Lake cold storage"""
from typing import List, Dict
from datetime import datetime
from sentence_transformers import SentenceTransformer
import sys
from pathlib import Path


from lakehouse.delta_store import DeltaStore
sys.path.append(str(Path(__file__).parent.parent))

from cdc.chunker import chunk_text, hash_chunk, create_chunk_record, compare_chunks
from cdc.hash_store import HashStore
from vectordb.milvus_db import MilvusDB
class CDCIngestionPipeline:
    """Ingestion pipeline with CDC support and Delta Lake cold storage"""
    
    def __init__(self, embedding_model: str = "all-MiniLM-L6-v2", reset_milvus: bool = False):
        self.model = SentenceTransformer(embedding_model)
        self.hash_store = HashStore()
        self.milvus = MilvusDB()
        self.delta_store = DeltaStore()
        
        # Initialize Milvus collection if needed
        if reset_milvus:
            self.milvus.connect()
            self.milvus.create_collection()
            self.hash_store.clear()
        
    def ingest_document(self, doc_id: str, content: str, source: str = "file") -> Dict:
        """Ingest document with CDC detection
        
        Args:
            doc_id: Document identifier
            content: Document text content
            source: Source type (file, wikipedia, stackoverflow, etc.)
        """
        # Step 1: Chunk and hash
        text_chunks = chunk_text(content)
        chunk_tuples = [(hash_chunk(chunk), chunk) for chunk in text_chunks]
        
        # Step 2: Compare with stored hashes
        old_hashes = self.hash_store.get_hashes(doc_id)
        cdc_result = compare_chunks(chunk_tuples, old_hashes)
        
        # Step 3: Process added chunks with position
        added_records = []
        added_texts = []
        added_hashes = []
        added_positions = []
        
        timestamp = int(datetime.utcnow().timestamp())
        
        for chunk_hash, chunk_content, position in cdc_result['added']:
            added_texts.append(chunk_content)
            added_hashes.append(chunk_hash)
            added_positions.append(position)
        
        # Step 4: Embed new chunks
        delta_records = []
        if added_texts:
            vectors = self.model.encode(added_texts).tolist()
            
            # Step 5: Insert into Milvus (hot tier - active chunks only)
            self.milvus.connect()
            doc_ids = [doc_id] * len(added_hashes)
            statuses = ['active'] * len(added_hashes)
            valid_from_list = [timestamp] * len(added_hashes)
            valid_to_list = [0] * len(added_hashes)  # 0 means NULL/active
            
            self.milvus.insert(added_hashes, vectors, statuses, doc_ids, added_positions, valid_from_list, valid_to_list, added_texts)
            
            # Step 6: Insert into Delta Lake (cold tier - complete history)
            for chunk_hash, text_content, vector, position in zip(added_hashes, added_texts, vectors, added_positions):
                delta_records.append({
                    'chunk_id': chunk_hash,
                    'content_text': text_content,
                    'content_vector': vector,
                    'doc_id': doc_id,
                    'position': position,
                    'valid_from': timestamp,
                    'valid_to': 0,  # 0 means NULL/active
                    'status': 'active',
                    'version_number': 1,  # Simplified versioning
                    'source': source  # Track source
                })
        
        # Step 7: Handle deleted chunks
        if cdc_result['deleted']:
            # Delete from Milvus (hot tier)
            self.milvus.connect()
            self.milvus.collection.delete(f"chunk_id in {list(cdc_result['deleted'])}")
            self.milvus.collection.flush()
            
            # Mark superseded in Delta Lake (cold tier)
            for chunk_hash in cdc_result['deleted']:
                delta_records.append({
                    'chunk_id': chunk_hash,
                    'content_text': '',
                    'content_vector': [0.0] * 384,
                    'doc_id': doc_id,
                    'position': 0,
                    'valid_from': 0,
                    'valid_to': timestamp,
                    'status': 'superseded',
                    'version_number': 0,
                    'source': source
                })
        
        # Step 8: Write to Delta Lake
        if delta_records:
            self.delta_store.write_chunks(delta_records)
        
        # Step 9: Update hash store
        new_hash_set = {h for h, _ in chunk_tuples}
        self.hash_store.update_hashes(doc_id, new_hash_set) # kol doc 3andou el hashes mte3 el chunks mte3ou et ensuite besh twalli bel broker .
        
        # Step 10: Return CDC summary
        summary = cdc_result['summary']
        summary['doc_id'] = doc_id
        summary['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        
        return summary
    
    def ingest_batch(self, documents: List[Dict]) -> Dict:
        """Ingest multiple documents with batched Delta Lake writes"""
        batch_summary = {
            'total_docs': len(documents),
            'total_added': 0,
            'total_deleted': 0,
            'total_unchanged': 0,
            'doc_summaries': []
        }
        
        all_delta_records = []
        
        for doc in documents:
            # Process without Delta Lake write
            summary = self._ingest_document_no_delta(doc['doc_id'], doc['content'])
            batch_summary['total_added'] += summary['added']
            batch_summary['total_deleted'] += summary['deleted']
            batch_summary['total_unchanged'] += summary['unchanged']
            batch_summary['doc_summaries'].append(summary)
            
            if 'delta_records' in summary:
                all_delta_records.extend(summary['delta_records'])
        
        # Single Delta Lake write for all documents
        if all_delta_records:
            self.delta_store.write_chunks(all_delta_records)
        
        return batch_summary
    
    def _ingest_document_no_delta(self, doc_id: str, content: str, source: str = "file") -> Dict:
        """Ingest document without Delta Lake write (for batching)"""
        text_chunks = chunk_text(content)
        chunk_tuples = [(hash_chunk(chunk), chunk) for chunk in text_chunks]
        old_hashes = self.hash_store.get_hashes(doc_id)
        cdc_result = compare_chunks(chunk_tuples, old_hashes)
        
        added_texts = []
        added_hashes = []
        added_positions = []
        timestamp = int(datetime.utcnow().timestamp())
        
        for chunk_hash, chunk_content, position in cdc_result['added']:
            added_texts.append(chunk_content)
            added_hashes.append(chunk_hash)
            added_positions.append(position)
        
        delta_records = []
        if added_texts:
            vectors = self.model.encode(added_texts).tolist()
            self.milvus.connect()
            doc_ids = [doc_id] * len(added_hashes)
            statuses = ['active'] * len(added_hashes)
            valid_from_list = [timestamp] * len(added_hashes)
            valid_to_list = [0] * len(added_hashes)
            
            self.milvus.insert(added_hashes, vectors, statuses, doc_ids, added_positions, valid_from_list, valid_to_list, added_texts)
            
            for chunk_hash, text_content, vector, position in zip(added_hashes, added_texts, vectors, added_positions):
                delta_records.append({
                    'chunk_id': chunk_hash,
                    'content_text': text_content,
                    'content_vector': vector,
                    'doc_id': doc_id,
                    'position': position,
                    'valid_from': timestamp,
                    'valid_to': 0,
                    'status': 'active',
                    'version_number': 1,
                    'source': source
                })
        
        if cdc_result['deleted']:
            self.milvus.connect()
            self.milvus.collection.delete(f"chunk_id in {list(cdc_result['deleted'])}")
            self.milvus.collection.flush()
            
            for chunk_hash in cdc_result['deleted']:
                delta_records.append({
                    'chunk_id': chunk_hash,
                    'content_text': '',
                    'content_vector': [0.0] * 384,
                    'doc_id': doc_id,
                    'position': 0,
                    'valid_from': 0,
                    'valid_to': timestamp,
                    'status': 'superseded',
                    'version_number': 0,
                    'source': source
                })
        
        new_hash_set = {h for h, _ in chunk_tuples}
        self.hash_store.update_hashes(doc_id, new_hash_set)
        
        summary = cdc_result['summary']
        summary['doc_id'] = doc_id
        summary['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        summary['delta_records'] = delta_records
        
        return summary
    
    def print_summary(self, summary: Dict):
        """Print CDC summary in readable format"""
        print("\n" + "="*60)
        print("CDC INGESTION SUMMARY")
        print("="*60)
        
        if 'total_docs' in summary:
            # Batch summary
            print(f"Documents processed: {summary['total_docs']}")
            print(f"Total chunks added: {summary['total_added']}")
            print(f"Total chunks deleted: {summary['total_deleted']}")
            print(f"Total chunks unchanged: {summary['total_unchanged']}")
        else:
            # Single document summary
            print(f"Document: {summary['doc_id']}")
            print(f"Chunks added: {summary['added']}")
            print(f"Chunks deleted: {summary['deleted']}")
            print(f"Chunks unchanged: {summary['unchanged']}")
            print(f"Total chunks: {summary['total_new']}")
        
        # Hash store stats
        stats = self.hash_store.get_stats()
        print(f"\nHash Store Stats:")
        print(f"  Total documents: {stats['total_documents']}")
        print(f"  Total active chunks: {stats['total_chunks']}")
        print(f"  Avg chunks/doc: {stats['avg_chunks_per_doc']:.1f}")
        print("="*60 + "\n")
