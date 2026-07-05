"""In-memory hash store for CDC comparison"""
from typing import Dict, Set
import json
from pathlib import Path

class HashStore:
    """Maintains active chunk hashes for fast CDC comparison"""
    
    def __init__(self, persist_path: str = "cdc_hash_store.json"):
        self.persist_path = Path(persist_path)
        self.store: Dict[str, Set[str]] = {}  # {doc_id: {chunk_hash1, chunk_hash2, ...}}
        self.load()
    
    def load(self):
        """Load hash store from disk"""
        if self.persist_path.exists():
            try:
                with open(self.persist_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.store = {doc_id: set(hashes) for doc_id, hashes in data.items()}
                print(f"Loaded hash store: {len(self.store)} documents")
            except Exception as e:
                print(f"Warning: Could not load hash store: {e}")
                self.store = {}
    
    def save(self):
        """Save hash store to disk"""
        try:
            data = {doc_id: list(hashes) for doc_id, hashes in self.store.items()}
            with open(self.persist_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save hash store: {e}")
    
    def get_hashes(self, doc_id: str) -> Set[str]:
        """Get stored hashes for a document"""
        return self.store.get(doc_id, set())
    
    def update_hashes(self, doc_id: str, new_hashes: Set[str]):
        """Update stored hashes for a document"""
        self.store[doc_id] = new_hashes
        self.save()
    
    def remove_document(self, doc_id: str):
        """Remove document from hash store"""
        if doc_id in self.store:
            del self.store[doc_id]
            self.save()
    
    def clear(self):
        """Clear all hashes from store and persist"""
        self.store = {}
        self.save()
        print("[HashStore] Cleared all hashes.")


    def get_stats(self) -> Dict:
        """Get hash store statistics"""
        total_chunks = sum(len(hashes) for hashes in self.store.values())
        return {
            'total_documents': len(self.store),
            'total_chunks': total_chunks,
            'avg_chunks_per_doc': total_chunks / len(self.store) if self.store else 0
        }
        
