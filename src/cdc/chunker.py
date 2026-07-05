import hashlib
from typing import List, Tuple
from datetime import datetime

def hash_chunk(chunk: str) -> str:
    """Generate SHA-256 hash for a chunk"""
    return hashlib.sha256(chunk.strip().encode()).hexdigest()

def chunk_text(text: str) -> List[str]:
    """Split text into chunks by double newlines or single newlines"""
    chunks = text.split('\n\n') if '\n\n' in text else text.split('\n')
    return [c.strip() for c in chunks if c.strip()]

def create_chunk_record(chunk_hash: str, text: str, doc_id: str, index: int, status: str = "active") -> dict:
    """Create a chunk record with versioning metadata"""
    return {
        "chunk_id": chunk_hash,
        "text": text,
        "doc_id": doc_id,
        "chunk_index": index,
        "valid_from": datetime.utcnow().isoformat() + "Z",
        "valid_to": None,
        "status": status
    }

def detect_version_changes(new_chunks: List[Tuple[str, str]], old_hashes: set) -> Tuple[List[str], List[str]]:
    """Detect new and deleted chunks"""
    new_hashes = {h for h, _ in new_chunks}
    added = list(new_hashes - old_hashes)
    deleted = list(old_hashes - new_hashes)
    return added, deleted

def compare_chunks(new_chunks: List[Tuple[str, str]], old_hashes: set) -> dict:
    """Compare new chunks with stored hashes and categorize changes
    
    Args:
        new_chunks: List of (hash, text) tuples with implicit position (list index)
        old_hashes: Set of previously stored chunk hashes
    
    Returns:
        Dict with keys: added, deleted, unchanged, summary
        Each added/unchanged chunk includes (hash, text, position)
    """
    new_hash_set = {h for h, _ in new_chunks}
    
    added = new_hash_set - old_hashes
    deleted = old_hashes - new_hash_set
    unchanged = new_hash_set & old_hashes
    
    # Create chunk records with position (list index)
    added_chunks = [(h, t, idx) for idx, (h, t) in enumerate(new_chunks) if h in added]
    unchanged_chunks = [(h, t, idx) for idx, (h, t) in enumerate(new_chunks) if h in unchanged]
    
    return {
        'added': added_chunks,
        'deleted': list(deleted),
        'unchanged': unchanged_chunks,
        'summary': {
            'added': len(added),
            'deleted': len(deleted),
            'unchanged': len(unchanged),
            'total_new': len(new_chunks),
            'total_old': len(old_hashes)
        }
    }
