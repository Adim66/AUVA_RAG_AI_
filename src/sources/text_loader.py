"""Text file loader for simulating streaming data ingestion"""
from pathlib import Path
from typing import List, Dict
from datetime import datetime

def load_text_files(directory: str, pattern: str = "*.txt") -> List[Dict]:
    """Load text files from directory
    
    Args:
        directory: Path to directory containing text files
        pattern: File pattern to match (default: *.txt)
    
    Returns:
        List of documents with {doc_id, content, timestamp}
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")
    
    documents = []
    for file_path in dir_path.glob(pattern):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            documents.append({
                'doc_id': file_path.stem,
                'content': content,
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'source_path': str(file_path)
            })
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
    
    return documents

def load_single_file(file_path: str) -> Dict:
    """Load a single text file
    
    Args:
        file_path: Path to text file
    
    Returns:
        Document dict with {doc_id, content, timestamp}
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    return {
        'doc_id': path.stem,
        'content': content,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'source_path': str(path)
    }
