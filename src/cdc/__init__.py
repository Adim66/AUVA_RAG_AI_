"""CDC (Change Data Capture) module for hash-based versioning"""
from .chunker import chunk_text, hash_chunk, compare_chunks
from .hash_store import HashStore
from .pdf_parser import extract_text

__all__ = ['chunk_text', 'hash_chunk', 'compare_chunks', 'HashStore', 'extract_text']
