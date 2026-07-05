"""Test Delta Lake integration"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))
from sentence_transformers import SentenceTransformer

from lakehouse.delta_store import DeltaStore
import numpy as np

def test_delta_lake():
    """Test Delta Lake storage and retrieval"""
    delta = DeltaStore()
    
    print("="*60)
    print("DELTA LAKE TEST")
    print("="*60)
    
    # Test 1: Read all chunks
    print("\n1. Reading all chunks from Delta Lake...")
    df = delta.read_chunks()
    print(f"   Total chunks stored: {len(df)}")
    
    if not df.is_empty():
        print(f"   Columns: {df.columns}")
        print(f"\n   First 3 rows:")
        for i, row in enumerate(df.head(3).iter_rows(named=True)):
            print(f"   Row {i+1}: chunk_id={row['chunk_id'][:16]}..., doc_id={row['doc_id']}, status={row['status']}")
    
    # Test 2: Get statistics
    print("\n2. Delta Lake Statistics:")
    stats = delta.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
            

    # Test 3: Query by document
    print("\n3. Querying chunks for article_001...")
    doc_chunks = delta.get_history("article_001")
    print(f"   Chunks found: {len(doc_chunks)}")
    
    # Test 4: Time-travel query (get current state)
    print("\n4. Time-travel query (current timestamp)...")
    import time
    current_time = int(time.time())
    current_chunks = delta.time_travel_query(current_time)
    print(f"   Active chunks at current time: {len(current_chunks)}")
    
    # Test 5: Similarity search (if we have data)
    if not df.is_empty() and 'content_vector' in df.columns:
        print("\n5. Testing similarity search...")
        # Create a random query vector
        model = SentenceTransformer("all-MiniLM-L6-v2")
        
        query_vector = model.encode("What are the new Quantum Computing breakthroughs announced?")
        query_vector = query_vector / np.linalg.norm(query_vector)
        
        results = delta.similarity_search(query_vector, top_k=3)
        print(f"   Top 3 similar chunks:")
        for i, result in enumerate(results, 1):
            content_preview = result['content'][:60].replace('\n', ' ')
            print(f"   {i}. Doc: {result['doc_id']}, Similarity: {result['similarity']:.4f}")
            print(f"      Preview: {content_preview}...")
    
    print("\n" + "="*60)
    print("Delta Lake integration working successfully!")
    print("="*60)

if __name__ == "__main__":
    test_delta_lake()
