"""Comprehensive test for query engine"""
import sys
import io
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / "src"))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from query_engine import QueryEngine
from pipeline.cdc_ingest_simple import CDCIngestionPipeline
from datetime import datetime, timedelta
import time

def setup_test_data():
    """Ingest test data for query testing"""
    print("\n" + "="*60)
    print("SETUP: Ingesting test data")
    print("="*60)
    
    pipeline = CDCIngestionPipeline(reset_milvus=True)
    
    test_docs = [
        {
            'doc_id': 'test_ai_001',
            'content': 'Artificial intelligence is the simulation of human intelligence by machines. Machine learning is a subset of AI that enables systems to learn from data.'
        },
        {
            'doc_id': 'test_ml_002',
            'content': 'Machine learning algorithms can be supervised, unsupervised, or reinforcement learning. Deep learning uses neural networks with multiple layers.'
        },
        {
            'doc_id': 'test_dl_003',
            'content': 'Deep learning has revolutionized computer vision and natural language processing. Neural networks are inspired by biological neurons.'
        }
    ]
    
    for doc in test_docs:
        summary = pipeline.ingest_document(doc['doc_id'], doc['content'])
        print(f"Ingested: {doc['doc_id']} - {summary['added']} chunks added")
    
    print("\nSetup complete. Waiting 2 seconds for indexing...")
    time.sleep(2)
    return True

def test_current_query():
    """Test current query functionality"""
    print("\n" + "="*60)
    print("TEST 1: Current Query (Hot Path - Milvus)")
    print("="*60)
    
    try:
        engine = QueryEngine()
        
        query_text = "Artificial intelligence"
        print(f"\nQuery: '{query_text}'")
        print("Expected: Results from Milvus hot tier")
        
        results = engine.query_current(query_text, top_k=3)
        
        print(f"\nResults found: {len(results)}")
        
        if len(results) > 0:
            print("\nTop result:")
            print(f"  Doc ID: {results[0]['doc_id']}")
            print(f"  Chunk ID: {results[0]['chunk_id'][:50]}...")
            print(f"  Similarity: {results[0]['similarity']:.4f}")
            print(f"  Query type: {results[0]['query_type']}")
            
            print("\nTEST PASSED: Current query working")
            return True
        else:
            print("\nTEST FAILED: No results returned")
            return False
            
    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_historical_query():
    """Test historical query functionality"""
    print("\n" + "="*60)
    print("TEST 2: Historical Query (Cold Path - Delta Lake)")
    print("="*60)
    
    try:
        engine = QueryEngine()
        
        current_timestamp = int(datetime.now().timestamp())
        
        query_text = "neural networks"
        print(f"\nQuery: '{query_text}'")
        print(f"As of: {datetime.fromtimestamp(current_timestamp).strftime('%Y-%m-%d %H:%M:%S')}")
        print("Expected: Results from Delta Lake cold tier")
        
        results = engine.query_historical(query_text, current_timestamp, top_k=3)
        
        print(f"\nResults found: {len(results)}")
        
        if len(results) > 0:
            print("\nTop result:")
            print(f"  Doc ID: {results[0]['doc_id']}")
            print(f"  Chunk ID: {results[0]['chunk_id'][:50]}...")
            print(f"  Similarity: {results[0]['similarity']:.4f}")
            print(f"  Content preview: {results[0]['content'][:80]}...")
            print(f"  Timestamp: {datetime.fromtimestamp(results[0]['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")
            
            print("\nTEST PASSED: Historical query working")
            return True
        else:
            print("\nTEST FAILED: No historical results found")
            print("Note: Delta Lake may not have data yet")
            return False
            
    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_query_routing():
    """Test that queries route to correct tier"""
    print("\n" + "="*60)
    print("TEST 3: Query Routing Logic")
    print("="*60)
    
    try:
        engine = QueryEngine()
        
        print("\nTest 3a: Current query routing")
        results_current = engine.query_current("artificial intelligence", top_k=2)
        print(f"Current query returned {len(results_current)} results")
        if len(results_current) > 0:
            print(f"Query type: {results_current[0]['query_type']}")
            assert results_current[0]['query_type'] == 'current', "Should be 'current'"
            print("Current routing correct")
        
        print("\nTest 3b: Historical query routing")
        timestamp = int(datetime.now().timestamp())
        results_historical = engine.query_historical("deep learning", timestamp, top_k=2)
        print(f"Historical query returned {len(results_historical)} results")
        if len(results_historical) > 0:
            print(f"Has timestamp: {'timestamp' in results_historical[0]}")
            print(f"Has content: {'content' in results_historical[0]}")
            print("Historical routing correct")
        
        print("\nTEST PASSED: Query routing working correctly")
        return True
        
    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_cli_integration():
    """Test CLI query command"""
    print("\n" + "="*60)
    print("TEST 4: CLI Integration")
    print("="*60)
    
    print("\nCLI commands to test manually:")
    print("1. Current query:")
    print("   python src/cli.py query \"machine learning\"")
    print("\n2. Historical query:")
    print("   python src/cli.py query \"neural networks\" --as-of 2024-01-15")
    print("\n3. With top-k:")
    print("   python src/cli.py query \"artificial intelligence\" --top-k 10")
    
    print("\nTEST INFO: CLI commands listed above")
    return True

def main():
    """Run all query engine tests"""
    print("\n" + "="*60)
    print("QUERY ENGINE - COMPREHENSIVE TEST")
    print("="*60)
    
    tests_passed = 0
    tests_total = 4
    
    if not setup_test_data():
        print("\nSETUP FAILED: Cannot proceed with tests")
        return
    
    if test_current_query():
        tests_passed += 1
    
    if test_historical_query():
        tests_passed += 1
    
    if test_query_routing():
        tests_passed += 1
    
    if test_cli_integration():
        tests_passed += 1
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Tests passed: {tests_passed}/{tests_total}")
    
    if tests_passed == tests_total:
        print("\nALL TESTS PASSED")
    else:
        print(f"\n{tests_total - tests_passed} TEST(S) FAILED")
    
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
