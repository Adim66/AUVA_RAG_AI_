"""Comprehensive test for RAG Query Engine with LLM generation"""
import sys
import io
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / "src"))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from query_engine_generation import RAGQueryEngine
from pipeline.cdc_ingest_simple import CDCIngestionPipeline
from datetime import datetime
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
            'content': '{name :adim boubaker ,job: software engineer ,expertise : machine learning and data science. age : 24}'
        },
        {
            'doc_id': 'test_ml_002',
            'content': '{topic : Machine learning algorithms can be supervised, unsupervised, or reinforcement learning. Deep learning uses neural networks with multiple layers.}'
        },
        {
            'doc_id': 'test_dl_003',
            'content': '{topic : Deep learning has revolutionized computer vision and natural language processing. Neural networks are inspired by biological neurons.}'
        }
    ]

    for doc in test_docs:
        summary = pipeline.ingest_document(doc['doc_id'], doc['content'])
        print(f"Ingested: {doc['doc_id']} - {summary['added']} chunks added")

    print("\nSetup complete. Waiting 2 seconds for indexing...")
    time.sleep(2)
    return True


def test_current_and_historical():
    """
    Single RAGQueryEngine instantiation to test both:
      - PassLLMGenerationHot  (Milvus / hot tier)
      - PassLLMGenerationCold (Delta Lake / cold tier)
    """
    print("\n" + "="*60)
    print("TEST: RAG Generation — Hot & Cold Tiers")
    print("="*60)

    # One single instantiation — LLM loaded once, reused for both tests
    try:
        engine = RAGQueryEngine()
        print("\n[OK] RAGQueryEngine instantiated successfully.")
    except Exception as e:
        print(f"\n[FAILED] Could not instantiate RAGQueryEngine: {e}")
        import traceback
        traceback.print_exc()
        return False

    results = {"hot": False, "cold": False}

    # ------------------------------------------------------------------
    # HOT TIER — PassLLMGenerationHot
    # ------------------------------------------------------------------
    print("\n" + "-"*60)
    print("SUB-TEST A: Hot Tier (Milvus) — PassLLMGenerationHot")
    print("-"*60)

    try:
        query_text = "who is adim boubaker?"
        print(f"\nQuery: '{query_text}'")

        response = engine.PassLLMGenerationHot(query_text, top_k=3, verbose=True)

        print(f"\nAnswer     : {response['answer']}")
        print(f"Model      : {response['model']}")
        print(f"Sources    : {len(response['sources'])} chunk(s) retrieved")
        for i, src in enumerate(response['sources'], 1):
            print(f"  {i}. doc_id={src['doc_id']} | similarity={src['similarity']:.4f}")

        assert response['answer'], "Answer should not be empty"
        assert len(response['sources']) > 0, "Should have at least one source"

        print("\n[PASSED] Hot tier RAG generation working.")
        results["hot"] = True

    except Exception as e:
        print(f"\n[FAILED] Hot tier test error: {e}")
        import traceback
        traceback.print_exc()

    # ------------------------------------------------------------------
    # COLD TIER — PassLLMGenerationCold
    # ------------------------------------------------------------------
    print("\n" + "-"*60)
    print("SUB-TEST B: Cold Tier (Delta Lake) — PassLLMGenerationCold")
    print("-"*60)

    try:
        query_text    = "5+2=? "
        as_of_ts      = int(datetime.now().timestamp())
        print(f"\nQuery : '{query_text}'")
        print(f"As of : {datetime.fromtimestamp(as_of_ts).strftime('%Y-%m-%d %H:%M:%S')}")

        response = engine.PassLLMGenerationCold(query_text, as_of_ts, top_k=3, verbose=True)

        print(f"\nAnswer     : {response['answer']}")
        print(f"Model      : {response['model']}")
        print(f"As of      : {response['as_of']}")
        print(f"Sources    : {len(response['sources'])} chunk(s) retrieved")
        for i, src in enumerate(response['sources'], 1):
            print(f"  {i}. doc_id={src['doc_id']} | similarity={src['similarity']:.4f} | ts={src['timestamp']}")

        assert response['answer'], "Answer should not be empty"
        assert response['as_of'], "as_of field should be present"

        print("\n[PASSED] Cold tier RAG generation working.")
        results["cold"] = True

    except Exception as e:
        print(f"\n[FAILED] Cold tier test error: {e}")
        import traceback
        traceback.print_exc()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "="*60)
    print("SUB-TEST SUMMARY")
    print("="*60)
    print(f"  Hot  tier (PassLLMGenerationHot)  : {'PASSED' if results['hot']  else 'FAILED'}")
    print(f"  Cold tier (PassLLMGenerationCold) : {'PASSED' if results['cold'] else 'FAILED'}")

    return results["hot"] and results["cold"]


def main():
    """Run all RAG query engine tests"""
    print("\n" + "="*60)
    print("RAG QUERY ENGINE - COMPREHENSIVE TEST")
    print("="*60)

    tests_passed = 0
    tests_total  = 1   # one combined test function

    if not setup_test_data():
        print("\nSETUP FAILED: Cannot proceed with tests")
        return

    if test_current_and_historical():
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