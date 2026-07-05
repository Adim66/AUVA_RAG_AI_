"""One-time setup for benchmarks - ingest v1 corpus"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.cdc_ingest_simple import CDCIngestionPipeline

def main():
    corpus_dir = Path(__file__).parent.parent / "data" / "benchmark_corpus"
    v1_files = sorted(corpus_dir.glob("*_v1.txt"))
    
    # Clean up old data
    import shutil
    lakehouse_dir = Path(__file__).parent.parent / "lakehouse"
    if lakehouse_dir.exists():
        shutil.rmtree(lakehouse_dir)
    
    print("Setting up benchmark data...")
    print(f"Ingesting {len(v1_files)} documents (v1)")
    
    pipeline = CDCIngestionPipeline(reset_milvus=True)
    
    for i, file_path in enumerate(v1_files, 1):
        if i % 20 == 0:
            print(f"  Progress: {i}/{len(v1_files)}")
        with open(file_path, 'r', encoding='utf-8') as f:
            doc_id = Path(file_path).stem.replace('_v1', '')
            pipeline.ingest_document(doc_id, f.read())
    
    stats = pipeline.hash_store.get_stats()
    print(f"\nSetup complete!")
    print(f"  Documents: {stats['total_documents']}")
    print(f"  Chunks: {stats['total_chunks']}")
    print(f"\nReady to run: python tests/benchmark_suite.py")

if __name__ == "__main__":
    main()
