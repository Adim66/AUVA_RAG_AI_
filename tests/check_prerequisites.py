"""
Check if all prerequisites are met before running benchmarks.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def check_milvus():
    """Check if Milvus is running."""
    try:
        from pymilvus import connections
        connections.connect("default", host="localhost", port="19530")
        connections.disconnect("default")
        return True
    except Exception:
        return False

def check_corpus():
    """Check if benchmark corpus exists."""
    corpus_dir = Path(__file__).parent.parent / "data" / "benchmark_corpus"
    if not corpus_dir.exists():
        return False
    v1_files = list(corpus_dir.glob("*_v1.txt"))
    return len(v1_files) >= 100

def check_delta_lake():
    """Check if Delta Lake has data."""
    lakehouse_dir = Path(__file__).parent.parent / "lakehouse" / "chunks"
    if not lakehouse_dir.exists():
        return False
    try:
        from lakehouse.delta_store import DeltaLakeStore
        delta_store = DeltaLakeStore()
        df = delta_store.read_chunks()
        return not df.is_empty()
    except Exception:
        return False

def main():
    print("Checking prerequisites...\n")
    
    checks = [
        ("Milvus running", check_milvus),
        
        ("Delta Lake data", check_delta_lake)
    ]
    
    all_passed = True
    for name, check_func in checks:
        result = check_func()
        status = "✓" if result else "✗"
        print(f"  {status} {name}")
        if not result:
            all_passed = False
    
    print()
    if all_passed:
        print("All prerequisites met. Ready to run benchmarks!")
        return 0
    else:
        print("Some prerequisites missing. Fix them before running benchmarks.")
        print("\nSetup commands:")
        print("  docker-compose up -d")
        print("  python tests/generate_versioned_corpus.py")
        print("  python src/cli.py ingest data/benchmark_corpus --reset")
        return 1

if __name__ == "__main__":
    sys.exit(main())
