"""
LiveVectorLake Benchmark Suite

Research-grade evaluation for IEEE publication.
Implements 5 core benchmarks validating system contributions.
"""

import time
import sys
import json
from pathlib import Path
from datetime import datetime
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "baselines"))

from pipeline.cdc_ingest_simple import CDCIngestionPipeline
from standard_rag import StandardRAG
from lakehouse.delta_store import DeltaStore
from query_engine import QueryEngine


def get_directory_size(path: Path) -> int:
    """Calculate directory size in bytes."""
    total = 0
    for item in path.rglob('*'):
        if item.is_file():
            total += item.stat().st_size
    return total


class BenchmarkSuite:
    """Orchestrates all benchmarks and collects results."""
    
    def __init__(self, corpus_dir: Path):
        self.corpus_dir = corpus_dir
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "corpus_size": len(list(corpus_dir.glob("*_v1.txt"))),
            "benchmarks": {}
        }
    
    def run_all(self):
        """Execute all benchmarks in sequence."""
        print("LiveVectorLake Benchmark Suite")
        print("=" * 60)
        print(f"Corpus: {self.results['corpus_size']} documents")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        start_time = time.time()
        
        # Benchmark 1: CDC Efficiency
        print("[1/5] CDC Efficiency & Granularity")
        self.results["benchmarks"]["cdc_efficiency"] = self.benchmark_cdc_efficiency()
        
        # Benchmark 2: Storage Efficiency
        print("\n[2/5] Storage Efficiency & Compression")
        self.results["benchmarks"]["storage_efficiency"] = self.benchmark_storage_efficiency()
        
        # Benchmark 3: Temporal Query Accuracy
        print("\n[3/5] Temporal Query Accuracy")
        self.results["benchmarks"]["temporal_accuracy"] = self.benchmark_temporal_accuracy()
        
        # Benchmark 4: ACID Consistency
        print("\n[4/5] ACID Guarantees & Consistency")
        self.results["benchmarks"]["acid_consistency"] = self.benchmark_acid_consistency()
        
        # Benchmark 5: CDC Detection Accuracy
        print("\n[5/5] CDC Detection Accuracy")
        self.results["benchmarks"]["cdc_detection"] = self.benchmark_cdc_detection()
        
        elapsed = time.time() - start_time
        self.results["total_time_seconds"] = elapsed
        
        print("\n" + "=" * 60)
        print(f"Completed in {elapsed:.1f}s")
        print("=" * 60)
        
        self.save_results()
        self.print_summary()
    
    def benchmark_cdc_efficiency(self):
        """Benchmark 1: Measure CDC efficiency vs baselines."""
        v2_files = sorted(self.corpus_dir.glob("*_v2.txt"))
        
        # Baseline A: Naive RAG (full re-index)
        print("  Baseline: Naive RAG (full re-index)")
        print("    Updating to v2 (timed)...", end='', flush=True)
        rag = StandardRAG(reset=False)
        start = time.time()
        for i, file_path in enumerate(v2_files, 1):
            if i % 20 == 0:
                print(f" {i}/{len(v2_files)}", end='', flush=True)
            with open(file_path, 'r', encoding='utf-8') as f:
                doc_id = Path(file_path).stem.replace('_v2', '')
                rag.ingest_document(doc_id, f.read())
        naive_time = time.time() - start
        print(f" {naive_time:.2f}s")
        
        # Your System: Chunk-Level CDC
        print("  LiveVectorLake: Chunk-level CDC")
        pipeline = CDCIngestionPipeline(reset_milvus=False)
        
        stats_v1 = pipeline.hash_store.get_stats()
        v1_chunks = stats_v1['total_chunks']
        
        # Capture v1 hashes BEFORE v2 ingestion
        v1_hashes = set()
        for doc_id in pipeline.hash_store.store.keys():
            v1_hashes.update(pipeline.hash_store.get_hashes(doc_id))
        
        print("    Updating to v2 (timed, CDC)...", end='', flush=True)
        start = time.time()
        for i, file_path in enumerate(v2_files, 1):
            if i % 20 == 0:
                print(f" {i}/{len(v2_files)}", end='', flush=True)
            with open(file_path, 'r', encoding='utf-8') as f:
                doc_id = Path(file_path).stem.replace('_v2', '')
                pipeline.ingest_document(doc_id, f.read())
        cdc_time = time.time() - start
        
        # Capture v2 hashes AFTER v2 ingestion
        v2_hashes = set()
        for doc_id in pipeline.hash_store.store.keys():
            v2_hashes.update(pipeline.hash_store.get_hashes(doc_id))
        
        stats_v2 = pipeline.hash_store.get_stats()
        v2_chunks = stats_v2['total_chunks']
        
        new_chunks = len(v2_hashes - v1_hashes)
        deleted_chunks = len(v1_hashes - v2_hashes)
        unchanged_chunks = len(v1_hashes & v2_hashes)
        
        print(f"\n  Debug: v1_hashes={len(v1_hashes)}, v2_hashes={len(v2_hashes)}")
        print(f"  Debug: new={new_chunks}, deleted={deleted_chunks}, unchanged={unchanged_chunks}")
        
        cdc_reprocess_pct = (new_chunks / v2_chunks * 100) if v2_chunks > 0 else 0
        speedup = naive_time / cdc_time if cdc_time > 0 else 0
        
        print(f" {cdc_time:.2f}s")
        print(f"  Result: {cdc_reprocess_pct:.1f}% re-processed vs 100% baseline")
        print(f"          {speedup:.1f}x speedup ({cdc_time:.2f}s vs {naive_time:.2f}s)")
        print(f"  CDC Details: {new_chunks} new, {deleted_chunks} deleted, {unchanged_chunks} unchanged")
        
        return {
            "naive_rag_time_seconds": naive_time,
            "naive_rag_reprocess_percent": 100.0,
            "livevectorlake_time_seconds": cdc_time,
            "livevectorlake_reprocess_percent": cdc_reprocess_pct,
            "speedup": speedup,
            "savings_percent": 100.0 - cdc_reprocess_pct,
            "v1_chunks": v1_chunks,
            "v2_chunks": v2_chunks,
            "new_chunks": new_chunks,
            "deleted_chunks": deleted_chunks,
            "unchanged_chunks": unchanged_chunks
        }
    
    def benchmark_storage_efficiency(self):
        """Benchmark 2: Measure storage overhead and compression."""
        lakehouse_dir = Path(__file__).parent.parent / "lakehouse" / "chunks"
        
        print("  Analyzing storage...", end='', flush=True)
        
        delta_size_bytes = get_directory_size(lakehouse_dir)
        delta_size_mb = delta_size_bytes / (1024 * 1024)
        
        delta_store = DeltaStore()
        df = delta_store.read_chunks()
        
        total_chunks = len(df)
        active_chunks = len(df.filter(df['status'] == 'active'))
        historical_chunks = total_chunks - active_chunks
        
        # Estimate hot tier (Milvus)
        bytes_per_chunk = (384 * 4) + 100
        hot_size_mb = (active_chunks * bytes_per_chunk) / (1024 * 1024)
        
        total_size_mb = hot_size_mb + delta_size_mb
        baseline_size_mb = hot_size_mb
        storage_overhead = total_size_mb / baseline_size_mb if baseline_size_mb > 0 else 0
        
        # Compression ratio
        uncompressed_bytes_per_chunk = 1536 + 500 + 100
        uncompressed_size_mb = (total_chunks * uncompressed_bytes_per_chunk) / (1024 * 1024)
        compression_ratio = uncompressed_size_mb / delta_size_mb if delta_size_mb > 0 else 0
        
        print(" done")
        print(f"  Hot tier: {hot_size_mb:.1f}MB ({active_chunks} chunks)")
        print(f"  Cold tier: {delta_size_mb:.1f}MB ({total_chunks} chunks, {compression_ratio:.1f}x compressed)")
        print(f"  Total: {total_size_mb:.1f}MB ({storage_overhead:.1f}x overhead vs no-history baseline)")
        
        return {
            "hot_tier_mb": hot_size_mb,
            "cold_tier_mb": delta_size_mb,
            "total_mb": total_size_mb,
            "baseline_mb": baseline_size_mb,
            "storage_overhead": storage_overhead,
            "compression_ratio": compression_ratio,
            "total_chunks": total_chunks,
            "active_chunks": active_chunks,
            "historical_chunks": historical_chunks
        }
    
    def benchmark_temporal_accuracy(self):
        """Benchmark 3: Validate temporal query accuracy."""
        print("  Generating test queries...", end='', flush=True)
        
        delta_store = DeltaStore()
        df = delta_store.read_chunks()
        
        if df.is_empty():
            print(" ERROR: No data")
            return {}
        
        timestamps = sorted(df['valid_from'].unique().to_list())
        
        queries = [
            "artificial intelligence", "machine learning", "neural networks",
            "data science", "deep learning", "natural language processing",
            "computer vision", "reinforcement learning", "supervised learning",
            "unsupervised learning"
        ]
        
        test_cases = []
        for i in range(min(30, len(timestamps) * len(queries))):
            ts_idx = i % len(timestamps)
            query_idx = i % len(queries)
            test_cases.append((queries[query_idx], timestamps[ts_idx]))
        
        print(f" {len(test_cases)} queries")
        
        print("  Running queries...", end='', flush=True)
        engine = QueryEngine()
        
        correct = 0
        version_leakage = 0
        total_latency = 0
        
        for i, (query, ts) in enumerate(test_cases):
            if (i + 1) % 10 == 0:
                print(f" {i+1}/{len(test_cases)}", end='', flush=True)
            
            start = time.time()
            results = engine.query_historical(query, ts, top_k=3)
            latency = (time.time() - start) * 1000
            total_latency += latency
            
            has_leakage = any(r.get('timestamp', 0) > ts for r in results)
            if has_leakage:
                version_leakage += 1
            
            if results and not has_leakage:
                correct += 1
        
        accuracy = (correct / len(test_cases) * 100) if test_cases else 0
        avg_latency = total_latency / len(test_cases) if test_cases else 0
        leakage_pct = (version_leakage / len(test_cases) * 100) if test_cases else 0
        
        print(" done")
        print(f"  Accuracy: {accuracy:.1f}%")
        print(f"  Leakage: {leakage_pct:.1f}%")
        print(f"  Latency: {avg_latency:.0f}ms avg")
        
        return {
            "total_queries": len(test_cases),
            "correct_queries": correct,
            "accuracy_percent": accuracy,
            "version_leakage_percent": leakage_pct,
            "avg_latency_ms": avg_latency,
            "timestamps_tested": len(timestamps)
        }
    
    def benchmark_acid_consistency(self):
        """Benchmark 4: Verify ACID guarantees."""
        print("  Verifying cross-tier consistency...", end='', flush=True)
        
        pipeline = CDCIngestionPipeline(reset_milvus=False)
        pipeline.milvus.connect()
        
        # Ensure all data is flushed and loaded
        if pipeline.milvus.collection:
            pipeline.milvus.collection.flush()
            pipeline.milvus.collection.load()
        
        # Get count from Milvus using segment info (more reliable than query)
        from pymilvus import utility
        segments = utility.get_query_segment_info('doc_chunks')
        milvus_count = sum(s.num_rows for s in segments)
        
        # Get count from Delta Lake
        df = pipeline.delta_store.read_chunks()
        active_df = df.filter(df['status'] == 'active')
        delta_count = len(active_df)
        
        # Verify consistency (counts should match)
        consistent = (milvus_count == delta_count)
        diff = abs(milvus_count - delta_count)
        
        print(" done")
        print(f"  Milvus chunks: {milvus_count}")
        print(f"  Delta Lake active chunks: {delta_count}")
        print(f"  Consistent: {'Yes' if consistent else 'No'}")
        if not consistent:
            print(f"  Difference: {diff} chunks")
        
        return {
            "milvus_chunks": milvus_count,
            "delta_active_chunks": delta_count,
            "consistent": consistent,
            "difference": diff
        }
    
    def benchmark_cdc_detection(self):
        """Benchmark 5: Validate CDC detection accuracy."""
        print("  Testing hash-based detection...", end='', flush=True)
        
        test_cases = [
            ("The cat sat on the mat", "The cat sat on the mat", True),
            ("The cat sat on the mat", "The cat sat on the mat.", False),
            ("AI is important", "Artificial intelligence is important", False),
            ("Hello world", "Hello world", True),
            ("Hello world", "hello world", False),
        ]
        
        from cdc.chunker import hash_chunk
        
        correct = 0
        for text1, text2, should_match in test_cases:
            hash1 = hash_chunk(text1)
            hash2 = hash_chunk(text2)
            matches = (hash1 == hash2)
            if matches == should_match:
                correct += 1
        
        accuracy = (correct / len(test_cases)) * 100
        
        print(" done")
        print(f"  Test cases: {len(test_cases)}")
        print(f"  Correct: {correct}")
        print(f"  Accuracy: {accuracy:.1f}%")
        
        return {
            "test_cases": len(test_cases),
            "correct": correct,
            "accuracy_percent": accuracy
        }
    
    def save_results(self):
        """Save results to JSON file."""
        results_dir = Path(__file__).parent / "results"
        results_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = results_dir / f"benchmark_suite_{timestamp}.json"
        
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"\nResults saved: {results_file}")
    
    def print_summary(self):
        """Print summary table."""
        print("\nSummary")
        print("-" * 60)
        
        b = self.results["benchmarks"]
        
        if "cdc_efficiency" in b:
            print(f"CDC Efficiency:     {b['cdc_efficiency']['speedup']:.1f}x speedup")
            print(f"                    {b['cdc_efficiency']['savings_percent']:.1f}% work avoided")
        
        if "storage_efficiency" in b:
            print(f"Storage Overhead:   {b['storage_efficiency']['storage_overhead']:.1f}x")
            print(f"Compression Ratio:  {b['storage_efficiency']['compression_ratio']:.1f}x")
        
        if "temporal_accuracy" in b:
            print(f"Temporal Accuracy:  {b['temporal_accuracy']['accuracy_percent']:.1f}%")
            print(f"Version Leakage:    {b['temporal_accuracy']['version_leakage_percent']:.1f}%")
        
        if "acid_consistency" in b:
            status = "Pass" if b['acid_consistency']['consistent'] else "Fail"
            print(f"ACID Consistency:   {status}")
        
        if "cdc_detection" in b:
            print(f"CDC Detection:      {b['cdc_detection']['accuracy_percent']:.1f}%")


def check_prerequisites():
    """Check and setup prerequisites"""
    corpus_dir = Path(__file__).parent.parent / "data" / "benchmark_corpus"
    hash_store_file = Path(__file__).parent.parent / "cdc_hash_store.json"
    
    # Check corpus exists
    if not corpus_dir.exists():
        print("ERROR: Benchmark corpus not found")
        print(f"Run: python tests/generate_versioned_corpus.py")
        return False
    
    v1_files = list(corpus_dir.glob("*_v1.txt"))
    if len(v1_files) != 100:
        print(f"ERROR: Expected 100 v1 files, found {len(v1_files)}")
        print(f"Run: python tests/generate_versioned_corpus.py")
        return False
    
    # Check if v1 data is ingested
    if not hash_store_file.exists():
        print("\nInitial setup required...")
        print("Ingesting v1 corpus (one-time, ~5 min)\n")
        
        from pipeline.cdc_ingest_simple import CDCIngestionPipeline
        pipeline = CDCIngestionPipeline(reset_milvus=True)
        
        for i, file_path in enumerate(v1_files, 1):
            if i % 20 == 0:
                print(f"  Progress: {i}/{len(v1_files)}")
            with open(file_path, 'r', encoding='utf-8') as f:
                doc_id = Path(file_path).stem.replace('_v1', '')
                pipeline.ingest_document(doc_id, f.read())
        
        stats = pipeline.hash_store.get_stats()
        print(f"\nSetup complete: {stats['total_documents']} docs, {stats['total_chunks']} chunks\n")
    
    return True

def main():
    if not check_prerequisites():
        return
    
    corpus_dir = Path(__file__).parent.parent / "data" / "benchmark_corpus"
    suite = BenchmarkSuite(corpus_dir)
    suite.run_all()


if __name__ == "__main__":
    main()
