# LiveVectorLake Benchmarks

Research-grade evaluation suite for IEEE publication.

---

## Overview

Five core benchmarks validating system contributions:

### 1. CDC Efficiency & Granularity
**What**: Compares chunk-level CDC against full document re-indexing baseline  
**Why**: Validates that fine-grained change detection avoids unnecessary re-processing  
**Measures**: Reprocessing percentage, update time, speedup vs Naive RAG

### 2. Storage Efficiency & Compression
**What**: Analyzes dual-tier storage overhead (hot Milvus + cold Delta Lake)  
**Why**: Quantifies cost of maintaining complete version history  
**Measures**: Hot/cold tier sizes, compression ratio, total overhead vs current-only baseline

### 3. Temporal Query Accuracy
**What**: Validates historical queries return correct version without temporal leakage  
**Why**: Ensures "as-of" queries retrieve knowledge as it existed at specific dates  
**Measures**: Temporal precision, version leakage rate, query latency

### 4. ACID Consistency
**What**: Verifies Milvus and Delta Lake maintain synchronized state  
**Why**: Confirms ACID guarantees across heterogeneous storage tiers  
**Measures**: Active chunk consistency between hot and cold tiers

### 5. CDC Detection Accuracy
**What**: Tests hash-based change detection with edge cases  
**Why**: Validates SHA-256 hashing correctly identifies identical vs modified content  
**Measures**: Detection accuracy on exact matches, typos, paraphrasing

---

## Prerequisites

### 1. Milvus Running

```bash
docker-compose up -d
docker ps | grep milvus
```

### 2. Benchmark Corpus Generated

```bash
python tests/generate_versioned_corpus.py
```

Expected output: `data/benchmark_corpus/` with 100 docs × 5 versions

### 3. Initial Ingestion

```bash
python src/cli.py ingest data/benchmark_corpus --reset
```

---

## Running Benchmarks

### Full Suite

```bash
python tests/benchmark_suite.py
```

Expected time: 15-20 minutes

### Individual Benchmarks

Not recommended. Run full suite for consistent results.

---

## Output

### Terminal Output

```
LiveVectorLake Benchmark Suite
============================================================
Corpus: 100 documents
Started: 2024-11-03 10:30:00

[1/5] CDC Efficiency & Granularity
  Baseline: Naive RAG (full re-index)
    Ingesting v1... 20/100 40/100 60/100 80/100 100/100 done
    Updating to v2 (full re-index)... 20/100 40/100 60/100 80/100 100/100 303.45s
  LiveVectorLake: Chunk-level CDC
    Ingesting v1... 20/100 40/100 60/100 80/100 100/100 1200 chunks
    Updating to v2 (CDC)... 20/100 40/100 60/100 80/100 100/100 7.28s
  Result: 2.4% re-processed vs 100% baseline
          41.7x speedup (7.28s vs 303.45s)

[2/5] Storage Efficiency & Compression
  Analyzing storage... done
  Hot tier: 1.2MB (1200 chunks)
  Cold tier: 2.3MB (6000 chunks, 1.8x compressed)
  Total: 3.5MB (2.9x overhead vs no-history baseline)

[3/5] Temporal Query Accuracy
  Generating test queries... 30 queries
  Running queries... 10/30 20/30 30/30 done
  Accuracy: 96.0%
  Leakage: 0.0%
  Latency: 437ms avg

[4/5] ACID Guarantees & Consistency
  Verifying cross-tier consistency... done
  Milvus chunks: 1200
  Delta Lake active chunks: 1200
  Consistent: Yes

[5/5] CDC Detection Accuracy
  Testing hash-based detection... done
  Test cases: 5
  Correct: 5
  Accuracy: 100.0%

============================================================
Completed in 1234.5s
============================================================

Summary
------------------------------------------------------------
CDC Efficiency:     41.7x speedup
                    97.6% work avoided
Storage Overhead:   2.9x
Compression Ratio:  1.8x
Temporal Accuracy:  96.0%
Version Leakage:    0.0%
ACID Consistency:   Pass
CDC Detection:      100.0%

Results saved: tests/results/benchmark_suite_20241103_103000.json
```

### JSON Results

Saved to `tests/results/benchmark_suite_TIMESTAMP.json`

```json
{
  "timestamp": "2024-11-03T10:30:00",
  "corpus_size": 100,
  "total_time_seconds": 1234.5,
  "benchmarks": {
    "cdc_efficiency": {
      "naive_rag_time_seconds": 303.45,
      "livevectorlake_time_seconds": 7.28,
      "speedup": 41.7,
      "savings_percent": 97.6
    },
    "storage_efficiency": {
      "hot_tier_mb": 1.2,
      "cold_tier_mb": 2.3,
      "total_mb": 3.5,
      "storage_overhead": 2.9,
      "compression_ratio": 1.8
    },
    "temporal_accuracy": {
      "accuracy_percent": 96.0,
      "version_leakage_percent": 0.0,
      "avg_latency_ms": 437
    },
    "acid_consistency": {
      "consistent": true
    },
    "cdc_detection": {
      "accuracy_percent": 100.0
    }
  }
}
```

---

## Expected Results

| Benchmark | Metric | Target | Typical |
|-----------|--------|--------|---------|
| CDC Efficiency | Speedup | >10x | 40-50x |
| | Work avoided | >90% | 95-98% |
| Storage | Overhead | <5x | 2-3x |
| | Compression | >1.5x | 1.5-2x |
| Temporal | Accuracy | >95% | 95-100% |
| | Leakage | 0% | 0% |
| | Latency | <2s | 400-600ms |
| ACID | Consistency | Pass | Pass |
| CDC Detection | Accuracy | 100% | 100% |

---

## Troubleshooting

### Milvus Connection Error

```bash
docker-compose restart
docker ps | grep milvus
```

### Corpus Not Found

```bash
python tests/generate_versioned_corpus.py
ls data/benchmark_corpus/*.txt | wc -l  # Should be 500
```

### Out of Memory

Reduce corpus size in `generate_versioned_corpus.py`:
```python
NUM_DOCS = 50  # Instead of 100
```

### Slow Performance

Expected on CPU. For faster results:
- Use GPU for embeddings
- Reduce corpus size
- Use smaller embedding model

---

## For Paper

### Evaluation Section

Use results from `benchmark_suite_TIMESTAMP.json` to populate:

**Table 1: CDC Efficiency**
| System | Reprocessing | Update Time | Speedup |
|--------|--------------|-------------|---------|
| Naive RAG | 100% | 303.45s | 1x |
| LiveVectorLake | 2.4% | 7.28s | 41.7x |

**Table 2: Storage Efficiency**
| Component | Size | Overhead |
|-----------|------|----------|
| Hot tier (current) | 1.2MB | 1x |
| Cold tier (history) | 2.3MB | 1.9x |
| Total | 3.5MB | 2.9x |

**Table 3: Temporal Query Accuracy**
| Metric | Value |
|--------|-------|
| Temporal precision | 96.0% |
| Version leakage | 0.0% |
| Query latency | 437ms |

**Table 4: System Validation**
| Test | Result |
|------|--------|
| ACID consistency | Pass |
| CDC detection | 100% |

---

## Notes

- Benchmarks use actual system implementation (no mocks)
- Results are deterministic (same corpus → same results)
- All metrics are quantitative and reproducible
- Baselines are fair (same hardware, same corpus)

---

**Last Updated**: 2024-11-03  
**Status**: Production-ready
