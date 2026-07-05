# LiveVectorLake

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **A real-time versioned knowledge base for streaming vector updates and temporal retrieval**

## Overview

LiveVectorLake is a dual-tier temporal knowledge base architecture that enables real-time semantic search on current knowledge while maintaining complete version history for compliance, auditability, and point-in-time retrieval.

**Scope**: LiveVectorLake assumes documents come from an existing stream (e.g., Kafka, webhooks, polling). We focus on CDC-based ingestion, dual-tier storage, and temporal queries. Source connectors (SharePoint, Confluence APIs) are out of scope. For demonstration, we simulate streams using local files.

### The Problem

Modern RAG systems face a fundamental architectural tension:

1. **Expensive Re-indexing**: Vector indices are optimized for query latency but poorly handle continuous updates. Changing one paragraph requires re-embedding entire documents (100% re-processing).
2. **No Temporal Queries**: Cannot answer "What did the policy say 6 months ago?" or reconstruct historical knowledge states.
3. **No Audit Trail**: Cannot prove what information was available at specific points in time for compliance or debugging.

### Solution: Dual-Tier Temporal Architecture

LiveVectorLake separates current knowledge (hot tier) from historical versions (cold tier):

- **Chunk-Level CDC**: SHA-256 content addressing for deterministic change detection (10-15% re-processing vs 100%)
- **Hot Tier**: Milvus with HNSW indexing for current knowledge (sub-100ms queries)
- **Cold Tier**: Delta Lake with Parquet for complete version history (sub-2s temporal queries)
- **ACID Consistency**: Write-ahead logging with compensating transactions across heterogeneous backends
- **Temporal Query Routing**: Automatic hot/cold path selection with temporal leakage prevention

---

## Key Features

**Core Components**:
- Content-addressable chunk-level CDC with SHA-256 hashing
- Position metadata (INT64 paragraph index) for audit trails
- Dual-tier storage: Milvus (hot) + Delta Lake (cold)
- ACID transactions via write-ahead logging
- Temporal query engine with hot/cold routing
- CLI with `--as-of` flag for point-in-time queries

**Implementation Status**:
- CDC detection: 100% accuracy on test corpus
- Hot tier queries: 65ms median latency
- Cold tier queries: 1.2s median latency
- Update efficiency: 10-15% re-processing (vs 100% baseline)
- Test coverage: Core functionality validated

---

## Quick Start

### Prerequisites

- Python 3.12+
- Docker & Docker Compose
- 4GB RAM minimum

### Installation

```bash
# Clone repository
git clone https://github.com/praj-tarun/LiveVectorLake.git
cd LiveVectorLake

# Install dependencies
pip install -r requirements.txt

# Start Milvus
docker-compose up -d
```

### Basic Usage

```bash
# Generate test data
python tests/generate_test_data.py

# Initial ingestion
python src/cli.py ingest data/test_news --reset

# Ingest modified data (test CDC)
python src/cli.py ingest data/test_news_v2

# Query current knowledge
python src/cli.py query "What is AI?"

# Historical query
python src/cli.py query "What is AI?" --as-of 2024-01-15
```

**Expected CDC Output**:
```
============================================================
CDC INGESTION SUMMARY
============================================================
Documents processed: 5
Total chunks added: 2      ← CDC detected changes!
Total chunks deleted: 2
Total chunks unchanged: 8
============================================================
```

---

## Architecture

LiveVectorLake implements a **dual-tier temporal RAG system** with automatic change detection:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       LIVEVECTORLAKE ARCHITECTURE                       │
└─────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────┐         ┌──────────────────┐         ┌──────────────────┐
  │  CDC Ingestion   │         │  Dual-Tier       │         │  Temporal Query  │
  │                  │         │  Storage         │         │  Engine          │
  ├──────────────────┤         ├──────────────────┤         ├──────────────────┤
  │ • Chunking       │────────▶│ HOT (Milvus)     │────────▶│ • Hot Path       │
  │ • SHA-256 Hash   │         │ - HNSW Index     │         │   (<100ms)       │
  │ • Change Detect  │         │ - Current chunks │         │                  │
  │ • Embedding      │         │                  │         │ • Cold Path      │
  │                  │────────▶│ COLD (Delta Lake)│────────▶│   (<2s)          │
  │                  │         │ - Parquet        │         │                  │
  │                  │         │ - Full history   │         │ • Time-travel    │
  └──────────────────┘         └──────────────────┘         └──────────────────┘
       10-15%                    ACID Consistency              Point-in-time
    re-processing                Write-ahead log                  retrieval
```

**Core Components**:
1. **CDC Chunker** ([chunker.py](src/cdc/chunker.py)) - SHA-256 content addressing for deterministic change detection
2. **Hash Store** ([hash_store.py](src/cdc/hash_store.py)) - Persistent chunk hash registry with JSON storage
3. **Hot Tier** ([milvus_db.py](src/vectordb/milvus_db.py)) - Milvus with HNSW indexing for current chunks
4. **Cold Tier** ([delta_store.py](src/lakehouse/delta_store.py)) - Delta Lake with Parquet for version history
5. **Embedding Engine** - SentenceTransformers (all-MiniLM-L6-v2, 384-dim)
6. **Query Router** ([query_engine.py](src/query_engine.py)) - Temporal query classifier with hot/cold path selection

**[View Detailed Architecture & Diagrams →](docs/ARCHITECTURE.md)**

---

## Performance

### Preliminary Results

Evaluation on 100-document corpus versioned across 5 time points:

| Metric | Standard RAG | LiveVectorLake | Result |
|--------|--------------|----------------|--------|
| Re-processing on update | 100% of chunks | 10-15% of chunks | 85-90% reduction |
| Current query latency | ~50ms | 65ms (median) | Comparable |
| Temporal query latency | Not supported | 1.2s (median) | New capability |
| Storage optimization | All chunks in vector DB | Only current in hot tier | 80-90% hot tier reduction |
| Change detection | Manual tracking | 100% accuracy (SHA-256) | Deterministic |
| Version history | Not available | Complete (ACID) | Full audit trail |

---

## Documentation

- **[Architecture](docs/ARCHITECTURE.md)** - Detailed system design, diagrams, and data flows
- **[Benchmarking Guide](tests/BENCHMARKS_README.md)** - How to run and interpret benchmarks
- **[Problem Statement](docs/Problem_statement.md)** - Research problems addressed
- **[Project Document](docs/Project.md)** - Complete research proposal
- **[Roadmap](docs/roadmap.md)** - Implementation phases and status

---

## Project Structure

```
LiveVectorLake/
├── src/
│   ├── cdc/                # Change detection components
│   ├── vectordb/           # Milvus integration
│   ├── lakehouse/          # Delta Lake storage
│   ├── pipeline/           # CDC ingestion pipeline
│   └── query_engine.py     # Query router
├── tests/
│   ├── benchmark_suite.py  # Comprehensive benchmarks
│   └── baselines/          # Baseline comparisons
├── docs/                   # Detailed documentation
└── data/                   # Test datasets
```

---

## Testing & Benchmarks

### Run Benchmarks

```bash
# Generate benchmark corpus
python tests/generate_versioned_corpus.py

# Run complete benchmark suite
python tests/benchmark_suite.py
```

### Validation Results

| Test | Expected | Actual | Status |
|------|----------|--------|--------|
| Initial ingestion | 10 added | 10 added | Pass |
| Re-ingest same | 0 added, 10 unchanged | 0 added, 10 unchanged | Pass |
| Modified data | 2 added, 2 deleted, 8 unchanged | 2 added, 2 deleted, 8 unchanged | Pass |
| Current query | <100ms | <100ms, 3 results | Pass |
| Historical query | <2s | <2s, 5 results | Pass |

**CDC Detection Accuracy**: 100%  
**Query Engine Tests**: 4/4 passing

---

## CLI Commands

```bash
# Ingest documents
python src/cli.py ingest <path> [--reset]

# Query current knowledge
python src/cli.py query "<text>" [--top-k N]

# Historical query
python src/cli.py query "<text>" --as-of YYYY-MM-DD

# Audit document history (planned)
python src/cli.py audit <doc_id>
```

---

## Troubleshooting

**Milvus Connection Error**:
```bash
docker ps | grep milvus
docker-compose restart
```

**Hash Store Not Persisting**:
- Check file permissions for `cdc_hash_store.json`

**Embedding Slow**:
- First run downloads model (~80MB)
- Subsequent runs use cached model

---

## Future Work

- **Comprehensive Evaluation**: Benchmark on BEIR, MS MARCO with standard metrics (MRR, NDCG@k, recall@k)
- **Learned Temporal Embeddings**: Contrastive learning for temporal-semantic joint representations
- **Distributed Deployment**: Scalability for petabyte-scale corpora
- **Batch Optimization**: Parallel CDC processing and bulk vector operations

---

## License

This project is licensed under the MIT License.
