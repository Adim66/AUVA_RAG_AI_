"""CLI tool for LiveVectorLake: CDC ingestion and querying"""
import argparse
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from sources.text_loader import load_text_files, load_single_file
from pipeline.cdc_ingest_simple import CDCIngestionPipeline
from query_engine import QueryEngine
from datetime import datetime

def ingest_command(args):
    """Handle ingest command"""
    print("Starting CDC ingestion...")
    pipeline = CDCIngestionPipeline(reset_milvus=args.reset)
    
    path = Path(args.path)
    
    if path.is_file():
        # Single file
        print(f"Loading file: {path}")
        doc = load_single_file(str(path))
        summary = pipeline.ingest_document(doc['doc_id'], doc['content'])
        pipeline.print_summary(summary)
    
    elif path.is_dir():
        # Directory of files
        print(f"Loading files from: {path}")
        documents = load_text_files(str(path), pattern=args.pattern)
        print(f"Found {len(documents)} files")
        
        if not documents:
            print("No files found!")
            return
        
        print("Ingesting documents (batched Delta Lake writes)...")
        summary = pipeline.ingest_batch(documents)
        pipeline.print_summary(summary)
    
    else:
        print(f"Error: Path not found: {path}")
        sys.exit(1)

def query_command(args):
    """Handle query command"""
    engine = QueryEngine()
    
    if args.as_of:
        # Historical query
        try:
            as_of_date = datetime.strptime(args.as_of, '%Y-%m-%d')
            as_of_timestamp = int(as_of_date.timestamp())
            results = engine.query_historical(args.query, as_of_timestamp, top_k=args.top_k)
            engine.print_results(results, args.query, query_type="historical")
        except ValueError:
            print(f"Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        # Current query
        results = engine.query_current(args.query, top_k=args.top_k)
        engine.print_results(results, args.query, query_type="current")

def audit_command(args):
    """Handle audit command (placeholder for Phase 4)"""
    print("Audit command will be implemented in Phase 4")
    print(f"Doc ID: {args.doc_id}")

def main():
    parser = argparse.ArgumentParser(description="LiveVectorLake CLI")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Ingest command
    ingest_parser = subparsers.add_parser('ingest', help='Ingest documents with CDC')
    ingest_parser.add_argument('path', help='File or directory path')
    ingest_parser.add_argument('--pattern', default='*.txt', help='File pattern (default: *.txt)')
    ingest_parser.add_argument('--reset', action='store_true', help='Reset Milvus collection')
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Query knowledge base')
    query_parser.add_argument('query', help='Query text')
    query_parser.add_argument('--as-of', help='Historical query date (YYYY-MM-DD)')
    query_parser.add_argument('--top-k', type=int, default=5, help='Number of results (default: 5)')
    
    # Audit command (placeholder)
    audit_parser = subparsers.add_parser('audit', help='Audit document history (Phase 4)')
    audit_parser.add_argument('doc_id', help='Document ID')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'ingest':
        ingest_command(args)
    elif args.command == 'query':
        query_command(args)
    elif args.command == 'audit':
        audit_command(args)

if __name__ == '__main__':
    main()
