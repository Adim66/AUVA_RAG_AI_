"""Streamlit web interface for LiveVectorLake"""
import streamlit as st
import sys
from pathlib import Path
from datetime import datetime, date
import time

# Add src to path
sys.path.append(str(Path(__file__).parent))

from query_engine import QueryEngine
from pipeline.cdc_ingest_simple import CDCIngestionPipeline

# Page config
st.set_page_config(
    page_title="LiveVectorLake",
    page_icon="🌊",
    layout="wide"
)

# Initialize session state
if 'query_engine' not in st.session_state:
    st.session_state.query_engine = QueryEngine()
if 'ingestion_history' not in st.session_state:
    st.session_state.ingestion_history = []

# Title
st.title("LiveVectorLake")
st.markdown("**Streaming Temporal RAG with Automatic Change Detection**")

# Sidebar
with st.sidebar:
    st.header("Navigation")
    page = st.radio("Go to", ["Query", "Ingest", "CDC History"])
    
    st.markdown("---")
    st.markdown("### System Status")
    st.markdown("✓ Milvus (Hot Tier)")
    st.markdown("✓ Delta Lake (Cold Tier)")

# Query Page
if page == "Query":
    st.header("Query Knowledge Base")
    
    # Query type selection
    query_type = st.radio(
        "Query Type",
        ["Current", "Historical"],
        horizontal=True,
        help="Current: Search active chunks. Historical: Search at specific date."
    )
    
    # Query input
    col1, col2 = st.columns([3, 1])
    
    with col1:
        query_text = st.text_input(
            "Enter your query",
            placeholder="What is machine learning?"
        )
    
    with col2:
        top_k = st.number_input("Results", min_value=1, max_value=20, value=5)
    
    # Historical date picker
    as_of_date = None
    if query_type == "Historical":
        as_of_date = st.date_input(
            "As of date",
            value=date.today(),
            help="Retrieve knowledge as it existed on this date"
        )
    
    # Search button
    if st.button("Search", type="primary"):
        if not query_text:
            st.warning("Please enter a query")
        else:
            with st.spinner("Searching..."):
                start_time = time.time()
                
                try:
                    if query_type == "Current":
                        results = st.session_state.query_engine.query_current(
                            query_text, 
                            top_k=top_k
                        )
                    else:
                        # Convert date to timestamp
                        as_of_timestamp = int(datetime.combine(
                            as_of_date, 
                            datetime.max.time()
                        ).timestamp())
                        results = st.session_state.query_engine.query_historical(
                            query_text,
                            as_of_timestamp,
                            top_k=top_k
                        )
                    
                    query_time = time.time() - start_time
                    
                    # Display results
                    st.success(f"Found {len(results)} results in {query_time:.2f}s")
                    
                    if results:
                        for i, result in enumerate(results, 1):
                            with st.expander(f"Result {i} - Similarity: {result['similarity']:.4f}"):
                                st.markdown(f"**Document ID:** `{result['doc_id']}`")
                                st.markdown(f"**Chunk ID:** `{result['chunk_id'][:50]}...`")
                                
                                if 'content' in result:
                                    st.markdown("**Content:**")
                                    st.text(result['content'][:500] + "..." if len(result['content']) > 500 else result['content'])
                                
                                if 'timestamp' in result:
                                    ts = datetime.fromtimestamp(result['timestamp'])
                                    st.markdown(f"**Timestamp:** {ts.strftime('%Y-%m-%d %H:%M:%S')}")
                                
                                st.markdown(f"**Query Type:** {result.get('query_type', 'N/A')}")
                    else:
                        st.info("No results found. Try a different query or date.")
                
                except Exception as e:
                    st.error(f"Query failed: {str(e)}")
                    st.exception(e)

# Ingest Page
elif page == "Ingest":
    st.header("Document Ingestion")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload document",
        type=['txt'],
        help="Upload a text file to ingest into the knowledge base"
    )
    
    # Reset option
    reset_collection = st.checkbox(
        "Reset collection",
        help="Clear all existing data before ingestion"
    )
    
    # Ingest button
    if st.button("Ingest", type="primary"):
        if not uploaded_file:
            st.warning("Please upload a file")
        else:
            with st.spinner("Ingesting document..."):
                try:
                    # Initialize pipeline
                    pipeline = CDCIngestionPipeline(reset_milvus=reset_collection)
                    start_time = time.time()
                    
                    # Read file content
                    content = uploaded_file.read().decode('utf-8')
                    doc_id = uploaded_file.name.replace('.txt', '')
                    
                    # Ingest single document
                    summary = pipeline.ingest_document(doc_id, content, source="file")
                    ingest_time = time.time() - start_time
                    
                    # Store in history
                    st.session_state.ingestion_history.append({
                        'doc_id': doc_id,
                        'timestamp': datetime.now(),
                        'summary': summary,
                        'time': ingest_time
                    })
                    
                    # Display summary
                    st.success(f"Ingestion completed in {ingest_time:.2f}s")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Added", summary.get('added', 0))
                    with col2:
                        st.metric("Deleted", summary.get('deleted', 0))
                    with col3:
                        st.metric("Unchanged", summary.get('unchanged', 0))
                    
                    # CDC details
                    if summary.get('added', 0) > 0 or summary.get('deleted', 0) > 0:
                        st.info("CDC detected changes in the document")
                    else:
                        st.info("No changes detected - document unchanged")
                
                except Exception as e:
                    st.error(f"Ingestion failed: {str(e)}")
                    st.exception(e)

# CDC History Page
elif page == "CDC History":
    st.header("CDC Ingestion History")
    
    if not st.session_state.ingestion_history:
        st.info("No ingestion history yet. Upload documents in the Ingest page.")
    else:
        for entry in reversed(st.session_state.ingestion_history):
            with st.expander(
                f"{entry['doc_id']} - {entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}"
            ):
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Added", entry['summary'].get('added', 0))
                with col2:
                    st.metric("Deleted", entry['summary'].get('deleted', 0))
                with col3:
                    st.metric("Unchanged", entry['summary'].get('unchanged', 0))
                with col4:
                    st.metric("Time", f"{entry['time']:.2f}s")
                
                # Status indicator
                if entry['summary'].get('added', 0) > 0:
                    st.markdown("🟢 **Status:** New chunks added")
                elif entry['summary'].get('deleted', 0) > 0:
                    st.markdown("🟡 **Status:** Chunks deleted/modified")
                else:
                    st.markdown("⚪ **Status:** No changes")

# Footer
st.markdown("---")
st.markdown(
    "LiveVectorLake - Streaming Temporal RAG | "
    "[GitHub](https://github.com/praj-tarun/LiveVectorLake)"
)
