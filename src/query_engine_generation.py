"""RAG Query Engine - extends QueryEngine with LLM generation"""
from datetime import datetime
from typing import List, Dict
import sys
from pathlib import Path
from transformers import pipeline

sys.path.append(str(Path(__file__).parent))

from query_engine import QueryEngine


DEFAULT_SYSTEM_PROMPT = """You are a precise and concise assistant. 
You answer questions strictly based on the provided context documents. do not repaet the same information from different documents. and make your answer human-friendly. and  imporovise a bit if the retrieved context is incomplete, but do not hallucinate information that is not supported by the context.
If the answer is not found in the context, say so clearly. 
Always cite which document your answer comes from."""


class RAGQueryEngine(QueryEngine):
    """
    Extends QueryEngine with LLM-based generation (RAG pattern).
    Uses retrieved chunks as context to generate grounded answers.

    Hot tier  → Milvus       (current data,    via query_current)
    Cold tier → Delta Lake   (historical data, via query_historical)
    """

    def __init__(
        self,
        milvus_db=None,
        delta_store=None,
        embedding_model: str = "all-MiniLM-L6-v2",
        llm_model: str = "HuggingFaceTB/SmolLM2-1.7B-Instruct",
        system_prompt: str = DEFAULT_SYSTEM_PROMPT,
    ):
        # Initialise parent: embedding model + Milvus + DeltaStore
        super().__init__(
            milvus_db=milvus_db,
            delta_store=delta_store,
            embedding_model=embedding_model,
        )

        self.llm_model_name = llm_model
        self.system_prompt  = system_prompt

        # Load LLM once at instantiation — reused for every generation call
        print(f"[RAGQueryEngine] Loading LLM: {llm_model} ...")
        self.llm = pipeline(
            "text-generation",
            model=llm_model,
            max_new_tokens=512,
            do_sample=False,   # greedy decoding — deterministic output
            pad_token_id=2,
        )
        print("[RAGQueryEngine] LLM ready.")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_prompt(self, query_text: str, retrieved_docs: List[Dict]) -> str:
        """
        Assemble the full prompt sent to the LLM:
            system_prompt + numbered context blocks + question
        """
        context_blocks = []
        for i, doc in enumerate(retrieved_docs, 1):
            content = doc.get("content", "").strip()
            doc_id  = doc.get("doc_id", f"doc_{i}")
            sim     = doc.get("similarity", 0.0)
            context_blocks.append(
                f"[Document {i} | id={doc_id} | similarity={sim:.4f}]\n{content}"
            )

        context_str = "\n\n".join(context_blocks) if context_blocks else "No context available."

        prompt = (
            f"{self.system_prompt}\n\n"
            f"### Context\n{context_str}\n\n"
            f"### Question\n{query_text}\n\n"
            f"### Answer\n"
        )
        return prompt

    def _parse_output(self, raw_output: str, prompt: str) -> str:
        """
        HuggingFace pipelines return prompt + generated text concatenated.
        Strip the prompt prefix and remove any model loop artefacts.
        Returns only the clean generated answer.
        """
        answer = raw_output

        # Remove the prompt prefix that HF pipelines prepend
        if answer.startswith(prompt):
            answer = answer[len(prompt):]

        # Cut off if the model starts looping back into the prompt structure
        answer = answer.split("### Question")[0]
        answer = answer.split("### Context")[0]
        answer = answer.strip()

        return answer if answer else "No answer could be generated."

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def PassLLMGenerationHot(
        self,
        query_text: str,
        top_k: int = 5,
        verbose: bool = False,
    ) -> Dict:
        """
        Full RAG pipeline on the HOT tier (Milvus — current data).

        Steps:
          1. Retrieve top-k chunks from Milvus
          2. Build grounded prompt with retrieved context
          3. Generate answer with LLM
          4. Parse and return clean structured response

        Args:
            query_text: Natural-language question from the user.
            top_k:      Number of chunks to retrieve as context.
            verbose:    If True, prints intermediate steps.

        Returns:
            {
                "query":   original question,
                "answer":  generated answer,
                "sources": retrieved chunks used as context,
                "model":   LLM model name,
            }
        """
        # 1. Retrieve from hot tier (Milvus)
        retrieved = self.query_current(query_text, top_k=top_k)
        if verbose:
            print(f"[RAGQueryEngine] Retrieved {len(retrieved)} chunks from Milvus.")

        # 2. Build prompt
        prompt = self._build_prompt(query_text, retrieved)
        if verbose:
            print(f"[RAGQueryEngine] Prompt (first 400 chars):\n{prompt[:400]}...")

        # 3. Generate
        raw = self.llm(prompt)[0]["generated_text"]

        # 4. Parse output
        answer = self._parse_output(raw, prompt)
        if verbose:
            print(f"[RAGQueryEngine] Answer:\n{answer}")

        return {
            "query":   query_text,
            "answer":  answer,
            "sources": [
                {
                    "doc_id":     d.get("doc_id"),
                    "chunk_id":   d.get("chunk_id"),
                    "similarity": d.get("similarity"),
                    "content":    d.get("content", "")[:150] + "..."
                                  if len(d.get("content", "")) > 150 else d.get("content", ""),
                }
                for d in retrieved
            ],
            "model": self.llm_model_name,
        }

    def PassLLMGenerationCold(
        self,
        query_text: str,
        as_of_timestamp: int,
        top_k: int = 5,
        verbose: bool = False,
    ) -> Dict:
        """
        Full RAG pipeline on the COLD tier (Delta Lake — historical data).

        Steps:
          1. Retrieve top-k chunks from Delta Lake at the given timestamp
          2. Build grounded prompt with retrieved context
          3. Generate answer with LLM
          4. Parse and return clean structured response

        Args:
            query_text:       Natural-language question from the user.
            as_of_timestamp:  Unix timestamp — query data as it existed at that point.
            top_k:            Number of chunks to retrieve as context.
            verbose:          If True, prints intermediate steps.

        Returns:
            {
                "query":   original question,
                "answer":  generated answer,
                "sources": retrieved chunks used as context (with timestamp + status),
                "model":   LLM model name,
                "as_of":   human-readable timestamp used for the historical query,
            }
        """
        # 1. Retrieve from cold tier (Delta Lake)
        retrieved = self.query_historical(query_text, as_of_timestamp, top_k=top_k)
        if verbose:
            print(f"[RAGQueryEngine] Retrieved {len(retrieved)} chunks from Delta Lake.")

        # 2. Build prompt
        prompt = self._build_prompt(query_text, retrieved)
        if verbose:
            print(f"[RAGQueryEngine] Prompt (first 400 chars):\n{prompt[:400]}...")

        # 3. Generate
        raw = self.llm(prompt)[0]["generated_text"]

        # 4. Parse output
        answer = self._parse_output(raw, prompt)
        if verbose:
            print(f"[RAGQueryEngine] Answer:\n{answer}")

        return {
            "query":   query_text,
            "answer":  answer,
            "sources": [
                {
                    "doc_id":     d.get("doc_id"),
                    "chunk_id":   d.get("chunk_id"),
                    "similarity": d.get("similarity"),
                    # timestamp and status are cold-tier specific fields
                    "timestamp":  datetime.fromtimestamp(d["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                                  if d.get("timestamp") else None,
                    "status":     d.get("status"),
                    "content":    d.get("content", "")[:150] + "..."
                                  if len(d.get("content", "")) > 150 else d.get("content", ""),
                }
                for d in retrieved
            ],
            "model": self.llm_model_name,
            "as_of": datetime.fromtimestamp(as_of_timestamp).strftime('%Y-%m-%d %H:%M:%S'),
        }