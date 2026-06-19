# tools/rag_tool.py
from rag.vector_db import VectorDB

class RAGTool:
    """
    RAG Tool for retrieving standard operating procedures (SOPs)
    relevant to operational incidents.
    """
    def __init__(self):
        self.db = VectorDB()

    def search_sops(self, query: str, top_k: int = 1) -> list[dict]:
        """
        Search the SOP database for a given query string.
        Returns a list of dicts: [{"doc_name": ..., "score": ..., "content": ...}]
        """
        return self.db.search(query, top_k=top_k)
