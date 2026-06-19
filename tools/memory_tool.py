# tools/memory_tool.py
from memory.database import MemoryDatabase

class MemoryTool:
    """
    Memory Tool for querying similar past incidents and recording 
    finalized strategy actions in SQLite.
    """
    def __init__(self):
        self.db = MemoryDatabase()

    def find_similar_incidents(self, incident_type: str, limit: int = 2) -> list[dict]:
        """
        Query SQLite for historical incidents matching keywords from incident_type.
        """
        return self.db.find_similar(incident_type, limit=limit)

    def record_incident(self, incident_name: str, solution: str, improvement: int, confidence: float) -> None:
        """
        Save the finalized incident outcome to SQLite history.
        """
        self.db.add_incident(incident_name, solution, improvement, confidence)
