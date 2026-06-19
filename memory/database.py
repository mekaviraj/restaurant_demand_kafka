# memory/database.py
import sqlite3
import os
from datetime import datetime

class MemoryDatabase:
    def __init__(self, db_path="memory/memory.db"):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        target_dir = os.path.join(base_dir, "memory")
        os.makedirs(target_dir, exist_ok=True)
        self.db_path = os.path.join(target_dir, "memory.db")
        self.init_db()

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS historical_incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident TEXT NOT NULL,
                    solution TEXT NOT NULL,
                    improvement INTEGER NOT NULL,
                    confidence REAL NOT NULL,
                    timestamp TEXT NOT NULL
                )
            """)
            
            # Check if seeded
            cursor.execute("SELECT COUNT(*) FROM historical_incidents")
            count = cursor.fetchone()[0]
            if count == 0:
                self.seed_db(cursor)
            conn.commit()

    def seed_db(self, cursor):
        mock_data = [
            ("Kitchen Bottleneck", "Added one cook to Station B (Prep/Assembly Line) and paused low-demand items.", 28, 0.91, "2026-06-15T12:00:00"),
            ("Courier Shortage", "Increased rider payout multiplier to 1.5x and stacked up to 2 orders per courier.", 22, 0.88, "2026-06-16T18:30:00"),
            ("Weather Surge", "Restricted delivery operations to 2.5km and enabled weather surcharge payout.", 30, 0.94, "2026-06-17T20:15:00"),
            ("Staff Shortage", "Reassigned floor staff to packing tables and authorized overtime bonuses.", 20, 0.89, "2026-06-18T14:45:00"),
            ("Inventory Shortage", "Marked out-of-stock items as unavailable and directed express supplier restocking.", 35, 0.93, "2026-06-19T09:00:00")
        ]
        cursor.executemany("""
            INSERT INTO historical_incidents (incident, solution, improvement, confidence, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, mock_data)
        print("🌱 Seeding Memory SQLite Database with historical incidents...")

    def find_similar(self, incident_type, limit=2):
        # Extract keywords longer than 3 characters (e.g. "Kitchen" out of "Kitchen Bottleneck")
        words = [w for w in incident_type.split() if len(w) > 3]
        if not words:
            words = [incident_type]
            
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            conditions = " OR ".join(["incident LIKE ?" for _ in words])
            params = [f"%{w}%" for w in words]
            
            query = f"""
                SELECT * FROM historical_incidents
                WHERE {conditions}
                ORDER BY timestamp DESC
                LIMIT ?
            """
            cursor.execute(query, params + [limit])
            results = [dict(row) for row in cursor.fetchall()]
            
            if not results:
                # Fallback to get latest records
                cursor.execute("""
                    SELECT * FROM historical_incidents
                    ORDER BY timestamp DESC
                    LIMIT ?
                """, [limit])
                results = [dict(row) for row in cursor.fetchall()]
                
            return results

    def add_incident(self, incident, solution, improvement, confidence):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            timestamp = datetime.now().isoformat()
            cursor.execute("""
                INSERT INTO historical_incidents (incident, solution, improvement, confidence, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (incident, solution, int(improvement), float(confidence), timestamp))
            conn.commit()
