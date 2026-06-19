# agents/planner.py
import re
from llm.client import GeminiClient
from tools.rag_tool import RAGTool
from tools.memory_tool import MemoryTool

class StrategyPlannerAgent:
    """
    Strategy Planner Agent:
    - Receives incident reports from the Incident Analyst Agent
    - Queries RAG database for relevant SOPs
    - Queries Memory database for similar past incidents and outcomes
    - Formulates corrective actions and estimates operational improvements
    - Persists the new incident in SQLite memory
    """
    def __init__(self):
        self.llm = GeminiClient()
        self.rag_tool = RAGTool()
        self.memory_tool = MemoryTool()
        self.system_instruction = (
            "You are an AI Strategy Planner Agent for a food delivery platform.\n"
            "Your task is to take an Incident Report, retrieved Standard Operating Procedures (SOP), "
            "and similar past incidents, and formulate a corrective action plan.\n\n"
            "Format your output exactly as:\n"
            "Retrieved SOP: [Name / Code of SOP retrieved]\n"
            "Expected Recovery Improvement: [Percentage value, e.g. 28%]\n"
            "Recommended Corrective Actions:\n"
            "1. [Action 1]\n"
            "2. [Action 2]\n"
            "3. [Action 3]\n\n"
            "Strategic Rationale:\n"
            "[Slight explanation of why this plan works]"
        )

    def formulate_plan(self, incident_report: dict) -> tuple[dict, str]:
        """
        Formulate a plan based on the incident report.
        Returns:
            parsed_plan (dict): keys 'actions', 'sop', 'improvement', 'past_incidents'
            raw_log (str): raw text response from the LLM for tracing
        """
        issue = incident_report.get("issue", "General Anomaly")
        
        # 1. Retrieve SOP via RAG
        rag_results = self.rag_tool.search_sops(issue, top_k=1)
        sop_content = rag_results[0]["content"] if rag_results else "No relevant SOP found."
        sop_name = rag_results[0]["doc_name"] if rag_results else "General SOP"
        
        # 2. Retrieve past incidents via Memory Tool
        past_incidents = self.memory_tool.find_similar_incidents(issue, limit=2)
        
        # Format Memory records for prompt
        memory_str = ""
        if past_incidents:
            for idx, pi in enumerate(past_incidents):
                memory_str += (
                    f"Past Incident {idx+1}:\n"
                    f"  Incident: {pi.get('incident')}\n"
                    f"  Solution Applied: {pi.get('solution')}\n"
                    f"  Improvement: {pi.get('improvement')}%\n"
                    f"  Confidence: {pi.get('confidence')}\n\n"
                )
        else:
            memory_str = "No similar past incidents recorded."

        # 3. Construct prompt
        prompt = (
            f"=== INCIDENT REPORT ===\n"
            f"Issue: {issue}\n"
            f"Confidence: {incident_report.get('confidence', 'N/A')}\n"
            f"Evidence:\n"
            + "\n".join([f"- {ev}" for ev in incident_report.get("evidence", [])]) + "\n\n"
            f"=== RETRIEVED SOP ===\n"
            f"{sop_content}\n\n"
            f"=== PAST MEMORY RECORDS ===\n"
            f"{memory_str}\n"
        )
        
        # 4. Generate response
        raw_log = self.llm.generate_response(self.system_instruction, prompt)
        
        # 5. Parse output
        parsed_plan = self._parse_plan(raw_log, past_incidents, sop_content)
        
        # 6. Save back to Memory (SQLite)
        solution_str = " | ".join(parsed_plan.get("actions", []))
        imp_val = parsed_plan.get("improvement", "25%")
        imp_numeric = 25
        try:
            imp_numeric = int(re.findall(r"\d+", imp_val)[0])
        except Exception:
            pass
            
        conf_val = 0.90
        try:
            conf_str = incident_report.get("confidence", "90%")
            conf_val = float(re.findall(r"\d+", conf_str)[0]) / 100.0
        except Exception:
            pass
            
        self.memory_tool.record_incident(
            incident_name=issue,
            solution=solution_str,
            improvement=imp_numeric,
            confidence=conf_val
        )
        
        return parsed_plan, raw_log

    def _parse_plan(self, log: str, past_incidents: list[dict], sop_content: str) -> dict:
        sop_ref = "SOP-General"
        improvement = "25%"
        actions = []
        
        sop_match = re.search(r"Retrieved SOP\s*[:*]*\s*([^\n]+)", log, re.IGNORECASE)
        imp_match = re.search(r"Expected Recovery Improvement\s*[:*]*\s*([^\n]+)", log, re.IGNORECASE)
        
        if sop_match:
            sop_ref = sop_match.group(1).replace("**", "").replace("*", "").strip()
        if imp_match:
            improvement = imp_match.group(1).replace("**", "").replace("*", "").strip()
            
        # Parse numbered list actions (e.g. 1. Action, 2. Action)
        for line in log.split("\n"):
            line = line.strip()
            match = re.match(r"^\d+\.\s*(.+)$", line)
            if match:
                act_text = match.group(1).replace("**", "").strip()
                if act_text:
                    actions.append(act_text)
                    
        # Fallback if no actions parsed
        if not actions:
            actions = ["Activate general incident response protocols."]

        return {
            "sop": sop_ref,
            "improvement": improvement,
            "actions": actions,
            "past_incidents": past_incidents,
            "sop_content": sop_content
        }
