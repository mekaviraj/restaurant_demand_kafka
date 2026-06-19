# agents/analyst.py
import re
from llm.client import GeminiClient
from tools.metrics_tool import MetricsTool

class IncidentAnalystAgent:
    """
    Incident Analyst Agent:
    - Reads operational metrics using the Metrics Tool
    - Analyses anomalies and identifies root causes using LLM
    - Produces a structured incident report and raw execution logs
    """
    def __init__(self, metrics_source=None):
        self.llm = GeminiClient()
        self.metrics_tool = MetricsTool(metrics_source)
        self.system_instruction = (
            "You are an AI Incident Analyst Agent for a food delivery platform.\n"
            "Your task is to analyze real-time operational metrics, detect anomalies, "
            "identify likely root causes, and produce a structured incident report.\n\n"
            "Format your output exactly as:\n"
            "Issue: [Name of the issue, e.g. Kitchen Bottleneck]\n"
            "Confidence: [Percentage between 0-100%, e.g. 91%]\n"
            "Evidence:\n"
            "- [Fact from the metrics supporting your claim]\n"
            "- [Fact from the metrics supporting your claim]\n"
        )

    def analyze(self) -> tuple[dict, str]:
        """
        Run the analysis.
        Returns:
            parsed_report (dict): A dictionary with keys 'issue', 'confidence', 'evidence'
            raw_log (str): The raw text response from the LLM for agent trace logging
        """
        metrics = self.metrics_tool.get_latest_metrics()
        
        # Format the prompt with current metrics
        prompt = (
            f"Please analyze these current operational metrics:\n"
            f"Delay Rate = {metrics.get('delay_rate', 0.0):.1f}%\n"
            f"Prep Time = {metrics.get('avg_prep_time', 0.0):.1f} min\n"
            f"Cancellation Rate = {metrics.get('cancellation_rate', 0.0):.1f}%\n"
            f"Reliability Score = {metrics.get('reliability_score', 100.0):.1f}%\n"
            f"Incident Mode Hint = {metrics.get('active_incident', 'normal')}\n"
        )
        
        # Get raw response from LLM
        raw_log = self.llm.generate_response(self.system_instruction, prompt)
        
        # Parse the structured response
        parsed_report = self._parse_report(raw_log, metrics)
        return parsed_report, raw_log

    def _parse_report(self, log: str, metrics: dict) -> dict:
        issue = "General Operational Anomaly"
        confidence = "75%"
        evidence = []
        
        # Attempt regex parsing of Issue and Confidence
        issue_match = re.search(r"Issue\s*[:*]*\s*([^\n]+)", log, re.IGNORECASE)
        confidence_match = re.search(r"Confidence\s*[:*]*\s*(\d+%)", log, re.IGNORECASE)
        
        if issue_match:
            issue = issue_match.group(1).replace("**", "").replace("*", "").strip()
        if confidence_match:
            confidence = confidence_match.group(1).strip()
            
        # Parse evidence lines starting with "-" or "*"
        for line in log.split("\n"):
            line = line.strip()
            if line.startswith("-") or line.startswith("*"):
                # Avoid catching headers
                if "evidence" not in line.lower() and "issue" not in line.lower() and "confidence" not in line.lower():
                    ev_text = line.lstrip("-* ").replace("**", "").strip()
                    if ev_text:
                        evidence.append(ev_text)
                    
        # Fallback if no evidence parsed
        if not evidence:
            evidence = ["Metrics deviate from normal thresholds."]

        return {
            "issue": issue,
            "confidence": confidence,
            "evidence": evidence,
            "metrics": metrics
        }
