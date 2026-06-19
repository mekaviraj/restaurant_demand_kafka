# tools/metrics_tool.py

class MetricsTool:
    """
    Metrics Tool for retrieving operational performance figures
    from the streaming consumer pipeline.
    """
    def __init__(self, metrics_source=None):
        self.metrics_source = metrics_source

    def get_latest_metrics(self) -> dict:
        """
        Return the latest Delay Rate, Average Prep Time, Cancellation Rate, and Reliability Score.
        """
        if self.metrics_source:
            if callable(self.metrics_source):
                return self.metrics_source()
            return self.metrics_source
        
        # Default fallback values for testing
        return {
            "delay_rate": 0.0,
            "avg_prep_time": 15.0,
            "cancellation_rate": 0.0,
            "reliability_score": 100.0
        }
