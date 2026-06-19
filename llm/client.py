# llm/client.py
import os
import json
import re

class GeminiClient:
    def __init__(self):
        self.api_key = os.environ.get("GEMINI_API_KEY")
        self.use_mock = not self.api_key
        
        if not self.use_mock:
            try:
                import google.generativeai as genai
                genai.configure(api_key=self.api_key)
                self.model = genai.GenerativeModel("gemini-1.5-flash")
                print("✨ Gemini API client initialized successfully.")
            except Exception as e:
                print(f"⚠️ Error initializing Gemini API client: {e}. Falling back to Mock Mode.")
                self.use_mock = True
        else:
            print("ℹ️ No GEMINI_API_KEY environment variable found. Running in MOCK Mode.")

    def generate_response(self, system_instruction: str, prompt: str) -> str:
        if not self.use_mock:
            try:
                full_prompt = f"{system_instruction}\n\n{prompt}"
                response = self.model.generate_content(full_prompt)
                return response.text
            except Exception as e:
                print(f"⚠️ Gemini API generation failed: {e}. Falling back to Mock.")
                # Fall through to mock response
                
        # Mock Response Logic
        return self._generate_mock_response(system_instruction, prompt)

    def _generate_mock_response(self, system_instruction: str, prompt: str) -> str:
        prompt_lower = prompt.lower()
        
        # 1. Incident Analyst Mock Response
        if "delay rate" in prompt_lower or "prep time" in prompt_lower or "cancellation rate" in prompt_lower:
            delay_rate = self._extract_metric(prompt, "Delay Rate") or "24%"
            prep_time = self._extract_metric(prompt, "Prep Time") or "28 min"
            cancel_rate = self._extract_metric(prompt, "Cancellation Rate") or "12%"
            
            # Determine issue type
            if "kitchen_delay" in prompt_lower or (self._float_val(prep_time) > 22):
                issue = "Kitchen Bottleneck"
                confidence = "91%"
                evidence = [
                    f"Average prep time has spiked to {prep_time} (exceeding standard SLA of 20 min).",
                    "Preparation delays directly correlate with order queues.",
                    "Analysis shows a 42% increase in prep durations compared to the daily average."
                ]
            elif "courier_delay" in prompt_lower or (self._float_val(delay_rate) > 20 and self._float_val(prep_time) <= 20):
                issue = "Courier Shortage"
                confidence = "89%"
                evidence = [
                    f"Delay rate has reached {delay_rate} despite normal kitchen prep times of {prep_time}.",
                    "Courier dispatch queue latency has increased.",
                    "Pending delivery assignments have built up."
                ]
            elif "weather_surge" in prompt_lower:
                issue = "Weather Surge"
                confidence = "95%"
                evidence = [
                    f"Adverse weather conditions detected with high delays ({delay_rate}) and elevated cancellation rate ({cancel_rate}).",
                    "Rider transit speeds have dropped.",
                    "Active couriers logged in has decreased."
                ]
            elif "staff_shortage" in prompt_lower:
                issue = "Staff Shortage"
                confidence = "88%"
                evidence = [
                    f"Average prep time has increased to {prep_time} and order cancellation rate is {cancel_rate}.",
                    "Kitchen stations are operating under capacity.",
                    "Order packing and assembly times are severely bottlenecked."
                ]
            elif "festival_rush" in prompt_lower:
                issue = "Festival Rush"
                confidence = "90%"
                evidence = [
                    f"Order inflow has surged, leading to prep times of {prep_time} and delay rate of {delay_rate}.",
                    "Customer transaction frequency is high.",
                    "Peak traffic delays are affecting courier times."
                ]
            elif "inventory_shortage" in prompt_lower or (self._float_val(cancel_rate) > 15):
                issue = "Inventory Shortage"
                confidence = "93%"
                evidence = [
                    f"Order cancellation rate is high ({cancel_rate}) while average prep time is {prep_time}.",
                    "Multiple menu items are running out of stock.",
                    "Automated alerts show raw ingredient deficits."
                ]
            else:
                issue = "General Operational Anomaly"
                confidence = "75%"
                evidence = [
                    f"Elevated delay rate ({delay_rate}) and cancellation rate ({cancel_rate}) detected.",
                    "Operational metrics are fluctuating near alert thresholds."
                ]

            evidence_str = "\n".join([f"- {ev}" for ev in evidence])
            
            return f"""### INCIDENT ANALYSIS REPORT

**Issue**: {issue}
**Confidence**: {confidence}

**Evidence**:
{evidence_str}

**Status**: Active Anomaly Detected
"""

        # 2. Strategy Planner Mock Response
        else:
            issue = "General Anomaly"
            if "kitchen bottleneck" in prompt_lower:
                issue = "Kitchen Bottleneck"
                actions = [
                    "Add one cook to Station B (Prep/Assembly Line) immediately.",
                    "Prioritize high-volume / high-frequency dishes (e.g., standard burgers and simple biryani).",
                    "Temporarily pause low-demand/high-prep time items from the digital menu."
                ]
                improvement = "28%"
                sop = "SOP-OPS-001 (Kitchen Delay SOP)"
            elif "courier shortage" in prompt_lower:
                issue = "Courier Shortage"
                actions = [
                    "Increase delivery payout multiplier to 1.5x for active riders to attract partners.",
                    "Route high-priority orders to third-party premium logistics networks.",
                    "Automatically stack up to 2 orders per delivery courier going to the same area."
                ]
                improvement = "22%"
                sop = "SOP-OPS-002 (Courier Delay SOP)"
            elif "weather surge" in prompt_lower:
                issue = "Weather Surge"
                actions = [
                    "Restrict delivery operations to a maximum 2.5km radius from the store.",
                    "Enable Weather Surcharge (+30%) payout bonuses for riders.",
                    "Transition to double-bag thermal packaging to protect food from rain and heat."
                ]
                improvement = "30%"
                sop = "SOP-OPS-006 (Weather Surge SOP)"
            elif "staff shortage" in prompt_lower:
                issue = "Staff Shortage"
                actions = [
                    "Re-assign floor staff / front-of-house crew to packing and assembly tables.",
                    "Optimize the active menu by suspending dishes that require multiple stations.",
                    "Authorize overtime bonuses (+2.0x hourly) for off-duty kitchen staff."
                ]
                improvement = "20%"
                sop = "SOP-OPS-003 (Staff Shortage SOP)"
            elif "festival rush" in prompt_lower:
                issue = "Festival Rush"
                actions = [
                    "Limit order throughput by adding a 5-minute stagger interval on aggregators.",
                    "Pre-prep popular high-demand items in batches before rush hours.",
                    "Switch active delivery radius to a compact 3km zone."
                ]
                improvement = "25%"
                sop = "SOP-OPS-004 (Festival Rush SOP)"
            elif "inventory shortage" in prompt_lower:
                issue = "Inventory Shortage"
                actions = [
                    "Mark out-of-stock items as Unavailable on delivery apps.",
                    "Offer pre-approved substitutes automatically to customers.",
                    "Direct emergency local vendor supply orders for ingredient replenishment."
                ]
                improvement = "35%"
                sop = "SOP-OPS-005 (Inventory Shortage SOP)"
            else:
                actions = [
                    "Monitor operational metrics and verify data flows.",
                    "Standardize delivery dispatcher assignments.",
                    "Ensure backup riders are on standby."
                ]
                improvement = "10%"
                sop = "SOP-OPS-General"

            actions_str = "\n".join([f"{i+1}. {act}" for i, act in enumerate(actions)])
            
            return f"""### STRATEGIC ACTION PLAN

**Retrieved SOP**: {sop}
**Expected Recovery Improvement**: {improvement}

**Recommended Corrective Actions**:
{actions_str}

**Strategic Rationale**:
The corrective actions align with standard operating procedures and target the specific constraints identified. Execution of these steps will relieve bottleneck congestion and restore service metrics to baseline levels.
"""

    def _extract_metric(self, text: str, name: str) -> str:
        # Match "Delay Rate = 24%" or "Delay Rate: 24%" or "Delay Rate = 24 min"
        match = re.search(rf"{name}\s*[:=]\s*([^\n]+)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    def _float_val(self, val_str: str) -> float:
        try:
            return float(re.findall(r"[-+]?\d*\.\d+|\d+", val_str)[0])
        except Exception:
            return 0.0
