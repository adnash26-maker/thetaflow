"""
ThetaFlow - AI Investment Analyst

Uses Claude to generate real contextual investment analysis.
Receives: headline + value chain context + financial data for matched tickers
Outputs: structured analysis with specific reasoning per event, not templates.
"""

import os
import json
import logging
from typing import Dict, List, Optional

logger = logging.getLogger("thetaflow.analyst")


class AIAnalyst:
    """Generates contextual investment analysis using Claude."""

    def __init__(self):
        self.api_key = os.getenv("ANTHROPIC_API_KEY")
        self.client = None
        if self.api_key:
            try:
                import anthropic
                self.client = anthropic.Anthropic(api_key=self.api_key)
                logger.info("AI Analyst initialized with Claude")
            except ImportError:
                logger.warning("anthropic package not installed")
        else:
            logger.warning("ANTHROPIC_API_KEY not set — AI analysis disabled")

    @property
    def available(self):
        return self.client is not None

    def analyze_event(self, headline: str, chain_matches: List[Dict],
                      ticker_financials: List[Dict]) -> Dict:
        """Generate full investment analysis for an event.

        Args:
            headline: The news event to analyze
            chain_matches: Which value chains were matched and why
            ticker_financials: Financial data for relevant tickers

        Returns:
            {analysis: str, ticker_analyses: {TICKER: str}, risk_factors: [str]}
        """
        if not self.available:
            return {"analysis": "", "ticker_analyses": {}, "risk_factors": []}

        # Build context for Claude
        chains_context = "\n".join([
            f"- {c['chain_name']} (relevance: {c['relevance_score']:.0%})"
            for c in chain_matches
        ])

        tickers_context = ""
        for t in ticker_financials[:8]:
            fin = t.get("financials", {})
            lines = [f"\n**{t['ticker']}** ({t['company']}) — {t['layer']} layer"]
            lines.append(f"  Price: ${t.get('current_price', 0):.2f} ({t.get('change_pct', 0):+.1f}% today)")
            if fin.get("pe_forward"): lines.append(f"  Forward P/E: {fin['pe_forward']}x")
            if fin.get("market_cap"): lines.append(f"  Market Cap: {fin['market_cap']}")
            if fin.get("distance_from_high_pct"): lines.append(f"  vs 52w High: {fin['distance_from_high_pct']}%")
            if fin.get("revenue_growth") is not None: lines.append(f"  Revenue Growth: {fin['revenue_growth']}%")
            lines.append(f"  Chain: {t['chain_name']} → {t['layer']}")
            lines.append(f"  Exposure: {t.get('exposure', 'medium')}")
            lines.append(f"  Notes: {t.get('thesis', '')}")
            tickers_context += "\n".join(lines)

        prompt = f"""You are a senior investment analyst at a top-tier hedge fund. Analyze this market event and provide actionable investment intelligence.

EVENT: {headline}

MATCHED VALUE CHAINS:
{chains_context}

RELEVANT TICKERS WITH FINANCIAL DATA:
{tickers_context}

Provide your analysis in this exact JSON format:
{{
  "summary": "2-3 sentence executive summary of the event's investment implications. Be specific about WHY this matters for each part of the value chain.",
  "ticker_analyses": {{
    "TICKER1": "Specific analysis for this ticker in context of THIS event. Reference actual financial metrics. State whether it's a buy, hold, or avoid at current levels and why. 2-3 sentences.",
    "TICKER2": "..."
  }},
  "risk_factors": ["Specific risk 1", "Specific risk 2", "Specific risk 3"],
  "time_horizon": "How long until this catalyst impacts stock prices",
  "contrarian_view": "One sentence on what could go wrong or what the market might be missing"
}}

IMPORTANT:
- Reference actual financial data (P/E, revenue growth, distance from highs)
- Be specific to THIS event — don't give generic analysis
- Each ticker analysis must explain the specific transmission mechanism from event to that company's earnings
- Include at least one contrarian or risk perspective
- If a ticker is overvalued despite the catalyst, say so
- Keep each ticker analysis to 2-3 sentences max"""

        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text.strip()

            # Parse JSON from response
            # Handle potential markdown code blocks
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()

            result = json.loads(text)
            return result

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {e}")
            # Return the raw text as summary if JSON parsing fails
            return {
                "analysis": text if 'text' in dir() else "",
                "ticker_analyses": {},
                "risk_factors": [],
            }
        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return {"analysis": "", "ticker_analyses": {}, "risk_factors": []}
