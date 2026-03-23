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

    def generate_dynamic_chains(self, headline: str) -> Optional[Dict]:
        """Use Claude to dynamically generate value chain analysis for a headline.

        Instead of matching against pre-built chains, Claude identifies
        relevant investment themes and builds chains on the fly.
        """
        if not self.available:
            return None

        prompt = f"""You are a senior PM at a multi-strategy hedge fund. Your edge is finding the second and third-order trades that the market misses. Everyone knows the obvious play — your job is to find what's NOT obvious.

EVENT: {headline}

Think step by step:
1. What is the OBVIOUS first-order reaction? (Acknowledge it briefly — the market prices this in minutes.)
2. What are the HIDDEN second-order effects? (Suppliers, competitors gaining share, downstream disruptions, adjacent sectors, companies losing/gaining pricing power.)
3. What are the THIRD-ORDER trades? (The picks no one is talking about yet that will move in weeks, not hours.)

Produce your analysis in this exact JSON format:

{{
  "chains": [
    {{
      "chain_id": "short_snake_case_id",
      "chain_name": "Human Readable Theme Name",
      "theme_color": "#hex_color",
      "relevance_score": 0.85
    }}
  ],
  "obvious_play": {{
    "ticker": "SYMBOL",
    "company": "Company Name",
    "direction": "bullish or bearish",
    "summary": "One sentence. Everyone already knows this. Market prices it in minutes."
  }},
  "top_picks": [
    {{
      "ticker": "SYMBOL",
      "company": "Full Company Name",
      "direction": "bullish",
      "conviction": 0.85,
      "action": "BUY",
      "order": 2,
      "layer": "Value Chain Layer Name",
      "chain_name": "Which chain this belongs to",
      "chain_id": "matching_chain_id",
      "exposure": "critical",
      "thesis": "2-3 sentences: (1) the NON-OBVIOUS connection most investors miss, (2) specific revenue/earnings impact with numbers if possible, (3) entry logic — why NOW at this price.",
      "time_horizon": "short_term",
      "impact_score": 85.0,
      "risk_reward": "3:1"
    }}
  ],
  "all_tickers": [
  ],
  "summary": "2-3 sentences focused on the HIDDEN opportunity. Don't waste words on what's obvious. Lead with what the market is missing and why it matters.",
  "risk_factors": ["Specific risk 1", "Specific risk 2", "Specific risk 3"],
  "contrarian_view": "One sentence — the strongest argument against these trades.",
  "time_horizon": "Overall time horizon for the non-obvious effects to play out"
}}

CRITICAL RULES:
- top_picks should be the NON-OBVIOUS plays. Do NOT put the headline company first unless it's genuinely mispriced.
- order: 2 = second-order effect, 3 = third-order effect. At least 3 of top_picks must be order 2 or 3.
- The edge is in the thesis: explain a connection most investors haven't made yet. "NVDA benefits from AI" is worthless. "ANET captures 15% of every new GPU cluster buildout through 400G switches" is edge.
- Use ONLY real US-listed tickers (NYSE/NASDAQ).
- Include 5-8 tickers in top_picks (prioritize non-obvious), 8-15 in all_tickers.
- action: "BUY", "SHORT", "WATCH", or "AVOID".
- risk_reward: "3:1", "5:1", etc.
- time_horizon per ticker: "immediate", "short_term", "medium_term", "long_term".
- theme_color: hex color matching the sector theme.
- If the event is about a specific company, acknowledge it in obvious_play, then focus top_picks on the hidden plays around it."""

        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text.strip()

            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()

            result = json.loads(text)

            if not result.get("top_picks"):
                logger.warning("Claude returned no top_picks")
                return None

            # Normalize ticker data — filter out non-dict items
            for key in ("top_picks", "all_tickers"):
                items = result.get(key, [])
                cleaned = [p for p in items if isinstance(p, dict) and p.get("ticker")]
                for pick in cleaned:
                    pick["ticker"] = str(pick["ticker"]).upper().strip()
                    pick["conviction"] = min(float(pick.get("conviction", 0.5)), 0.95)
                    pick["impact_score"] = float(pick.get("impact_score", 50))
                result[key] = cleaned

            # Map all_tickers → recommendations (frontend field name)
            result["recommendations"] = result.pop("all_tickers", result.get("top_picks", []))

            # Sort top_picks by conviction * impact_score
            result["top_picks"] = sorted(
                result["top_picks"],
                key=lambda t: t.get("conviction", 0) * t.get("impact_score", 0),
                reverse=True
            )[:5]

            return result

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse dynamic chain JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Dynamic chain generation failed: {e}")
            return None
