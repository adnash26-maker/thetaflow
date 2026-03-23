"""
ThetaFlow - Impact Propagation Engine

Simulates how catalyst events propagate through value chains to generate
investment recommendations. Uses historical price correlations and
chain position to estimate impact magnitude and timing.

Core Logic:
1. Event triggers a value chain
2. Impact propagates upstream → downstream through chain nodes
3. Each node's impact is weighted by: sensitivity, position, historical correlation
4. Output: ranked list of tickers with expected impact direction and magnitude
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass

from value_chains import ALL_CHAINS, ValueChain, ChainNode, find_chains_for_event

logger = logging.getLogger("thetaflow.impact")


@dataclass
class ImpactRecommendation:
    """A single investment recommendation from the impact engine."""
    ticker: str
    company: str
    chain_id: str
    chain_name: str
    layer: str
    direction: str       # "bullish", "bearish"
    conviction: float    # 0-1 (strength of recommendation)
    impact_score: float  # 0-100
    time_horizon: str    # "immediate", "short_term", "medium_term"
    thesis: str          # Human-readable explanation
    current_price: float = 0
    change_pct: float = 0
    exposure: str = "high"


class ImpactEngine:
    """Propagates events through value chains to generate investment signals."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def analyze_event(self, headline: str, event_type: str = "news") -> Dict:
        """Analyze a single event and return full impact analysis."""
        chain_matches = find_chains_for_event(headline)

        if not chain_matches:
            return {
                "headline": headline,
                "chains_matched": 0,
                "recommendations": [],
                "summary": "No value chain exposure identified for this event.",
            }

        all_recommendations = []
        for match in chain_matches:
            chain = ALL_CHAINS.get(match["chain_id"])
            if not chain:
                continue

            recs = self._propagate_through_chain(
                chain, match["relevance_score"], event_type
            )
            all_recommendations.extend(recs)

        # Fetch current prices for all recommended tickers
        self._enrich_with_prices(all_recommendations)

        # Sort by conviction * impact_score
        all_recommendations.sort(
            key=lambda r: r.conviction * r.impact_score,
            reverse=True
        )

        # Generate summary
        top_picks = all_recommendations[:5]
        summary = self._generate_summary(headline, chain_matches, top_picks)

        return {
            "headline": headline,
            "event_type": event_type,
            "chains_matched": len(chain_matches),
            "chains": chain_matches,
            "recommendations": [self._rec_to_dict(r) for r in all_recommendations],
            "top_picks": [self._rec_to_dict(r) for r in top_picks],
            "summary": summary,
            "analyzed_at": datetime.utcnow().isoformat(),
        }

    def get_chain_analysis(self, chain_id: str) -> Dict:
        """Get full analysis for a specific value chain with current prices."""
        chain = ALL_CHAINS.get(chain_id)
        if not chain:
            return {"error": "Chain not found"}

        nodes_data = []
        all_recs = []

        for node in chain.nodes:
            node_tickers = []
            for t in node.tickers:
                rec = ImpactRecommendation(
                    ticker=t["ticker"],
                    company=t["company"],
                    chain_id=chain_id,
                    chain_name=chain.name,
                    layer=node.layer,
                    direction="bullish",
                    conviction=node.sensitivity * 0.7,
                    impact_score=node.sensitivity * 100,
                    time_horizon=self._estimate_time_horizon(node.position),
                    thesis=t.get("notes", ""),
                    exposure=t.get("exposure", "medium"),
                )
                all_recs.append(rec)
                node_tickers.append(self._rec_to_dict(rec))

            nodes_data.append({
                "layer": node.layer,
                "description": node.description,
                "position": node.position,
                "sensitivity": node.sensitivity,
                "tickers": node_tickers,
            })

        # Fetch prices
        self._enrich_with_prices(all_recs)

        return {
            "chain_id": chain_id,
            "chain_name": chain.name,
            "description": chain.description,
            "theme_color": chain.theme_color,
            "nodes": nodes_data,
            "total_tickers": len(all_recs),
            "catalyst_keywords": chain.catalyst_keywords,
        }

    def get_portfolio_view(self) -> Dict:
        """Get a cross-chain portfolio view of all recommended tickers."""
        all_recs = []

        for chain_id, chain in ALL_CHAINS.items():
            for node in chain.nodes:
                for t in node.tickers:
                    all_recs.append(ImpactRecommendation(
                        ticker=t["ticker"],
                        company=t["company"],
                        chain_id=chain_id,
                        chain_name=chain.name,
                        layer=node.layer,
                        direction="bullish" if t.get("exposure") != "negative" else "bearish",
                        conviction=node.sensitivity * 0.6,
                        impact_score=node.sensitivity * 80,
                        time_horizon=self._estimate_time_horizon(node.position),
                        thesis=t.get("notes", ""),
                        exposure=t.get("exposure", "medium"),
                    ))

        self._enrich_with_prices(all_recs)

        # Deduplicate tickers (some appear in multiple chains)
        seen = {}
        for rec in all_recs:
            if rec.ticker not in seen or rec.conviction > seen[rec.ticker].conviction:
                seen[rec.ticker] = rec

        unique_recs = sorted(seen.values(), key=lambda r: r.conviction * r.impact_score, reverse=True)

        return {
            "total_tickers": len(unique_recs),
            "chains_covered": len(ALL_CHAINS),
            "tickers": [self._rec_to_dict(r) for r in unique_recs],
            "by_chain": {
                cid: {
                    "name": chain.name,
                    "color": chain.theme_color,
                    "ticker_count": sum(1 for r in unique_recs if r.chain_id == cid),
                }
                for cid, chain in ALL_CHAINS.items()
            },
            "generated_at": datetime.utcnow().isoformat(),
        }

    def _propagate_through_chain(self, chain: ValueChain,
                                  relevance: float,
                                  event_type: str) -> List[ImpactRecommendation]:
        """Propagate an event's impact through all nodes of a chain."""
        recs = []

        for node in chain.nodes:
            # Impact decays slightly as you move downstream
            position_factor = 1.0 - (node.position * 0.05)
            impact = relevance * node.sensitivity * position_factor

            for t in node.tickers:
                # Exposure level affects conviction
                exposure_multiplier = {
                    "critical": 1.0, "high": 0.8, "medium": 0.5,
                    "low": 0.3, "negative": 0.7,
                }.get(t.get("exposure", "medium"), 0.5)

                direction = "bearish" if t.get("exposure") == "negative" else "bullish"

                conviction = min(impact * exposure_multiplier, 0.95)

                recs.append(ImpactRecommendation(
                    ticker=t["ticker"],
                    company=t["company"],
                    chain_id=chain.id,
                    chain_name=chain.name,
                    layer=node.layer,
                    direction=direction,
                    conviction=round(conviction, 2),
                    impact_score=round(impact * 100, 1),
                    time_horizon=self._estimate_time_horizon(node.position),
                    thesis=t.get("notes", ""),
                    exposure=t.get("exposure", "medium"),
                ))

        return recs

    def _estimate_time_horizon(self, position: int) -> str:
        """Upstream nodes react faster than downstream."""
        if position <= 1:
            return "immediate"
        elif position <= 3:
            return "short_term"
        elif position <= 5:
            return "medium_term"
        else:
            return "long_term"

    def _enrich_with_prices(self, recs: List[ImpactRecommendation]):
        """Add current price data to recommendations from the database."""
        if not recs:
            return

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        for rec in recs:
            row = conn.execute("""
                SELECT price, change_pct FROM ticker_prices
                WHERE ticker = ? ORDER BY timestamp DESC LIMIT 1
            """, (rec.ticker,)).fetchone()
            if row:
                rec.current_price = row["price"]
                rec.change_pct = row["change_pct"]
        conn.close()

    def _generate_summary(self, headline: str, chains: List[Dict],
                          top_picks: List[ImpactRecommendation]) -> str:
        """Generate a human-readable investment summary."""
        chain_names = [c["chain_name"] for c in chains]
        bullish = [r for r in top_picks if r.direction == "bullish"]
        bearish = [r for r in top_picks if r.direction == "bearish"]

        summary = f"This event impacts {len(chains)} value chain{'s' if len(chains) > 1 else ''}: {', '.join(chain_names)}. "

        if bullish:
            tickers = ', '.join(f"{r.ticker} ({r.company})" for r in bullish[:3])
            summary += f"Bullish for {tickers}. "

        if bearish:
            tickers = ', '.join(f"{r.ticker} ({r.company})" for r in bearish[:2])
            summary += f"Potential headwind for {tickers}. "

        if top_picks:
            best = top_picks[0]
            summary += f"Highest conviction: {best.ticker} ({best.conviction:.0%} confidence, {best.time_horizon.replace('_', ' ')} horizon)."

        return summary

    def _rec_to_dict(self, rec: ImpactRecommendation) -> Dict:
        """Serialize a recommendation."""
        return {
            "ticker": rec.ticker,
            "company": rec.company,
            "chain_id": rec.chain_id,
            "chain_name": rec.chain_name,
            "layer": rec.layer,
            "direction": rec.direction,
            "conviction": rec.conviction,
            "impact_score": rec.impact_score,
            "time_horizon": rec.time_horizon,
            "thesis": rec.thesis,
            "current_price": rec.current_price,
            "change_pct": rec.change_pct,
            "exposure": rec.exposure,
        }
