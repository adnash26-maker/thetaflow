"""
ThetaFlow - Financial Data Engine

Pulls real financial metrics from Yahoo Finance for investment-grade analysis:
- P/E ratio, forward P/E, PEG ratio
- Revenue, revenue growth, profit margins
- Market cap, enterprise value
- 52-week high/low, distance from highs
- Analyst price targets (consensus)
- Short interest, institutional ownership
"""

import logging
import requests
from typing import Dict, Optional
from functools import lru_cache
from datetime import datetime

logger = logging.getLogger("thetaflow.financials")


def get_full_financials(ticker: str) -> Optional[Dict]:
    """Pull comprehensive financial data from Yahoo Finance.
    Uses the quoteSummary endpoint which returns everything in one call."""
    try:
        # Yahoo Finance quoteSummary — returns detailed financials
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
        modules = "price,summaryDetail,defaultKeyStatistics,financialData,earningsTrend"
        resp = requests.get(url, params={"modules": modules},
                           headers={"User-Agent": "ThetaFlow/1.0"}, timeout=8)

        if resp.status_code != 200:
            # Fallback to simpler chart endpoint
            return _get_basic_quote(ticker)

        data = resp.json().get("quoteSummary", {}).get("result", [])
        if not data:
            return _get_basic_quote(ticker)

        result = data[0]
        price = result.get("price", {})
        summary = result.get("summaryDetail", {})
        key_stats = result.get("defaultKeyStatistics", {})
        financials = result.get("financialData", {})

        def _val(obj, key, default=None):
            """Extract 'raw' value from Yahoo's nested format."""
            v = obj.get(key, {})
            if isinstance(v, dict):
                return v.get("raw", v.get("fmt", default))
            return v if v else default

        current_price = _val(price, "regularMarketPrice", 0)
        prev_close = _val(price, "regularMarketPreviousClose", current_price)
        change = round(current_price - prev_close, 2) if current_price and prev_close else 0
        change_pct = round((change / prev_close * 100), 2) if prev_close else 0

        fifty_two_high = _val(summary, "fiftyTwoWeekHigh", 0)
        fifty_two_low = _val(summary, "fiftyTwoWeekLow", 0)
        distance_from_high = round(((current_price - fifty_two_high) / fifty_two_high * 100), 1) if fifty_two_high else 0

        market_cap = _val(price, "marketCap", 0)
        market_cap_str = _format_large_number(market_cap) if market_cap else "N/A"

        pe_trailing = _val(summary, "trailingPE")
        pe_forward = _val(summary, "forwardPE") or _val(key_stats, "forwardPE")
        peg_ratio = _val(key_stats, "pegRatio")

        revenue_growth = _val(financials, "revenueGrowth")
        profit_margin = _val(financials, "profitMargins")
        operating_margin = _val(financials, "operatingMargins")
        revenue = _val(financials, "totalRevenue")
        revenue_str = _format_large_number(revenue) if revenue else "N/A"

        target_mean = _val(financials, "targetMeanPrice")
        target_high = _val(financials, "targetHighPrice")
        target_low = _val(financials, "targetLowPrice")
        num_analysts = _val(financials, "numberOfAnalystOpinions")
        recommendation = _val(financials, "recommendationKey", "none")

        short_ratio = _val(key_stats, "shortRatio")
        short_pct = _val(key_stats, "shortPercentOfFloat")
        beta = _val(key_stats, "beta")
        enterprise_value = _val(key_stats, "enterpriseValue")

        # Upside/downside to analyst target
        upside_pct = round(((target_mean - current_price) / current_price * 100), 1) if target_mean and current_price else None

        return {
            "ticker": ticker,
            "price": round(current_price, 2),
            "change": change,
            "change_pct": change_pct,
            "market_cap": market_cap,
            "market_cap_str": market_cap_str,
            "pe_trailing": round(pe_trailing, 1) if pe_trailing else None,
            "pe_forward": round(pe_forward, 1) if pe_forward else None,
            "peg_ratio": round(peg_ratio, 2) if peg_ratio else None,
            "revenue": revenue,
            "revenue_str": revenue_str,
            "revenue_growth": round(revenue_growth * 100, 1) if revenue_growth else None,
            "profit_margin": round(profit_margin * 100, 1) if profit_margin else None,
            "operating_margin": round(operating_margin * 100, 1) if operating_margin else None,
            "fifty_two_high": round(fifty_two_high, 2) if fifty_two_high else None,
            "fifty_two_low": round(fifty_two_low, 2) if fifty_two_low else None,
            "distance_from_high_pct": distance_from_high,
            "analyst_target": round(target_mean, 2) if target_mean else None,
            "analyst_target_high": round(target_high, 2) if target_high else None,
            "analyst_target_low": round(target_low, 2) if target_low else None,
            "analyst_count": num_analysts,
            "analyst_upside_pct": upside_pct,
            "recommendation": recommendation,
            "short_ratio": round(short_ratio, 1) if short_ratio else None,
            "short_pct_float": round(short_pct * 100, 1) if short_pct else None,
            "beta": round(beta, 2) if beta else None,
            "enterprise_value": enterprise_value,
        }
    except Exception as e:
        logger.debug(f"Full financials failed for {ticker}: {e}")
        return _get_basic_quote(ticker)


def _get_basic_quote(ticker: str) -> Optional[Dict]:
    """Fallback: basic price data from chart endpoint."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        resp = requests.get(url, headers={"User-Agent": "ThetaFlow/1.0"},
                           params={"interval": "1d", "range": "5d"}, timeout=5)
        if resp.status_code != 200:
            return None
        meta = resp.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
        price = meta.get("regularMarketPrice", 0)
        prev = meta.get("chartPreviousClose", price)
        return {
            "ticker": ticker,
            "price": round(price, 2),
            "change": round(price - prev, 2),
            "change_pct": round((price - prev) / prev * 100, 2) if prev else 0,
            "market_cap": meta.get("marketCap"),
            "market_cap_str": _format_large_number(meta.get("marketCap", 0)),
        }
    except:
        return None


def generate_investment_thesis(ticker_data: Dict, chain_context: str,
                                layer: str, exposure: str,
                                catalyst_headline: str) -> str:
    """Generate actual investment-grade analysis from financial data."""
    t = ticker_data
    ticker = t.get("ticker", "")
    price = t.get("price", 0)
    lines = []

    # Valuation assessment
    pe_fwd = t.get("pe_forward")
    pe_trail = t.get("pe_trailing")
    peg = t.get("peg_ratio")
    rev_growth = t.get("revenue_growth")

    if pe_fwd and pe_fwd > 0:
        if pe_fwd > 50:
            lines.append(f"Valuation is stretched at {pe_fwd:.0f}x forward earnings — market already pricing in significant growth. New catalysts may have limited upside unless they exceed consensus.")
        elif pe_fwd > 30:
            lines.append(f"Trading at {pe_fwd:.0f}x forward earnings — premium valuation reflects strong growth expectations. Catalyst needs to move estimates higher to drive the stock.")
        elif pe_fwd > 15:
            lines.append(f"Reasonable {pe_fwd:.0f}x forward P/E. If this catalyst drives earnings revisions upward, there's room for multiple expansion.")
        else:
            lines.append(f"Attractive {pe_fwd:.0f}x forward P/E — the market may be underpricing the growth opportunity from this catalyst.")

    if peg and peg > 0:
        if peg < 1:
            lines.append(f"PEG ratio of {peg:.1f} suggests the stock is undervalued relative to its growth rate — favorable entry point.")
        elif peg > 2:
            lines.append(f"PEG of {peg:.1f} — growth premium is high. Need to see earnings beats to justify current valuation.")

    # Revenue growth context
    if rev_growth is not None:
        if rev_growth > 30:
            lines.append(f"Revenue growing {rev_growth:.0f}% YoY — strong organic momentum. This catalyst could accelerate an already fast-growing top line.")
        elif rev_growth > 10:
            lines.append(f"Solid {rev_growth:.0f}% revenue growth. Catalyst could push this into a higher growth bracket if it drives incremental demand.")
        elif rev_growth > 0:
            lines.append(f"Modest {rev_growth:.0f}% revenue growth — this catalyst could be the inflection point for re-acceleration.")
        else:
            lines.append(f"Revenue declining {rev_growth:.0f}% — this catalyst is needed to reverse the trajectory. Higher risk/reward.")

    # Margin analysis
    margin = t.get("profit_margin")
    if margin is not None:
        if margin > 25:
            lines.append(f"Strong {margin:.0f}% profit margins — incremental revenue from this catalyst flows through to earnings at high rates.")
        elif margin > 0:
            lines.append(f"Moderate {margin:.0f}% margins — revenue growth matters more than profitability improvement here.")
        else:
            lines.append(f"Currently unprofitable ({margin:.0f}% margin) — this is a growth story. Catalyst value is in revenue acceleration, not near-term earnings.")

    # Technical positioning
    dist = t.get("distance_from_high_pct")
    if dist is not None:
        if dist > -5:
            lines.append(f"Trading within 5% of 52-week high — strong momentum, but less margin of safety. Consider waiting for a pullback to scale in.")
        elif dist < -30:
            lines.append(f"Trading {abs(dist):.0f}% below 52-week high — significant mean reversion potential if this catalyst changes the narrative.")
        elif dist < -15:
            lines.append(f"Down {abs(dist):.0f}% from highs — room for recovery. This catalyst could trigger a re-rating.")

    # Analyst consensus
    upside = t.get("analyst_upside_pct")
    target = t.get("analyst_target")
    count = t.get("analyst_count")
    if upside is not None and target and count:
        if upside > 20:
            lines.append(f"Analysts see {upside:.0f}% upside to ${target:.0f} consensus target ({count} analysts). Wall Street already bullish — catalyst could drive target revisions even higher.")
        elif upside > 0:
            lines.append(f"Analyst target of ${target:.0f} implies {upside:.0f}% upside ({count} analysts). Moderate upside — catalyst may not be fully reflected in consensus yet.")
        else:
            lines.append(f"Trading above analyst consensus target of ${target:.0f} — stock has run ahead of estimates. Need earnings revisions to sustain.")

    # Short interest
    short_pct = t.get("short_pct_float")
    if short_pct and short_pct > 10:
        lines.append(f"Elevated short interest at {short_pct:.0f}% of float — a positive catalyst could trigger a short squeeze amplifying upside.")

    # Risk assessment
    beta = t.get("beta")
    if beta and beta > 1.5:
        lines.append(f"High beta ({beta:.1f}) — expect amplified moves in both directions. Position size accordingly.")

    # Exposure context
    if exposure == "critical":
        lines.append(f"This is a direct, high-exposure play on this catalyst within the {layer} layer.")
    elif exposure == "negative":
        lines.append(f"CAUTION: This company faces headwinds from this catalyst. Consider as a hedge or short candidate.")

    if not lines:
        lines.append(f"Limited financial data available for detailed analysis.")

    return " ".join(lines)


def _format_large_number(n) -> str:
    if not n or n == 0:
        return "N/A"
    if n >= 1e12:
        return f"${n/1e12:.1f}T"
    if n >= 1e9:
        return f"${n/1e9:.1f}B"
    if n >= 1e6:
        return f"${n/1e6:.0f}M"
    return f"${n:,.0f}"
