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
    """Pull financial data by scraping Yahoo Finance public page.
    The API endpoints require auth now, but the HTML page embeds all data."""
    try:
        # Step 1: Get price + 52-week from chart API (always works)
        basic = _get_basic_quote(ticker)
        if not basic:
            return None

        result = {**basic}

        # Step 2: Scrape key metrics from Yahoo Finance page
        url = f"https://finance.yahoo.com/quote/{ticker}/"
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        resp = requests.get(url, headers=headers, timeout=10)

        if resp.status_code == 200:
            html = resp.text

            def _scrape_metric(label, html_text):
                """Extract a metric value from Yahoo's HTML."""
                import re
                # Pattern: label text followed by value in next element
                patterns = [
                    rf'{label}.*?class="value[^"]*"[^>]*>([^<]+)',
                    rf'{label}[^<]*</\w+>\s*<\w+[^>]*>([^<]+)',
                ]
                for pat in patterns:
                    m = re.search(pat, html_text, re.IGNORECASE | re.DOTALL)
                    if m:
                        val = m.group(1).strip()
                        return val
                return None

            def _parse_number(s):
                if not s or s == 'N/A' or s == '--':
                    return None
                s = s.replace(',', '').strip()
                try:
                    if 'T' in s:
                        return float(s.replace('T','')) * 1e12
                    if 'B' in s:
                        return float(s.replace('B','')) * 1e9
                    if 'M' in s:
                        return float(s.replace('M','')) * 1e6
                    if '%' in s:
                        return float(s.replace('%',''))
                    return float(s)
                except:
                    return None

            # Extract key metrics
            pe_raw = _scrape_metric("PE Ratio", html)
            fwd_pe_raw = _scrape_metric("Forward P/E", html)
            mkt_cap_raw = _scrape_metric("Market Cap", html)
            eps_raw = _scrape_metric("EPS", html)
            beta_raw = _scrape_metric("Beta", html)

            pe_val = _parse_number(pe_raw)
            fwd_pe_val = _parse_number(fwd_pe_raw)
            mkt_cap_val = _parse_number(mkt_cap_raw)
            beta_val = _parse_number(beta_raw)

            if pe_val:
                result["pe_trailing"] = round(pe_val, 1)
            if fwd_pe_val:
                result["pe_forward"] = round(fwd_pe_val, 1)
            if mkt_cap_val:
                result["market_cap"] = mkt_cap_val
                result["market_cap_str"] = _format_large_number(mkt_cap_val)
            if beta_val:
                result["beta"] = round(beta_val, 2)

            # 52-week data from chart API
            if result.get("fifty_two_high") and result.get("price"):
                result["distance_from_high_pct"] = round(
                    ((result["price"] - result["fifty_two_high"]) / result["fifty_two_high"] * 100), 1
                )

        return result
    except Exception as e:
        logger.debug(f"Full financials failed for {ticker}: {e}")
        return _get_basic_quote(ticker)


def _get_basic_quote(ticker: str) -> Optional[Dict]:
    """Basic price + 52-week data from chart endpoint (always works, no auth)."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        resp = requests.get(url, headers={"User-Agent": "ThetaFlow/1.0"},
                           params={"interval": "1d", "range": "1mo"}, timeout=5)
        if resp.status_code != 200:
            return None
        meta = resp.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
        price = meta.get("regularMarketPrice", 0)
        prev = meta.get("chartPreviousClose", price)
        high52 = meta.get("fiftyTwoWeekHigh")
        low52 = meta.get("fiftyTwoWeekLow")
        return {
            "ticker": ticker,
            "price": round(price, 2),
            "change": round(price - prev, 2),
            "change_pct": round((price - prev) / prev * 100, 2) if prev else 0,
            "market_cap": meta.get("marketCap"),
            "market_cap_str": _format_large_number(meta.get("marketCap", 0)),
            "fifty_two_high": round(high52, 2) if high52 else None,
            "fifty_two_low": round(low52, 2) if low52 else None,
            "volume": meta.get("regularMarketVolume"),
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
