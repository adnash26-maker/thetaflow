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
import math
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger("thetaflow.financials")

# ── In-memory cache with TTL ──
_quote_cache: Dict[str, tuple] = {}  # ticker -> (data, timestamp)
_CACHE_TTL = 300  # 5 minutes


def _cache_get(ticker: str) -> Optional[Dict]:
    if ticker in _quote_cache:
        data, cached_at = _quote_cache[ticker]
        if (datetime.utcnow() - cached_at).total_seconds() < _CACHE_TTL:
            return data
    return None


def _cache_set(ticker: str, data: Dict):
    _quote_cache[ticker] = (data, datetime.utcnow())


def get_full_financials(ticker: str) -> Optional[Dict]:
    """Fast financial data from Yahoo Finance chart API only (no slow page scrape).
    Uses in-memory cache with 5-minute TTL."""
    cached = _cache_get(ticker)
    if cached:
        return cached

    result = _get_basic_quote(ticker)
    if result:
        # Compute distance from 52-week high
        if result.get("fifty_two_high") and result.get("price"):
            result["distance_from_high_pct"] = round(
                ((result["price"] - result["fifty_two_high"]) / result["fifty_two_high"] * 100), 1
            )
        _cache_set(ticker, result)
    return result


def get_full_financials_batch(tickers: List[str], max_workers: int = 8) -> Dict[str, Dict]:
    """Fetch financials for multiple tickers in parallel.
    Returns {ticker: financial_data} dict."""
    results = {}

    # Check cache first
    uncached = []
    for t in tickers:
        cached = _cache_get(t)
        if cached:
            results[t] = cached
        else:
            uncached.append(t)

    if not uncached:
        return results

    # Fetch uncached tickers in parallel
    with ThreadPoolExecutor(max_workers=min(max_workers, len(uncached))) as executor:
        futures = {executor.submit(_get_basic_quote, t): t for t in uncached}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                data = future.result()
                if data:
                    if data.get("fifty_two_high") and data.get("price"):
                        data["distance_from_high_pct"] = round(
                            ((data["price"] - data["fifty_two_high"]) / data["fifty_two_high"] * 100), 1
                        )
                    _cache_set(ticker, data)
                    results[ticker] = data
            except Exception:
                pass

    return results


def _get_basic_quote(ticker: str) -> Optional[Dict]:
    """Fast price + 52-week data from chart endpoint (single HTTP call, no scraping)."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        resp = requests.get(url, headers={"User-Agent": "ThetaFlow/1.0"},
                           params={"interval": "1d", "range": "1mo"}, timeout=3)
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


# ── Price History & Projection Cone ──

_price_history_cache = {}


def get_price_history(ticker: str, months: int = 6) -> Optional[Dict]:
    """Fetch daily price history from Yahoo Finance chart API."""
    cache_key = f"{ticker}_{months}"
    now = datetime.utcnow()

    if cache_key in _price_history_cache:
        cached, cached_at = _price_history_cache[cache_key]
        if (now - cached_at).total_seconds() < 300:
            return cached

    try:
        range_map = {1: "1mo", 3: "3mo", 6: "6mo", 12: "1y"}
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        resp = requests.get(url, headers={"User-Agent": "ThetaFlow/1.0"},
                           params={"interval": "1d", "range": range_map.get(months, "6mo")},
                           timeout=8)
        if resp.status_code != 200:
            return None

        chart = resp.json().get("chart", {}).get("result", [{}])[0]
        timestamps = chart.get("timestamp", [])
        quotes = chart.get("indicators", {}).get("quote", [{}])[0]
        closes_raw = quotes.get("close", [])

        if not timestamps or not closes_raw:
            return None

        # Zip and filter out None values
        dates = []
        closes = []
        for ts, c in zip(timestamps, closes_raw):
            if c is not None and c > 0:
                dates.append(datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d"))
                closes.append(round(c, 2))

        if len(closes) < 20:
            return None

        result = {
            "ticker": ticker,
            "dates": dates,
            "closes": closes,
            "current_price": closes[-1],
        }

        _price_history_cache[cache_key] = (result, now)
        return result
    except Exception as e:
        logger.debug(f"Price history failed for {ticker}: {e}")
        return None


def compute_projection_cone(closes: List[float], direction: str,
                            conviction: float, days_forward: int = 30) -> Optional[Dict]:
    """Compute projection cone using Geometric Brownian Motion."""
    if len(closes) < 20:
        return None

    # Calculate daily log returns
    log_returns = []
    for i in range(1, len(closes)):
        if closes[i] > 0 and closes[i - 1] > 0:
            log_returns.append(math.log(closes[i] / closes[i - 1]))

    if len(log_returns) < 10:
        return None

    # Historical volatility
    mean_return = sum(log_returns) / len(log_returns)
    variance = sum((r - mean_return) ** 2 for r in log_returns) / (len(log_returns) - 1)
    daily_vol = math.sqrt(variance)

    # Direction-adjusted drift
    sign = 1.0 if direction == "bullish" else -1.0
    max_daily_drift = 0.003  # ~9% over 30 days at max conviction
    drift = sign * conviction * max_daily_drift

    last_price = closes[-1]
    today = datetime.utcnow()

    center = []
    upper_1sd = []
    lower_1sd = []
    upper_2sd = []
    lower_2sd = []
    proj_dates = []
    trading_day = 0

    for day in range(1, days_forward + 1):
        date = today + timedelta(days=day)
        if date.weekday() >= 5:
            continue
        trading_day += 1

        proj_dates.append(date.strftime("%Y-%m-%d"))
        t = trading_day

        center_log = math.log(last_price) + drift * t
        vol_spread = daily_vol * math.sqrt(t)

        center.append(round(math.exp(center_log), 2))
        upper_1sd.append(round(math.exp(center_log + vol_spread), 2))
        lower_1sd.append(round(math.exp(center_log - vol_spread), 2))
        upper_2sd.append(round(math.exp(center_log + 2 * vol_spread), 2))
        lower_2sd.append(round(math.exp(center_log - 2 * vol_spread), 2))

    return {
        "center": center,
        "upper_1sd": upper_1sd,
        "lower_1sd": lower_1sd,
        "upper_2sd": upper_2sd,
        "lower_2sd": lower_2sd,
        "dates": proj_dates,
        "daily_vol": round(daily_vol, 5),
        "drift": round(drift, 5),
    }


def get_ticker_chart_data(ticker: str, direction: str = "bullish",
                          conviction: float = 0.5) -> Optional[Dict]:
    """Get complete chart data: 6-month history + projection cone."""
    history = get_price_history(ticker)
    if not history or not history.get("closes"):
        return None

    projection = compute_projection_cone(
        history["closes"], direction, conviction
    )

    return {
        "ticker": ticker,
        "history": {
            "dates": history["dates"],
            "closes": history["closes"],
        },
        "projection": projection,
    }


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
