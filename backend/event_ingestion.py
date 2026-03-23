"""
ThetaFlow - Event Ingestion Layer

Pulls catalysts from:
1. NewsAPI — breaking news, earnings, policy announcements
2. SEC EDGAR — 8-K filings (material events), 10-K/10-Q (financials)
3. FRED — macroeconomic indicators (GDP, CPI, unemployment, interest rates)
4. Yahoo Finance — earnings dates, price movements
"""

import os
import json
import time
import logging
import sqlite3
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger("thetaflow.events")


@dataclass
class CatalystEvent:
    """A single event that could impact value chains."""
    title: str
    source: str          # "newsapi", "sec_edgar", "fred", "yahoo"
    event_type: str      # "news", "filing", "macro", "earnings", "price_move"
    severity: str        # "low", "medium", "high", "critical"
    timestamp: str
    url: str = ""
    metadata: dict = None
    matched_chains: list = None  # Filled after chain matching

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.matched_chains is None:
            self.matched_chains = []


class EventDatabase:
    """SQLite storage for events and their chain impacts."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                event_type TEXT NOT NULL,
                severity TEXT DEFAULT 'medium',
                timestamp TEXT NOT NULL,
                url TEXT,
                metadata TEXT,
                matched_chains TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chain_impacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER,
                chain_id TEXT NOT NULL,
                chain_name TEXT,
                relevance_score REAL,
                impact_direction TEXT,  -- 'bullish', 'bearish', 'neutral'
                impact_magnitude REAL,  -- 0-1
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (event_id) REFERENCES events(id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ticker_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                price REAL,
                change_pct REAL,
                volume REAL,
                market_cap REAL,
                pe_ratio REAL,
                timestamp TEXT NOT NULL,
                UNIQUE(ticker, timestamp)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS macro_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                indicator TEXT NOT NULL,
                value REAL,
                previous_value REAL,
                change_pct REAL,
                period TEXT,
                timestamp TEXT NOT NULL,
                source TEXT DEFAULT 'fred'
            )
        """)
        # Reuse users/subscribers/alert_history from TrendSniper
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                tier TEXT DEFAULT 'free',
                api_key TEXT UNIQUE,
                stripe_customer_id TEXT,
                stripe_subscription_id TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_login_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                persona TEXT DEFAULT 'general',
                frequency TEXT DEFAULT 'weekly',
                verified INTEGER DEFAULT 0,
                verification_token TEXT,
                unsubscribe_token TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_sent_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                trigger_value REAL,
                sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
                subscriber_count INTEGER
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_source ON events(source)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker ON ticker_prices(ticker)")
        conn.commit()
        conn.close()

    def store_event(self, event: CatalystEvent) -> int:
        conn = sqlite3.connect(self.db_path)
        c = conn.execute("""
            INSERT INTO events (title, source, event_type, severity, timestamp, url, metadata, matched_chains)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.title, event.source, event.event_type, event.severity,
            event.timestamp, event.url,
            json.dumps(event.metadata), json.dumps(event.matched_chains)
        ))
        event_id = c.lastrowid
        conn.commit()
        conn.close()
        return event_id

    def store_price(self, ticker: str, price: float, change_pct: float,
                    volume: float = 0, market_cap: float = 0, pe_ratio: float = 0):
        conn = sqlite3.connect(self.db_path)
        now = datetime.utcnow().strftime("%Y-%m-%d")
        try:
            conn.execute("""
                INSERT OR REPLACE INTO ticker_prices (ticker, price, change_pct, volume, market_cap, pe_ratio, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ticker, price, change_pct, volume, market_cap, pe_ratio, now))
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to store price for {ticker}: {e}")
        conn.close()

    def get_recent_events(self, hours: int = 24, limit: int = 50) -> List[dict]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        rows = conn.execute("""
            SELECT * FROM events WHERE timestamp > ? ORDER BY timestamp DESC LIMIT ?
        """, (cutoff, limit)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_ticker_history(self, ticker: str, days: int = 30) -> List[dict]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT * FROM ticker_prices WHERE ticker = ? AND timestamp > ? ORDER BY timestamp ASC
        """, (ticker, cutoff)).fetchall()
        conn.close()
        return [dict(r) for r in rows]


# ── News Collector ──

class NewsCollector:
    """Pulls financial/macro news from NewsAPI."""

    BASE_URL = "https://newsapi.org/v2/everything"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("NEWSAPI_KEY")
        self.available = bool(self.api_key)

    def collect(self) -> List[CatalystEvent]:
        if not self.available:
            logger.warning("NewsAPI key not set")
            return []

        events = []
        now = datetime.utcnow().isoformat()
        week_ago = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d")

        # Financial/macro-focused queries
        queries = [
            "AI infrastructure investment",
            "data center construction",
            "semiconductor earnings",
            "federal reserve interest rate",
            "cybersecurity breach",
            "renewable energy policy",
            "GLP-1 obesity drug",
            "chip manufacturing",
            "nuclear energy data center",
            "cloud computing capex",
        ]

        seen_titles = set()
        for query in queries:
            try:
                resp = requests.get(self.BASE_URL, params={
                    "q": query, "from": week_ago, "sortBy": "relevancy",
                    "language": "en", "pageSize": 5, "apiKey": self.api_key,
                }, timeout=10)

                if resp.status_code != 200:
                    continue

                data = resp.json()
                for article in data.get("articles", []):
                    title = article.get("title", "")
                    if not title or len(title) < 15 or title in seen_titles:
                        continue
                    seen_titles.add(title)

                    source_name = article.get("source", {}).get("name", "unknown")
                    pub_date = article.get("publishedAt", now)

                    # Determine severity from source credibility
                    high_credibility = ["reuters", "bloomberg", "wsj", "financial times",
                                        "cnbc", "wall street journal", "sec.gov"]
                    severity = "high" if any(s in source_name.lower() for s in high_credibility) else "medium"

                    events.append(CatalystEvent(
                        title=title[:200],
                        source="newsapi",
                        event_type="news",
                        severity=severity,
                        timestamp=pub_date,
                        url=article.get("url", ""),
                        metadata={
                            "source_name": source_name,
                            "query": query,
                            "description": (article.get("description") or "")[:300],
                        }
                    ))

                time.sleep(1)
            except Exception as e:
                logger.error(f"NewsAPI error for '{query}': {e}")

        logger.info(f"NewsAPI: collected {len(events)} catalyst events")
        return events


# ── SEC EDGAR Collector ──

class SECEdgarCollector:
    """Pulls recent SEC filings (8-K material events) from EDGAR."""

    FULL_TEXT_URL = "https://efts.sec.gov/LATEST/search-index?q={query}&dateRange=custom&startdt={start}&enddt={end}&forms=8-K"
    RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=8-K&dateb=&owner=include&count=20&search_text=&action=getcompany&output=atom"

    HEADERS = {"User-Agent": "ThetaFlow/1.0 (research@thetaflow.com)"}

    def collect(self) -> List[CatalystEvent]:
        """Pull recent 8-K filings from SEC EDGAR full-text search."""
        events = []
        now = datetime.utcnow()
        start = (now - timedelta(days=3)).strftime("%Y-%m-%d")
        end = now.strftime("%Y-%m-%d")

        # Search for relevant 8-K filings
        search_terms = [
            "data center", "artificial intelligence", "semiconductor",
            "cybersecurity incident", "renewable energy", "nuclear",
        ]

        for term in search_terms:
            try:
                url = f"https://efts.sec.gov/LATEST/search-index?q=%22{term}%22&forms=8-K&dateRange=custom&startdt={start}&enddt={end}"
                resp = requests.get(url, headers=self.HEADERS, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    for hit in data.get("hits", {}).get("hits", [])[:5]:
                        source = hit.get("_source", {})
                        title = source.get("display_names", [term])[0] if source.get("display_names") else term
                        filing_date = source.get("file_date", now.isoformat())

                        events.append(CatalystEvent(
                            title=f"SEC 8-K: {title} — {term}",
                            source="sec_edgar",
                            event_type="filing",
                            severity="high",
                            timestamp=filing_date,
                            url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=8-K",
                            metadata={"search_term": term, "form_type": "8-K"}
                        ))
                time.sleep(1)  # SEC rate limit: 10 req/sec
            except Exception as e:
                logger.debug(f"SEC EDGAR error for '{term}': {e}")

        logger.info(f"SEC EDGAR: collected {len(events)} filing events")
        return events


# ── FRED Macro Collector ──

class FREDCollector:
    """Pulls macroeconomic indicators from FRED (Federal Reserve Economic Data).
    Free API, requires key from https://fred.stlouisfed.org/docs/api/api_key.html"""

    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    # Key macro indicators and their investment implications
    INDICATORS = {
        "DFF": {"name": "Federal Funds Rate", "impact": "Rate hikes hurt growth stocks, help banks"},
        "CPIAUCSL": {"name": "Consumer Price Index", "impact": "High inflation → Fed hawkish → tech sells off"},
        "UNRATE": {"name": "Unemployment Rate", "impact": "Rising unemployment → recession risk → defensive posture"},
        "GDP": {"name": "Real GDP", "impact": "GDP growth → bullish risk assets"},
        "T10Y2Y": {"name": "10Y-2Y Yield Spread", "impact": "Inversion → recession signal. Steepening → recovery"},
    }

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        self.available = bool(self.api_key)

    def collect(self) -> List[CatalystEvent]:
        if not self.available:
            logger.info("FRED_API_KEY not set — using macro summaries from news instead")
            return []

        events = []
        for series_id, info in self.INDICATORS.items():
            try:
                resp = requests.get(self.BASE_URL, params={
                    "series_id": series_id,
                    "api_key": self.api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 2,
                }, timeout=10)

                if resp.status_code == 200:
                    observations = resp.json().get("observations", [])
                    if len(observations) >= 2:
                        current = float(observations[0].get("value", 0))
                        previous = float(observations[1].get("value", 0))
                        change = current - previous
                        change_pct = (change / previous * 100) if previous else 0

                        severity = "high" if abs(change_pct) > 5 else "medium" if abs(change_pct) > 1 else "low"

                        events.append(CatalystEvent(
                            title=f"{info['name']}: {current:.2f} ({change_pct:+.1f}%)",
                            source="fred",
                            event_type="macro",
                            severity=severity,
                            timestamp=observations[0].get("date", datetime.utcnow().isoformat()),
                            metadata={
                                "series_id": series_id,
                                "indicator_name": info["name"],
                                "current_value": current,
                                "previous_value": previous,
                                "change_pct": round(change_pct, 2),
                                "impact_thesis": info["impact"],
                            }
                        ))
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"FRED error for {series_id}: {e}")

        logger.info(f"FRED: collected {len(events)} macro indicators")
        return events


# ── Yahoo Finance Price Collector ──

class YahooFinanceCollector:
    """Pulls live stock quotes for all tickers in value chains."""

    def collect_prices(self, tickers: List[str]) -> Dict[str, Dict]:
        """Fetch current prices for a list of tickers."""
        results = {}
        for ticker in tickers:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                resp = requests.get(url, headers={"User-Agent": "ThetaFlow/1.0"},
                                    params={"interval": "1d", "range": "5d"}, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
                    price = meta.get("regularMarketPrice", 0)
                    prev = meta.get("chartPreviousClose", meta.get("previousClose", price))
                    change = price - prev
                    change_pct = (change / prev * 100) if prev else 0

                    results[ticker] = {
                        "price": round(price, 2),
                        "change": round(change, 2),
                        "change_pct": round(change_pct, 2),
                        "volume": meta.get("regularMarketVolume", 0),
                        "market_cap": meta.get("marketCap", 0),
                        "currency": meta.get("currency", "USD"),
                    }
                time.sleep(0.3)  # Rate limiting
            except Exception as e:
                logger.debug(f"Yahoo Finance error for {ticker}: {e}")

        logger.info(f"Yahoo Finance: fetched prices for {len(results)}/{len(tickers)} tickers")
        return results


# ── Event Orchestrator ──

class EventOrchestrator:
    """Coordinates event collection across all sources."""

    def __init__(self, db: EventDatabase):
        self.db = db
        self.news = NewsCollector()
        self.sec = SECEdgarCollector()
        self.fred = FREDCollector()
        self.yahoo = YahooFinanceCollector()

    def run_collection(self) -> Dict[str, int]:
        """Run full event collection cycle."""
        from value_chains import find_chains_for_event, get_chain_tickers, ALL_CHAINS

        counts = {}
        logger.info("Starting ThetaFlow event collection...")

        # 1. Collect events from all sources
        news_events = self.news.collect()
        counts["news"] = len(news_events)

        sec_events = self.sec.collect()
        counts["sec_filings"] = len(sec_events)

        fred_events = self.fred.collect()
        counts["macro"] = len(fred_events)

        all_events = news_events + sec_events + fred_events

        # 2. Match events to value chains
        for event in all_events:
            matches = find_chains_for_event(event.title)
            event.matched_chains = matches
            event_id = self.db.store_event(event)

            # Store chain impacts
            conn = sqlite3.connect(self.db.db_path)
            for match in matches:
                direction = "bullish"  # Default — could use NLP for sentiment
                conn.execute("""
                    INSERT INTO chain_impacts (event_id, chain_id, chain_name, relevance_score, impact_direction, impact_magnitude)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (event_id, match["chain_id"], match["chain_name"],
                      match["relevance_score"], direction, match["relevance_score"]))
            conn.commit()
            conn.close()

        counts["chain_matches"] = sum(len(e.matched_chains) for e in all_events)

        # 3. Fetch prices for all tickers across all chains
        all_tickers = set()
        for chain in ALL_CHAINS.values():
            for node in chain.nodes:
                for t in node.tickers:
                    all_tickers.add(t["ticker"])

        prices = self.yahoo.collect_prices(list(all_tickers))
        for ticker, price_data in prices.items():
            self.db.store_price(
                ticker, price_data["price"], price_data["change_pct"],
                price_data.get("volume", 0), price_data.get("market_cap", 0)
            )
        counts["prices"] = len(prices)

        total = sum(counts.values())
        logger.info(f"ThetaFlow collection complete: {total} data points ({counts})")
        return counts
