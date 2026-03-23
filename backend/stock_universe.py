"""
ThetaFlow - Stock Universe Engine

Maps SIC industry codes to value chain layers, enabling coverage of
ALL ~8,000 publicly listed US companies instead of just curated tickers.

Data source: SEC EDGAR company tickers JSON (free, no API key)
https://www.sec.gov/files/company_tickers.json
"""

import os
import json
import logging
import requests
import sqlite3
from typing import List, Dict, Optional
from datetime import datetime, timedelta

logger = logging.getLogger("thetaflow.universe")

# SIC codes mapped to value chain layers
# Format: SIC code range → (chain_id, layer_name, exposure_level)
SIC_TO_CHAIN = {
    # AI Infrastructure chain
    (3674, 3674): ("ai_infra", "Semiconductor Manufacturing", "high"),      # Semiconductors
    (3672, 3672): ("ai_infra", "Semiconductor Manufacturing", "medium"),    # Printed circuit boards
    (3661, 3669): ("ai_infra", "Networking & Memory", "medium"),            # Telephone & telegraph apparatus
    (3571, 3579): ("ai_infra", "Cloud Platforms", "medium"),                # Electronic computers
    (7372, 7372): ("ai_infra", "AI Application Layer (SaaS)", "medium"),    # Prepackaged software
    (7371, 7371): ("ai_infra", "AI Application Layer (SaaS)", "medium"),    # Computer programming
    (7374, 7374): ("ai_infra", "Cloud Platforms", "medium"),                # Computer processing & data prep
    (4911, 4911): ("ai_infra", "Nuclear / Power Generation", "medium"),     # Electric services
    (4931, 4931): ("ai_infra", "Nuclear / Power Generation", "medium"),     # Electric & other services combined
    (6798, 6798): ("ai_infra", "Data Center REITs & Builders", "high"),     # Real estate investment trusts
    (3825, 3825): ("ai_infra", "Cooling & Power Infrastructure", "medium"), # Instruments for measuring

    # Clean Energy chain
    (3674, 3674): ("clean_energy", "Solar & Wind Manufacturing", "medium"), # Semiconductors (solar cells)
    (3691, 3692): ("clean_energy", "Battery Manufacturing", "high"),        # Storage batteries
    (3711, 3711): ("clean_energy", "EV Manufacturers", "high"),             # Motor vehicles
    (3714, 3714): ("clean_energy", "EV Manufacturers", "medium"),           # Motor vehicle parts
    (5171, 5171): ("clean_energy", "Grid & Utilities", "medium"),           # Petroleum products wholesale
    (4911, 4911): ("clean_energy", "Grid & Utilities", "high"),             # Electric services
    (1040, 1040): ("clean_energy", "Critical Minerals & Mining", "high"),   # Gold mining
    (1090, 1090): ("clean_energy", "Critical Minerals & Mining", "high"),   # Metal mining NEC
    (2860, 2869): ("clean_energy", "Critical Minerals & Mining", "medium"), # Industrial chemicals

    # Biotech / GLP-1 chain
    (2836, 2836): ("biotech_glp1", "Drug Manufacturers", "critical"),       # Biological products
    (2834, 2834): ("biotech_glp1", "Drug Manufacturers", "high"),           # Pharmaceutical preparations
    (2835, 2835): ("biotech_glp1", "Drug Manufacturers", "high"),           # In vitro diagnostics
    (3841, 3841): ("biotech_glp1", "Drug Delivery & CDMO", "medium"),       # Surgical & medical instruments
    (3851, 3851): ("biotech_glp1", "Drug Delivery & CDMO", "medium"),       # Ophthalmic goods
    (5912, 5912): ("biotech_glp1", "Telehealth & Digital Health", "medium"),# Drug stores
    (8000, 8099): ("biotech_glp1", "Telehealth & Digital Health", "medium"),# Health services

    # Cybersecurity chain
    (7372, 7372): ("cybersecurity", "Endpoint & XDR", "high"),              # Prepackaged software
    (7371, 7371): ("cybersecurity", "Cloud Security", "high"),              # Computer programming services
    (3669, 3669): ("cybersecurity", "Network & Firewall", "medium"),        # Communications equipment NEC
    (7374, 7374): ("cybersecurity", "Identity & Access", "medium"),         # Computer processing services
}


class StockUniverse:
    """Manages the full universe of publicly listed stocks with industry mapping."""

    SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
    HEADERS = {"User-Agent": "ThetaFlow research@thetaflow.com"}

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_universe (
                cik INTEGER PRIMARY KEY,
                ticker TEXT NOT NULL,
                company_name TEXT NOT NULL,
                sic_code INTEGER,
                sic_description TEXT,
                chain_id TEXT,
                chain_layer TEXT,
                exposure TEXT DEFAULT 'medium',
                last_updated TEXT,
                UNIQUE(ticker)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_universe_ticker ON stock_universe(ticker)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_universe_chain ON stock_universe(chain_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_universe_sic ON stock_universe(sic_code)")
        conn.commit()
        conn.close()

    def load_from_sec(self) -> int:
        """Download full company list from SEC EDGAR and map to chains."""
        try:
            resp = requests.get(self.SEC_TICKERS_URL, headers=self.HEADERS, timeout=15)
            if resp.status_code != 200:
                logger.error(f"SEC EDGAR returned {resp.status_code}")
                return 0

            data = resp.json()
            conn = sqlite3.connect(self.db_path)
            now = datetime.utcnow().isoformat()
            count = 0

            for key, company in data.items():
                cik = company.get("cik_str")
                ticker = company.get("ticker", "").upper()
                name = company.get("title", "")

                if not ticker or not name:
                    continue

                # Look up SIC code for this company
                sic_code = self._get_sic_code(cik)

                # Map SIC to chain
                chain_id, chain_layer, exposure = self._map_sic_to_chain(sic_code)

                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_universe
                        (cik, ticker, company_name, sic_code, chain_id, chain_layer, exposure, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (cik, ticker, name, sic_code, chain_id, chain_layer, exposure, now))
                    count += 1
                except Exception:
                    pass

            conn.commit()
            conn.close()
            logger.info(f"Stock universe: loaded {count} companies from SEC EDGAR")
            return count

        except Exception as e:
            logger.error(f"Failed to load SEC EDGAR data: {e}")
            return 0

    def _get_sic_code(self, cik) -> Optional[int]:
        """Get SIC code for a company from SEC EDGAR."""
        # For now, we'll get SIC codes from the bulk download
        # The company_tickers.json doesn't include SIC, but company_tickers_exchange.json does
        return None  # Will be populated by the exchange-specific download

    def _map_sic_to_chain(self, sic_code: int) -> tuple:
        """Map a SIC code to a value chain layer."""
        if not sic_code:
            return (None, None, "medium")

        for (sic_low, sic_high), (chain_id, layer, exposure) in SIC_TO_CHAIN.items():
            if sic_low <= sic_code <= sic_high:
                return (chain_id, layer, exposure)

        return (None, None, "medium")

    def search_tickers(self, query: str, chain_id: str = None, limit: int = 20) -> List[Dict]:
        """Search the stock universe by name, ticker, or chain."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        if chain_id:
            rows = conn.execute("""
                SELECT * FROM stock_universe
                WHERE chain_id = ? AND (ticker LIKE ? OR company_name LIKE ?)
                ORDER BY company_name LIMIT ?
            """, (chain_id, f"%{query}%", f"%{query}%", limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM stock_universe
                WHERE ticker LIKE ? OR company_name LIKE ?
                ORDER BY company_name LIMIT ?
            """, (f"%{query}%", f"%{query}%", limit)).fetchall()

        conn.close()
        return [dict(r) for r in rows]

    def get_chain_companies(self, chain_id: str, layer: str = None, limit: int = 50) -> List[Dict]:
        """Get all companies mapped to a specific chain/layer."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        if layer:
            rows = conn.execute("""
                SELECT * FROM stock_universe
                WHERE chain_id = ? AND chain_layer = ?
                ORDER BY exposure DESC, company_name LIMIT ?
            """, (chain_id, layer, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM stock_universe WHERE chain_id = ?
                ORDER BY chain_layer, exposure DESC, company_name LIMIT ?
            """, (chain_id, limit)).fetchall()

        conn.close()
        return [dict(r) for r in rows]

    def get_universe_stats(self) -> Dict:
        """Get stats about the stock universe."""
        conn = sqlite3.connect(self.db_path)
        total = conn.execute("SELECT COUNT(*) FROM stock_universe").fetchone()[0]
        mapped = conn.execute("SELECT COUNT(*) FROM stock_universe WHERE chain_id IS NOT NULL").fetchone()[0]
        by_chain = conn.execute("""
            SELECT chain_id, COUNT(*) as cnt FROM stock_universe
            WHERE chain_id IS NOT NULL GROUP BY chain_id
        """).fetchall()
        conn.close()

        return {
            "total_companies": total,
            "chain_mapped": mapped,
            "unmapped": total - mapped,
            "by_chain": {r[0]: r[1] for r in by_chain},
        }


def load_sec_with_sic() -> List[Dict]:
    """Load companies with SIC codes from SEC EDGAR.
    Uses the company_tickers_exchange.json which includes exchange info."""
    try:
        # This endpoint includes more data
        url = "https://www.sec.gov/files/company_tickers_exchange.json"
        headers = {"User-Agent": "ThetaFlow research@thetaflow.com"}
        resp = requests.get(url, headers=headers, timeout=15)

        if resp.status_code != 200:
            # Fallback to basic tickers
            url = "https://www.sec.gov/files/company_tickers.json"
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                return []
            data = resp.json()
            return [{"cik": v["cik_str"], "ticker": v["ticker"], "name": v["title"]}
                    for v in data.values()]

        data = resp.json()
        fields = data.get("fields", [])
        rows = data.get("data", [])

        companies = []
        for row in rows:
            company = dict(zip(fields, row))
            companies.append({
                "cik": company.get("cik"),
                "ticker": company.get("ticker", "").upper(),
                "name": company.get("name", ""),
                "exchange": company.get("exchange", ""),
            })

        logger.info(f"Loaded {len(companies)} companies from SEC EDGAR")
        return companies

    except Exception as e:
        logger.error(f"SEC EDGAR load failed: {e}")
        return []
