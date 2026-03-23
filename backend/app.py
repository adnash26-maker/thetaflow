"""
ThetaFlow - Investment Intelligence Platform

Ingests current events, maps them to value chains, simulates impact
propagation, and generates investment recommendations across the
full supply chain — from raw materials to end applications.
"""

import os
import sys
import io
import csv
import json
import logging
import sqlite3
import re
import time
import uuid
import requests
import hashlib
import secrets
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from flask import Flask, jsonify, request, send_from_directory, Response, redirect, session
from flask_cors import CORS
from functools import wraps

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from event_ingestion import EventDatabase, EventOrchestrator
from impact_engine import ImpactEngine
from value_chains import ALL_CHAINS, find_chains_for_event, get_chain_tickers
from stock_universe import StockUniverse, load_sec_with_sic
from ai_analyst import AIAnalyst

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("thetaflow.api")

_frontend_dir = os.getenv("THETAFLOW_FRONTEND", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend"))
app = Flask(__name__, static_folder=_frontend_dir, static_url_path="")
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))
CORS(app, supports_credentials=True)

DB_PATH = os.getenv("THETAFLOW_DB", os.path.join(os.path.expanduser("~"), "thetaflow.db"))

db = EventDatabase(DB_PATH)
orchestrator = EventOrchestrator(db)
engine = ImpactEngine(DB_PATH)
universe = StockUniverse(DB_PATH)
analyst = AIAnalyst()

last_collection_at = None

# ── Auth (reused from TrendSniper) ──

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"{salt}:{h.hex()}"

def verify_password(password: str, stored: str) -> bool:
    salt, h = stored.split(':')
    return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex() == h

def auth_required(tier_minimum=None):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            user = None
            api_key = request.headers.get('X-API-Key')
            if api_key:
                conn = sqlite3.connect(DB_PATH)
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT * FROM users WHERE api_key = ?", (api_key,)).fetchone()
                conn.close()
                if row:
                    user = dict(row)
            if not user and 'user_id' in session:
                conn = sqlite3.connect(DB_PATH)
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT * FROM users WHERE id = ?", (session['user_id'],)).fetchone()
                conn.close()
                if row:
                    user = dict(row)
            if not user:
                return jsonify({"error": "Authentication required"}), 401
            if tier_minimum:
                tiers = {'free': 0, 'starter': 1, 'pro': 2, 'enterprise': 3}
                if tiers.get(user.get('tier', 'free'), 0) < tiers.get(tier_minimum, 0):
                    return jsonify({"error": f"Requires {tier_minimum} tier"}), 403
            request.current_user = user
            return f(*args, **kwargs)
        return wrapped
    return decorator

# ── Pages ──

def _find_html(filename):
    """Find and read an HTML file - tries every possible path."""
    candidates = [
        os.getenv("THETAFLOW_FRONTEND", ""),
        os.path.dirname(os.path.abspath(__file__)),
        os.getcwd(),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend"),
        os.path.join(os.getcwd(), "frontend"),
    ]
    for d in candidates:
        path = os.path.join(d, filename) if d else ""
        if path and os.path.isfile(path):
            with open(path, "r") as f:
                return f.read()
    return None

@app.route("/")
def serve_landing():
    html = _find_html("landing.html")
    if html:
        return Response(html, mimetype="text/html")
    return "Landing page not found", 404

@app.route("/dashboard")
def serve_dashboard():
    html = _find_html("dashboard.html")
    if html:
        return Response(html, mimetype="text/html")
    return "Dashboard not found", 404

# ── Core API ──

@app.route("/api/debug-paths")
def debug_paths():
    """Temporary debug route to find frontend directory on Railway."""
    import glob
    cwd = os.getcwd()
    app_file = os.path.abspath(__file__)
    candidates = {
        "cwd": cwd,
        "app_file": app_file,
        "env_frontend": os.getenv("THETAFLOW_FRONTEND", "not set"),
        "cwd_contents": os.listdir(cwd) if os.path.isdir(cwd) else "not a dir",
    }
    # Search for dashboard.html
    for root_dir in [cwd, os.path.dirname(app_file), "/app", "/opt"]:
        for f in glob.glob(os.path.join(root_dir, "**", "dashboard.html"), recursive=True):
            candidates[f"found_{f}"] = True
    return jsonify(candidates)

@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "product": "ThetaFlow",
        "timestamp": datetime.utcnow().isoformat(),
        "last_collection": last_collection_at,
        "chains_available": len(ALL_CHAINS),
    })

@app.route("/api/chains", methods=["GET"])
def get_chains():
    """List all available value chains."""
    chains = []
    for cid, chain in ALL_CHAINS.items():
        total_tickers = sum(len(n.tickers) for n in chain.nodes)
        chains.append({
            "id": cid,
            "name": chain.name,
            "description": chain.description,
            "theme_color": chain.theme_color,
            "node_count": len(chain.nodes),
            "ticker_count": total_tickers,
            "catalyst_keywords": chain.catalyst_keywords[:5],
        })
    return jsonify({"success": True, "chains": chains})

@app.route("/api/chains/<chain_id>", methods=["GET"])
def get_chain_detail(chain_id):
    """Get full analysis for a specific value chain with live prices."""
    analysis = engine.get_chain_analysis(chain_id)
    if "error" in analysis:
        return jsonify({"success": False, "error": analysis["error"]}), 404
    return jsonify({"success": True, **analysis})

@app.route("/api/top-headlines", methods=["GET"])
def get_top_headlines():
    """Get one top headline per major category from WSJ RSS feeds."""
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime

    feeds = {
        "Technology": {"url": "https://feeds.a.dj.com/rss/RSSWSJD.xml", "color": "#8b5cf6"},
        "Markets": {"url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml", "color": "#6366f1"},
        "Business": {"url": "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml", "color": "#10b981"},
        "World": {"url": "https://feeds.a.dj.com/rss/RSSWorldNews.xml", "color": "#f59e0b"},
    }

    headlines = []
    for category, info in feeds.items():
        try:
            resp = requests.get(info["url"], headers={"User-Agent": "ThetaFlow/1.0"}, timeout=6)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            item = root.find(".//item")
            if item is not None:
                title = (item.findtext("title") or "").strip()
                if title and len(title) > 15:
                    headlines.append({
                        "category": category,
                        "title": title[:120],
                        "url": item.findtext("link", ""),
                        "color": info["color"],
                        "source": "WSJ",
                    })
        except Exception:
            pass

    return jsonify({"success": True, "headlines": headlines})

@app.route("/api/signals", methods=["GET"])
def get_signals():
    """Get today's top events pre-analyzed with chain impacts and ticker recommendations.
    This is the primary dashboard endpoint — shows what happened and what it means."""
    hours = request.args.get("hours", 72, type=int)
    limit = request.args.get("limit", 10, type=int)

    events = db.get_recent_events(hours=hours, limit=50)
    # Prioritize WSJ and news headlines over SEC filings
    priority = {"wsj": 0, "newsapi": 1, "fred": 2, "sec_edgar": 3, "yahoo": 4}
    events.sort(key=lambda e: (priority.get(e.get("source", ""), 5), e.get("timestamp", "")), reverse=False)
    events.sort(key=lambda e: priority.get(e.get("source", ""), 5))
    signals = []

    for event in events:
        try:
            matched_chains = json.loads(event.get("matched_chains", "[]"))
        except (json.JSONDecodeError, TypeError):
            matched_chains = []

        # Only show events that matched at least one chain
        if not matched_chains:
            # Try re-analyzing (event might have been stored before keyword improvements)
            matched_chains = find_chains_for_event(event["title"])

        if not matched_chains:
            continue

        # Get top ticker recommendations for this event
        analysis = engine.analyze_event(event["title"], event.get("event_type", "news"))
        top_picks = analysis.get("top_picks", [])[:3]

        signals.append({
            "title": event["title"],
            "source": event["source"],
            "severity": event["severity"],
            "timestamp": event["timestamp"],
            "url": event.get("url", ""),
            "chains": matched_chains[:2],
            "top_picks": top_picks,
            "summary": analysis.get("summary", ""),
        })

        if len(signals) >= limit:
            break

    return jsonify({
        "success": True,
        "signals": signals,
        "count": len(signals),
        "generated_at": datetime.utcnow().isoformat(),
    })

@app.route("/api/analyze", methods=["POST"])
def analyze_event_endpoint():
    """Analyze a news headline with AI-powered dynamic value chain generation."""
    from financial_data import get_full_financials, get_ticker_chart_data

    data = request.json or {}
    headline = data.get("headline", "").strip()
    if not headline:
        return jsonify({"error": "headline is required"}), 400

    event_type = data.get("event_type", "news")
    analysis = None

    # ── Primary path: Dynamic AI-generated chains ──
    if analyst.available:
        try:
            ai_result = analyst.generate_dynamic_chains(headline)
            if ai_result and ai_result.get("top_picks"):
                analysis = _enrich_dynamic_result(ai_result, headline)
                analysis["ai_powered"] = True
                analysis["dynamic_chains"] = True
        except Exception as e:
            logger.error(f"Dynamic chain generation failed, falling back: {e}")

    # ── Fallback path: Pre-built chain matching ──
    if analysis is None:
        analysis = engine.analyze_event(headline, event_type)

        if analyst.available and analysis.get("chains_matched", 0) > 0:
            try:
                ticker_data = []
                for t in analysis.get("top_picks", [])[:5]:
                    ticker_data.append({
                        "ticker": t["ticker"], "company": t["company"],
                        "current_price": t.get("current_price", 0),
                        "change_pct": t.get("change_pct", 0),
                        "chain_name": t.get("chain_name", ""),
                        "layer": t.get("layer", ""),
                        "exposure": t.get("exposure", ""),
                        "thesis": t.get("thesis", ""),
                        "financials": t.get("financials", {}),
                    })
                ai_text = analyst.analyze_event(headline, analysis.get("chains", []), ticker_data)
                if ai_text.get("summary"):
                    analysis["summary"] = ai_text["summary"]
                if ai_text.get("risk_factors"):
                    analysis["risk_factors"] = ai_text["risk_factors"]
                if ai_text.get("contrarian_view"):
                    analysis["contrarian_view"] = ai_text["contrarian_view"]
                if ai_text.get("time_horizon"):
                    analysis["ai_time_horizon"] = ai_text["time_horizon"]
                ticker_analyses = ai_text.get("ticker_analyses", {})
                for pick in analysis.get("top_picks", []):
                    ai_t = ticker_analyses.get(pick["ticker"])
                    if ai_t:
                        pick["investment_analysis"] = ai_t
                analysis["ai_powered"] = True
            except Exception:
                analysis["ai_powered"] = False
        else:
            analysis["ai_powered"] = False

        # Add chart data to fallback picks
        _add_chart_data_to_picks(analysis.get("top_picks", []))
        analysis["dynamic_chains"] = False

    # Save top picks to recommendation history for scorecard tracking
    _save_recommendations(analysis)

    return jsonify({"success": True, **analysis})


def _save_recommendations(analysis: dict):
    """Save top picks to DB for performance tracking."""
    try:
        headline = analysis.get("headline", "")
        conn = sqlite3.connect(DB_PATH)
        for pick in analysis.get("top_picks", [])[:5]:
            if not pick.get("ticker") or not pick.get("current_price"):
                continue
            # Compute target price
            proj_sign = 1 if pick.get("direction") == "bullish" else -1
            conv = pick.get("conviction", 0.5)
            import math
            target = pick["current_price"] * math.exp(proj_sign * conv * 0.003 * 22)
            conn.execute("""
                INSERT INTO recommendation_history
                (ticker, company, action, direction, conviction, entry_price, target_price,
                 headline, order_type, chain_name, risk_reward)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pick["ticker"], pick.get("company", ""),
                pick.get("action", "BUY"), pick.get("direction", "bullish"),
                conv, pick["current_price"], round(target, 2),
                headline[:200], pick.get("order", 1),
                pick.get("chain_name", ""), pick.get("risk_reward", ""),
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Failed to save recommendations: {e}")


@app.route("/api/scorecard", methods=["GET"])
def get_scorecard():
    """Get performance scorecard of past recommendations with live P&L."""
    from financial_data import _get_basic_quote

    days = request.args.get("days", 14, type=int)
    limit = request.args.get("limit", 10, type=int)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

    # Get unique recent recommendations (dedupe by ticker, keep latest)
    rows = conn.execute("""
        SELECT * FROM recommendation_history
        WHERE created_at > ? ORDER BY created_at DESC LIMIT ?
    """, (cutoff, limit * 3)).fetchall()
    conn.close()

    seen = set()
    recs = []
    for row in rows:
        r = dict(row)
        if r["ticker"] in seen:
            continue
        seen.add(r["ticker"])

        # Fetch current price
        quote = _get_basic_quote(r["ticker"])
        if quote and quote.get("price"):
            current = quote["price"]
            entry = r["entry_price"]
            if r["direction"] == "bearish":
                pnl_pct = round((entry - current) / entry * 100, 1)
            else:
                pnl_pct = round((current - entry) / entry * 100, 1)

            recs.append({
                "ticker": r["ticker"],
                "company": r["company"],
                "action": r["action"],
                "direction": r["direction"],
                "conviction": r["conviction"],
                "entry_price": entry,
                "current_price": current,
                "target_price": r["target_price"],
                "pnl_pct": pnl_pct,
                "correct": pnl_pct > 0,
                "headline": r["headline"],
                "order_type": r["order_type"],
                "chain_name": r["chain_name"],
                "created_at": r["created_at"],
            })
            time.sleep(0.2)

        if len(recs) >= limit:
            break

    wins = sum(1 for r in recs if r["correct"])
    total = len(recs)

    return jsonify({
        "success": True,
        "recommendations": recs,
        "stats": {
            "total": total,
            "wins": wins,
            "win_rate": round(wins / total * 100) if total > 0 else 0,
            "avg_pnl": round(sum(r["pnl_pct"] for r in recs) / total, 1) if total > 0 else 0,
        }
    })


def _enrich_dynamic_result(ai_result: dict, headline: str) -> dict:
    """Enrich Claude's dynamic chain result with live financial data and charts."""
    from financial_data import get_full_financials, get_ticker_chart_data

    seen_tickers = set()
    valid_picks = []

    all_picks = ai_result.get("top_picks", []) + [
        r for r in ai_result.get("recommendations", [])
        if r.get("ticker") not in {p.get("ticker") for p in ai_result.get("top_picks", [])}
    ]

    for pick in all_picks:
        ticker = pick.get("ticker", "").upper()
        if not ticker or ticker in seen_tickers or len(ticker) > 5:
            continue

        fin = get_full_financials(ticker)
        if fin is None:
            logger.warning(f"Ticker {ticker} not found on Yahoo Finance, skipping")
            continue

        seen_tickers.add(ticker)

        pick["current_price"] = fin.get("price", 0)
        pick["change_pct"] = fin.get("change_pct", 0)
        pick["financials"] = {
            "market_cap": fin.get("market_cap_str"),
            "pe_forward": fin.get("pe_forward"),
            "pe_trailing": fin.get("pe_trailing"),
            "revenue_growth": fin.get("revenue_growth"),
            "profit_margin": fin.get("profit_margin"),
            "fifty_two_high": fin.get("fifty_two_high"),
            "fifty_two_low": fin.get("fifty_two_low"),
            "distance_from_high_pct": fin.get("distance_from_high_pct"),
            "beta": fin.get("beta"),
        }

        pick["investment_analysis"] = pick.get("thesis", "")

        # Chart data for top 5 only
        if len(valid_picks) < 5:
            chart = get_ticker_chart_data(
                ticker,
                direction=pick.get("direction", "bullish"),
                conviction=pick.get("conviction", 0.5)
            )
            if chart:
                pick["chart_data"] = chart

        valid_picks.append(pick)

        if len(seen_tickers) >= 8:
            time.sleep(0.3)

    all_valid = sorted(
        valid_picks,
        key=lambda t: t.get("conviction", 0) * t.get("impact_score", 0),
        reverse=True
    )

    return {
        "headline": headline,
        "event_type": "news",
        "chains_matched": len(ai_result.get("chains", [])),
        "chains": ai_result.get("chains", []),
        "top_picks": all_valid[:5],
        "recommendations": all_valid,
        "summary": ai_result.get("summary", ""),
        "risk_factors": ai_result.get("risk_factors", []),
        "obvious_play": ai_result.get("obvious_play"),
        "historical_precedents": ai_result.get("historical_precedents", []),
        "active_catalysts": ai_result.get("active_catalysts", []),
        "contrarian_view": ai_result.get("contrarian_view", ""),
        "ai_time_horizon": ai_result.get("time_horizon", ""),
        "analyzed_at": datetime.utcnow().isoformat(),
    }


def _add_chart_data_to_picks(picks):
    """Add chart data to ticker picks (fallback path)."""
    from financial_data import get_ticker_chart_data

    for pick in (picks or [])[:5]:
        ticker = pick.get("ticker")
        if not ticker:
            continue
        chart = get_ticker_chart_data(
            ticker,
            direction=pick.get("direction", "bullish"),
            conviction=pick.get("conviction", 0.5)
        )
        if chart:
            pick["chart_data"] = chart

@app.route("/api/events", methods=["GET"])
def get_events():
    """Get recent catalyst events with their chain impacts."""
    hours = request.args.get("hours", 72, type=int)
    limit = request.args.get("limit", 30, type=int)
    events = db.get_recent_events(hours=hours, limit=limit)
    # Parse JSON fields
    for e in events:
        try:
            e["matched_chains"] = json.loads(e.get("matched_chains", "[]"))
            e["metadata"] = json.loads(e.get("metadata", "{}"))
        except (json.JSONDecodeError, TypeError):
            e["matched_chains"] = []
            e["metadata"] = {}
    return jsonify({"success": True, "events": events, "count": len(events)})

@app.route("/api/portfolio", methods=["GET"])
def get_portfolio():
    """Get cross-chain portfolio view of all recommended tickers."""
    portfolio = engine.get_portfolio_view()
    return jsonify({"success": True, **portfolio})

@app.route("/api/ticker/<ticker>", methods=["GET"])
def get_ticker_info(ticker):
    """Get info about a specific ticker including which chains it belongs to."""
    ticker = ticker.upper()
    chains_containing = []
    for cid, chain in ALL_CHAINS.items():
        for node in chain.nodes:
            for t in node.tickers:
                if t["ticker"] == ticker:
                    chains_containing.append({
                        "chain_id": cid,
                        "chain_name": chain.name,
                        "layer": node.layer,
                        "position": node.position,
                        "sensitivity": node.sensitivity,
                        "exposure": t.get("exposure", "medium"),
                        "notes": t.get("notes", ""),
                    })

    if not chains_containing:
        return jsonify({"success": False, "error": "Ticker not found in any chain"}), 404

    # Get price history
    history = db.get_ticker_history(ticker, days=30)

    return jsonify({
        "success": True,
        "ticker": ticker,
        "company": chains_containing[0].get("notes", "").split(".")[0] if chains_containing else "",
        "chains": chains_containing,
        "chain_count": len(chains_containing),
        "price_history": history,
    })

@app.route("/api/universe/search", methods=["GET"])
def search_universe():
    """Search all publicly listed stocks."""
    query = request.args.get("q", "")
    chain_id = request.args.get("chain_id")
    limit = request.args.get("limit", 20, type=int)
    results = universe.search_tickers(query, chain_id, limit)
    return jsonify({"success": True, "results": results, "count": len(results)})

@app.route("/api/universe/stats", methods=["GET"])
def universe_stats():
    """Get stock universe coverage stats."""
    stats = universe.get_universe_stats()
    return jsonify({"success": True, **stats})

@app.route("/api/universe/chain/<chain_id>", methods=["GET"])
def universe_chain_companies(chain_id):
    """Get all companies in the universe mapped to a chain."""
    layer = request.args.get("layer")
    limit = request.args.get("limit", 50, type=int)
    companies = universe.get_chain_companies(chain_id, layer, limit)
    return jsonify({"success": True, "companies": companies, "count": len(companies)})

@app.route("/api/universe/load", methods=["POST"])
def load_universe():
    """Load/refresh the stock universe from SEC EDGAR."""
    count = universe.load_from_sec()
    return jsonify({"success": True, "companies_loaded": count})

@app.route("/api/collect", methods=["POST"])
def trigger_collection():
    """Manually trigger event collection."""
    global last_collection_at
    try:
        counts = orchestrator.run_collection()
        last_collection_at = datetime.utcnow().isoformat()
        return jsonify({"success": True, "collected": counts})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/export", methods=["GET"])
def export_csv_portfolio():
    """Export portfolio recommendations as CSV."""
    portfolio = engine.get_portfolio_view()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ticker", "company", "chain", "layer", "direction",
                     "conviction", "impact_score", "time_horizon", "price", "change_pct", "thesis"])
    for t in portfolio.get("tickers", []):
        writer.writerow([
            t["ticker"], t["company"], t["chain_name"], t["layer"],
            t["direction"], t["conviction"], t["impact_score"],
            t["time_horizon"], t["current_price"], t["change_pct"], t["thesis"]
        ])
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={"Content-Disposition": "attachment;filename=thetaflow_portfolio.csv"})

# ── Auth Endpoints ──

@app.route("/api/auth/register", methods=["POST"])
def register():
    data = request.json or {}
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")
    if not email or not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
        return jsonify({"error": "Valid email required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO users (email, password_hash, tier) VALUES (?, ?, ?)",
                     (email, hash_password(password), "free"))
        conn.commit()
        user_id = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()[0]
        conn.close()
        session['user_id'] = user_id
        return jsonify({"success": True, "user": {"id": user_id, "email": email, "tier": "free"}})
    except sqlite3.IntegrityError:
        return jsonify({"error": "Email already registered"}), 409

@app.route("/api/auth/login", methods=["POST"])
def login():
    data = request.json or {}
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    if not row or not verify_password(password, row["password_hash"]):
        conn.close()
        return jsonify({"error": "Invalid email or password"}), 401
    conn.execute("UPDATE users SET last_login_at = ? WHERE id = ?",
                 (datetime.utcnow().isoformat(), row["id"]))
    conn.commit()
    conn.close()
    user = dict(row)
    session['user_id'] = user["id"]
    return jsonify({"success": True, "user": {
        "id": user["id"], "email": user["email"], "tier": user["tier"],
        "api_key": user["api_key"],
    }})

@app.route("/api/auth/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"success": True})

@app.route("/api/auth/me", methods=["GET"])
def get_me():
    if 'user_id' not in session:
        return jsonify({"authenticated": False}), 401
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM users WHERE id = ?", (session['user_id'],)).fetchone()
    conn.close()
    if not row:
        session.clear()
        return jsonify({"authenticated": False}), 401
    user = dict(row)
    return jsonify({"authenticated": True, "user": {
        "id": user["id"], "email": user["email"], "tier": user["tier"],
        "api_key": user["api_key"],
    }})

# ── Startup & Scheduler ──

def initialize_data():
    global last_collection_at
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    conn.close()
    if count == 0:
        logger.info("Empty database — running initial event collection...")
        orchestrator.run_collection()
        last_collection_at = datetime.utcnow().isoformat()
        logger.info("Initial data loaded.")

def start_scheduler():
    global last_collection_at
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        sched = BackgroundScheduler()

        def scheduled_collection():
            global last_collection_at
            try:
                logger.info("Scheduled event collection starting...")
                orchestrator.run_collection()
                last_collection_at = datetime.utcnow().isoformat()
                logger.info(f"Scheduled collection complete at {last_collection_at}")
            except Exception as e:
                logger.error(f"Scheduled collection failed: {e}")

        sched.add_job(scheduled_collection, 'interval', hours=4,
                      id='event_collection', replace_existing=True)
        sched.start()
        logger.info("Scheduler started: event collection every 4 hours")
    except ImportError:
        logger.warning("APScheduler not installed — scheduled jobs disabled")

if __name__ == "__main__":
    initialize_data()
    if os.environ.get("WERKZEUG_RUN_MAIN") or not app.debug:
        start_scheduler()
    port = int(os.getenv("PORT", 5002))
    app.run(host="0.0.0.0", port=port, debug=True)
