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
import uuid
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("thetaflow.api")

app = Flask(__name__, static_folder="../frontend", static_url_path="")
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))
CORS(app, supports_credentials=True)

DB_PATH = os.getenv("THETAFLOW_DB", os.path.join(os.path.expanduser("~"), "thetaflow.db"))

db = EventDatabase(DB_PATH)
orchestrator = EventOrchestrator(db)
engine = ImpactEngine(DB_PATH)
universe = StockUniverse(DB_PATH)

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

@app.route("/")
def serve_landing():
    return send_from_directory(app.static_folder, "landing.html")

@app.route("/dashboard")
def serve_dashboard():
    return send_from_directory(app.static_folder, "dashboard.html")

# ── Core API ──

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

@app.route("/api/signals", methods=["GET"])
def get_signals():
    """Get today's top events pre-analyzed with chain impacts and ticker recommendations.
    This is the primary dashboard endpoint — shows what happened and what it means."""
    hours = request.args.get("hours", 72, type=int)
    limit = request.args.get("limit", 10, type=int)

    events = db.get_recent_events(hours=hours, limit=50)
    # Prioritize news headlines over SEC filings for better readability
    events.sort(key=lambda e: (0 if e.get("source") == "newsapi" else 1, e.get("timestamp", "")), reverse=False)
    events.sort(key=lambda e: e.get("source") == "newsapi", reverse=True)
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
def analyze_event():
    """Analyze a news headline or event and get investment recommendations."""
    data = request.json or {}
    headline = data.get("headline", "").strip()
    if not headline:
        return jsonify({"error": "headline is required"}), 400

    event_type = data.get("event_type", "news")
    analysis = engine.analyze_event(headline, event_type)
    return jsonify({"success": True, **analysis})

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
