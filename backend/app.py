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
    """Find and read an HTML file - tries every possible path.
    Priority: THETAFLOW_FRONTEND env > backend dir > frontend sibling > cwd."""
    _this_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.getenv("THETAFLOW_FRONTEND", ""),
        _this_dir,  # backend/ dir (has copies)
        os.path.join(_this_dir, "..", "frontend"),  # ../frontend/
        os.getcwd(),
        os.path.join(os.getcwd(), "frontend"),
        os.path.join(os.getcwd(), "backend"),
        "/app/frontend",  # Railway common path
        "/app/backend",
    ]
    for d in candidates:
        if not d:
            continue
        path = os.path.join(d, filename)
        if os.path.isfile(path):
            logger.debug(f"Found {filename} at {path}")
            with open(path, "r") as f:
                return f.read()
    logger.error(f"Could not find {filename} in any candidate path: {candidates}")
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
    """Debug route to find frontend directory on Railway."""
    import glob
    _this_dir = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    info = {
        "cwd": cwd,
        "app_file": os.path.abspath(__file__),
        "this_dir": _this_dir,
        "env_frontend": os.getenv("THETAFLOW_FRONTEND", "not set"),
        "cwd_contents": sorted(os.listdir(cwd)) if os.path.isdir(cwd) else [],
        "this_dir_contents": sorted(os.listdir(_this_dir)) if os.path.isdir(_this_dir) else [],
        "dashboard_found": _find_html("dashboard.html") is not None,
        "landing_found": _find_html("landing.html") is not None,
    }
    # Search common paths
    for search_dir in [cwd, _this_dir, "/app", os.path.join(cwd, "..")]:
        try:
            for f in glob.glob(os.path.join(search_dir, "**", "dashboard.html"), recursive=True):
                info[f"found_{f}"] = True
        except Exception:
            pass
    return jsonify(info)

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
        "Economy": {"url": "https://feeds.a.dj.com/rss/RSSEconomy.xml", "color": "#f59e0b"},
    }

    headlines = []
    for category, info in feeds.items():
        try:
            # Cache-busting: timestamp param + no-cache headers to bypass CDN/proxy caches
            cache_bust_url = f"{info['url']}?_t={int(time.time())}"
            resp = requests.get(cache_bust_url, headers={
                "User-Agent": "ThetaFlow/1.0",
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "If-None-Match": "",  # Force fresh response
            }, timeout=6)
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

    response = jsonify({"success": True, "headlines": headlines})
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

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

@app.route("/api/analyze-stream", methods=["POST"])
def analyze_stream():
    """Stream analysis results via SSE for progressive rendering."""
    from financial_data import get_full_financials_batch, get_ticker_chart_data
    from concurrent.futures import ThreadPoolExecutor

    data = request.json or {}
    headline = data.get("headline", "").strip()
    if not headline:
        return jsonify({"error": "headline is required"}), 400

    def sse(event_type, payload):
        return f"event: {event_type}\ndata: {json.dumps(payload)}\n\n"

    def generate():
        yield sse("status", {"message": "Identifying investment themes...", "phase": "ai", "pct": 5})

        if not analyst.available:
            yield sse("error", {"message": "AI analyst not available"})
            return

        try:
            # Stream Claude API — send progress as tokens arrive
            prompt = analyst._build_dynamic_prompt(headline)
            full_text = ""

            with analyst.client.messages.stream(
                model="claude-sonnet-4-20250514",
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}]
            ) as stream:
                token_count = 0
                last_pct = 5
                for chunk in stream.text_stream:
                    full_text += chunk
                    token_count += 1
                    # Send progress every ~40 tokens (~10 updates total)
                    pct = min(5 + int(token_count / 5), 85)
                    if pct >= last_pct + 8:
                        last_pct = pct
                        messages = [
                            "Analyzing supply chain connections...",
                            "Finding historical precedents...",
                            "Computing earnings impact...",
                            "Identifying cross-catalyst compounding...",
                            "Scoring conviction levels...",
                            "Ranking non-obvious plays...",
                            "Building trade recommendations...",
                            "Quantifying risk/reward...",
                            "Finalizing analysis...",
                        ]
                        msg_idx = min(pct // 10, len(messages) - 1)
                        yield sse("progress", {"pct": pct, "message": messages[msg_idx]})

            # Parse the complete response
            ai_result = analyst._parse_dynamic_result(full_text)
            if not ai_result or not ai_result.get("top_picks"):
                yield sse("error", {"message": "Could not generate analysis. Try a more specific headline."})
                return

            # Send raw AI result immediately (before financial enrichment)
            yield sse("ai_result", {
                "chains": ai_result.get("chains", []),
                "summary": ai_result.get("summary", ""),
                "obvious_play": ai_result.get("obvious_play"),
                "historical_precedents": ai_result.get("historical_precedents", []),
                "active_catalysts": ai_result.get("active_catalysts", []),
                "top_picks_preview": [
                    {"ticker": p.get("ticker"), "company": p.get("company"),
                     "direction": p.get("direction"), "conviction": p.get("conviction"),
                     "action": p.get("action"), "order": p.get("order"),
                     "thesis": p.get("thesis"), "earnings_impact": p.get("earnings_impact"),
                     "precedent_move": p.get("precedent_move"), "layer": p.get("layer"),
                     "chain_name": p.get("chain_name"), "exposure": p.get("exposure"),
                     "time_horizon": p.get("time_horizon"), "impact_score": p.get("impact_score"),
                     "risk_reward": p.get("risk_reward")}
                    for p in ai_result.get("top_picks", [])
                ],
                "risk_factors": ai_result.get("risk_factors", []),
                "contrarian_view": ai_result.get("contrarian_view", ""),
                "time_horizon": ai_result.get("time_horizon", ""),
            })

            yield sse("status", {"message": "Fetching live market data...", "phase": "enrich", "pct": 88})

            # Enrich with financial data
            enriched = _enrich_dynamic_result(ai_result, headline)
            enriched["ai_powered"] = True
            enriched["dynamic_chains"] = True

            _save_recommendations(enriched)

            yield sse("complete", {"success": True, **enriched})

        except Exception as e:
            logger.error(f"Stream analysis failed: {e}")
            yield sse("error", {"message": f"Analysis failed: {str(e)[:100]}"})

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )


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


@app.route("/api/scored-picks", methods=["GET"])
def get_scored_picks():
    """Get AI-scored stock picks across all chains with ThetaFlow Score."""
    from financial_data import get_full_financials

    # 1. Gather all unique tickers from chains + recommendation history
    all_tickers = {}
    for chain in ALL_CHAINS.values():
        for node in chain.nodes:
            for t in node.tickers:
                tk = t["ticker"]
                if tk not in all_tickers:
                    all_tickers[tk] = {
                        "ticker": tk, "company": t["company"],
                        "chain_name": chain.name, "layer": node.layer,
                        "exposure": t.get("exposure", "medium"),
                        "sensitivity": node.sensitivity,
                    }

    # 2. Get recommendation frequency from history
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT ticker, COUNT(*) as freq, AVG(conviction) as avg_conv,
               MAX(created_at) as last_seen
        FROM recommendation_history GROUP BY ticker
    """).fetchall()
    conn.close()
    rec_freq = {r["ticker"]: dict(r) for r in rows}

    # 3. Enrich top candidates with live prices
    candidates = sorted(all_tickers.values(),
                       key=lambda t: rec_freq.get(t["ticker"], {}).get("avg_conv", t["sensitivity"]),
                       reverse=True)[:30]

    enriched = []
    for c in candidates:
        fin = get_full_financials(c["ticker"])
        if not fin or not fin.get("price"):
            continue
        c["current_price"] = fin["price"]
        c["change_pct"] = fin.get("change_pct", 0)
        c["market_cap"] = fin.get("market_cap_str", "")
        c["pe_forward"] = fin.get("pe_forward")
        c["distance_from_high"] = fin.get("distance_from_high_pct")
        c["beta"] = fin.get("beta")
        rf = rec_freq.get(c["ticker"], {})
        c["rec_frequency"] = rf.get("freq", 0)
        c["avg_conviction"] = rf.get("avg_conv", c["sensitivity"])
        enriched.append(c)
        time.sleep(0.2)
        if len(enriched) >= 20:
            break

    # 4. Send to Claude for AI scoring
    if analyst.available and enriched:
        try:
            ticker_list = "\n".join([
                f"- {t['ticker']} ({t['company']}): ${t['current_price']:.0f}, "
                f"chain={t['chain_name']}, P/E={t.get('pe_forward') or 'N/A'}, "
                f"from_high={t.get('distance_from_high') or 'N/A'}%, "
                f"rec_freq={t['rec_frequency']}, avg_conv={t['avg_conviction']:.0%}"
                for t in enriched
            ])

            response = analyst.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=3000,
                messages=[{"role": "user", "content": f"""Score these stocks on a 1-100 ThetaFlow Score based on: catalyst exposure, valuation attractiveness, momentum, and risk/reward asymmetry. Consider current market conditions.

STOCKS:
{ticker_list}

Return JSON array. For each stock:
{{"ticker":"SYM","score":85,"grade":"A","action":"BUY","rationale":"One sentence why."}}

Score meaning: 90-100=Strong Buy, 75-89=Buy, 60-74=Watch, 40-59=Neutral, below 40=Avoid.
Grade: A/B/C/D/F.
Sort by score descending. Return ONLY the JSON array, no other text."""}]
            )

            text = response.content[0].text.strip()
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()

            scores = json.loads(text)
            score_map = {s["ticker"]: s for s in scores if isinstance(s, dict)}

            for t in enriched:
                sc = score_map.get(t["ticker"], {})
                t["score"] = sc.get("score", 50)
                t["grade"] = sc.get("grade", "C")
                t["action"] = sc.get("action", "WATCH")
                t["rationale"] = sc.get("rationale", "")

        except Exception as e:
            logger.error(f"AI scoring failed: {e}")
            for t in enriched:
                t["score"] = round(t["avg_conviction"] * 100)
                t["grade"] = "A" if t["score"] >= 75 else "B" if t["score"] >= 60 else "C"
                t["action"] = "BUY" if t["score"] >= 75 else "WATCH"
                t["rationale"] = ""
    else:
        for t in enriched:
            t["score"] = round(t["avg_conviction"] * 100)
            t["grade"] = "A" if t["score"] >= 75 else "B" if t["score"] >= 60 else "C"
            t["action"] = "BUY" if t["score"] >= 75 else "WATCH"
            t["rationale"] = ""

    enriched.sort(key=lambda t: t.get("score", 0), reverse=True)

    return jsonify({
        "success": True,
        "picks": enriched,
        "total": len(enriched),
        "ai_scored": analyst.available,
    })


def _enrich_dynamic_result(ai_result: dict, headline: str) -> dict:
    """Enrich Claude's dynamic chain result with live financial data and charts.
    Uses parallel batch fetching for speed."""
    from financial_data import get_full_financials_batch, get_ticker_chart_data
    from concurrent.futures import ThreadPoolExecutor

    all_picks = ai_result.get("top_picks", []) + [
        r for r in ai_result.get("recommendations", [])
        if r.get("ticker") not in {p.get("ticker") for p in ai_result.get("top_picks", [])}
    ]

    # Collect unique valid tickers
    unique_tickers = []
    seen = set()
    for pick in all_picks:
        ticker = pick.get("ticker", "").upper().strip()
        if ticker and ticker not in seen and len(ticker) <= 5:
            seen.add(ticker)
            unique_tickers.append(ticker)

    # Parallel batch fetch all financials at once
    batch_data = get_full_financials_batch(unique_tickers[:12])

    # Pre-fetch chart data in parallel for top 5 tickers
    chart_tickers = unique_tickers[:5]
    chart_data_map = {}
    if chart_tickers:
        def _fetch_chart(tk):
            # Find the pick to get direction/conviction
            for p in all_picks:
                if p.get("ticker", "").upper().strip() == tk:
                    return (tk, get_ticker_chart_data(tk, p.get("direction", "bullish"), p.get("conviction", 0.5)))
            return (tk, get_ticker_chart_data(tk))

        with ThreadPoolExecutor(max_workers=5) as executor:
            for tk, chart in executor.map(lambda t: _fetch_chart(t), chart_tickers):
                if chart:
                    chart_data_map[tk] = chart

    valid_picks = []
    seen_tickers = set()

    for pick in all_picks:
        ticker = pick.get("ticker", "").upper().strip()
        if not ticker or ticker in seen_tickers or len(ticker) > 5:
            continue

        fin = batch_data.get(ticker)
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

        # Attach pre-fetched chart data
        if ticker in chart_data_map:
            pick["chart_data"] = chart_data_map[ticker]

        valid_picks.append(pick)

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

@app.route("/api/export-xlsx", methods=["POST"])
def export_xlsx():
    """Export analysis as Excel workbook with valuation model buildup."""
    import math
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
    except ImportError:
        return jsonify({"error": "openpyxl not installed"}), 500

    data = request.json or {}
    if not data.get("top_picks"):
        return jsonify({"error": "No analysis data provided"}), 400

    wb = Workbook()

    # ── Style constants ──
    hdr_font = Font(name='Calibri', bold=True, size=10, color='FFFFFF')
    hdr_fill = PatternFill(start_color='1B2A4A', end_color='1B2A4A', fill_type='solid')
    sec_font = Font(name='Calibri', bold=True, size=11, color='1B2A4A')
    mono_font = Font(name='Consolas', size=10)
    pct_fmt = '0.0%'
    usd_fmt = '$#,##0.00'
    usd_whole = '$#,##0'
    thin_border = Border(
        bottom=Side(style='thin', color='D0D5DD'),
    )

    def style_header_row(ws, row, col_count):
        for c in range(1, col_count + 1):
            cell = ws.cell(row=row, column=c)
            cell.font = hdr_font
            cell.fill = hdr_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')

    def auto_width(ws):
        for col in ws.columns:
            max_len = 0
            col_letter = col[0].column_letter
            for cell in col:
                if cell.value:
                    max_len = max(max_len, len(str(cell.value)))
            ws.column_dimensions[col_letter].width = min(max_len + 3, 45)

    # ════════════════════════════════════════
    # SHEET 1: Summary
    # ════════════════════════════════════════
    ws = wb.active
    ws.title = "Summary"
    ws.sheet_properties.tabColor = "22D3EE"

    ws.cell(row=1, column=1, value="THETAFLOW ANALYSIS").font = Font(name='Calibri', bold=True, size=14, color='1B2A4A')
    ws.cell(row=2, column=1, value=f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    ws.cell(row=2, column=1).font = Font(name='Calibri', size=9, color='6B7D96')

    ws.cell(row=4, column=1, value="Headline").font = sec_font
    ws.cell(row=4, column=2, value=data.get("headline", ""))
    ws.cell(row=5, column=1, value="Direction").font = sec_font
    top_dir = data.get("top_picks", [{}])[0].get("direction", "bullish")
    ws.cell(row=5, column=2, value=top_dir.upper())
    ws.cell(row=5, column=2).font = Font(name='Calibri', bold=True, size=10,
                                          color='10B981' if top_dir == 'bullish' else 'EF4444')
    ws.cell(row=6, column=1, value="Conviction").font = sec_font
    top_conv = data.get("top_picks", [{}])[0].get("conviction", 0)
    ws.cell(row=6, column=2, value=top_conv)
    ws.cell(row=6, column=2).number_format = pct_fmt
    ws.cell(row=7, column=1, value="Time Horizon").font = sec_font
    ws.cell(row=7, column=2, value=data.get("ai_time_horizon", data.get("time_horizon", "")))

    # Themes
    chains = data.get("chains", [])
    if chains:
        ws.cell(row=9, column=1, value="Investment Themes").font = sec_font
        for i, c in enumerate(chains):
            ws.cell(row=10 + i, column=1, value=c.get("chain_name", ""))
            ws.cell(row=10 + i, column=2, value=c.get("relevance_score", 0))
            ws.cell(row=10 + i, column=2).number_format = pct_fmt

    # Thesis
    r = 10 + len(chains) + 1
    ws.cell(row=r, column=1, value="Thesis").font = sec_font
    ws.cell(row=r + 1, column=1, value=data.get("summary", ""))
    ws.merge_cells(start_row=r + 1, start_column=1, end_row=r + 1, end_column=4)

    # Obvious play
    op = data.get("obvious_play")
    if op:
        r += 3
        ws.cell(row=r, column=1, value="Obvious Play (Priced In)").font = sec_font
        ws.cell(row=r + 1, column=1, value=op.get("ticker", ""))
        ws.cell(row=r + 1, column=2, value=op.get("summary", ""))

    auto_width(ws)

    # ════════════════════════════════════════
    # SHEET 2: Trades
    # ════════════════════════════════════════
    ws2 = wb.create_sheet("Trades")
    ws2.sheet_properties.tabColor = "34D399"
    headers = ["Ticker", "Company", "Action", "Direction", "Conviction",
               "Order", "Chain", "Layer", "Exposure", "Impact Score",
               "Risk/Reward", "Time Horizon",
               "Earnings Impact", "Historical Precedent", "Thesis"]
    for c, h in enumerate(headers, 1):
        ws2.cell(row=1, column=c, value=h)
    style_header_row(ws2, 1, len(headers))

    all_picks = data.get("recommendations", data.get("top_picks", []))
    for i, t in enumerate(all_picks, 2):
        ws2.cell(row=i, column=1, value=t.get("ticker", "")).font = Font(name='Consolas', bold=True, size=10)
        ws2.cell(row=i, column=2, value=t.get("company", ""))
        ws2.cell(row=i, column=3, value=t.get("action", ""))
        ws2.cell(row=i, column=4, value=t.get("direction", ""))
        ws2.cell(row=i, column=5, value=t.get("conviction", 0))
        ws2.cell(row=i, column=5).number_format = pct_fmt
        ws2.cell(row=i, column=6, value=t.get("order", 1))
        ws2.cell(row=i, column=7, value=t.get("chain_name", ""))
        ws2.cell(row=i, column=8, value=t.get("layer", ""))
        ws2.cell(row=i, column=9, value=t.get("exposure", ""))
        ws2.cell(row=i, column=10, value=t.get("impact_score", 0))
        ws2.cell(row=i, column=11, value=t.get("risk_reward", ""))
        ws2.cell(row=i, column=12, value=(t.get("time_horizon", "") or "").replace("_", " "))
        ws2.cell(row=i, column=13, value=t.get("earnings_impact", ""))
        ws2.cell(row=i, column=14, value=t.get("precedent_move", ""))
        ws2.cell(row=i, column=15, value=t.get("thesis", t.get("investment_analysis", "")))
        for c in range(1, len(headers) + 1):
            ws2.cell(row=i, column=c).border = thin_border

    auto_width(ws2)

    # ════════════════════════════════════════
    # SHEET 3: Valuation & Pro Forma
    # ════════════════════════════════════════
    ws3 = wb.create_sheet("Valuation")
    ws3.sheet_properties.tabColor = "FBBF24"
    val_headers = ["Ticker", "Company", "Current Price", "Market Cap",
                   "52w High", "52w Low", "Dist from High",
                   "Direction", "Conviction", "Impact Score",
                   "30d Projected Move", "Target Price", "Upside/Downside",
                   "Conviction-Wtd Return", "Earnings Impact", "Risk/Reward"]
    for c, h in enumerate(val_headers, 1):
        ws3.cell(row=1, column=c, value=h)
    style_header_row(ws3, 1, len(val_headers))

    top_picks = data.get("top_picks", [])
    for i, t in enumerate(top_picks, 2):
        fin = t.get("financials", {})
        price = t.get("current_price", 0) or 0
        conv = t.get("conviction", 0.5)
        direction = t.get("direction", "bullish")
        proj_sign = 1 if direction == "bullish" else -1
        proj_30d = (math.exp(proj_sign * conv * 0.003 * 22) - 1)
        target = price * (1 + proj_30d) if price else 0

        ws3.cell(row=i, column=1, value=t.get("ticker", "")).font = Font(name='Consolas', bold=True, size=10)
        ws3.cell(row=i, column=2, value=t.get("company", ""))

        ws3.cell(row=i, column=3, value=price)
        ws3.cell(row=i, column=3).number_format = usd_fmt

        # Market cap — try to parse from string like "$1.2B"
        mc_str = fin.get("market_cap", "") or ""
        mc_val = None
        if mc_str:
            try:
                mc_clean = mc_str.replace("$", "").replace(",", "").strip()
                if mc_clean.endswith("T"):
                    mc_val = float(mc_clean[:-1]) * 1e12
                elif mc_clean.endswith("B"):
                    mc_val = float(mc_clean[:-1]) * 1e9
                elif mc_clean.endswith("M"):
                    mc_val = float(mc_clean[:-1]) * 1e6
                else:
                    mc_val = float(mc_clean)
            except (ValueError, IndexError):
                pass
        ws3.cell(row=i, column=4, value=mc_val if mc_val else mc_str)
        if mc_val:
            ws3.cell(row=i, column=4).number_format = usd_whole

        ws3.cell(row=i, column=5, value=fin.get("fifty_two_high"))
        ws3.cell(row=i, column=5).number_format = usd_fmt
        ws3.cell(row=i, column=6, value=fin.get("fifty_two_low"))
        ws3.cell(row=i, column=6).number_format = usd_fmt

        dist = fin.get("distance_from_high_pct")
        ws3.cell(row=i, column=7, value=dist / 100 if dist else None)
        ws3.cell(row=i, column=7).number_format = pct_fmt

        ws3.cell(row=i, column=8, value=direction)
        ws3.cell(row=i, column=9, value=conv)
        ws3.cell(row=i, column=9).number_format = pct_fmt
        ws3.cell(row=i, column=10, value=t.get("impact_score", 0))

        ws3.cell(row=i, column=11, value=proj_30d)
        ws3.cell(row=i, column=11).number_format = pct_fmt
        ws3.cell(row=i, column=11).font = Font(name='Consolas', size=10,
            color='10B981' if proj_30d >= 0 else 'EF4444')

        ws3.cell(row=i, column=12, value=target)
        ws3.cell(row=i, column=12).number_format = usd_fmt

        upside = proj_30d  # same as projected move
        ws3.cell(row=i, column=13, value=upside)
        ws3.cell(row=i, column=13).number_format = pct_fmt
        ws3.cell(row=i, column=13).font = Font(name='Consolas', size=10,
            color='10B981' if upside >= 0 else 'EF4444')

        # Conviction-weighted return
        cw_ret = conv * upside
        ws3.cell(row=i, column=14, value=cw_ret)
        ws3.cell(row=i, column=14).number_format = pct_fmt

        ws3.cell(row=i, column=15, value=t.get("earnings_impact", ""))
        ws3.cell(row=i, column=16, value=t.get("risk_reward", ""))

        for c in range(1, len(val_headers) + 1):
            ws3.cell(row=i, column=c).border = thin_border

    # Portfolio-level summary row
    sum_row = len(top_picks) + 3
    ws3.cell(row=sum_row, column=1, value="PORTFOLIO SUMMARY").font = sec_font
    if top_picks:
        avg_conv = sum(t.get("conviction", 0) for t in top_picks) / len(top_picks)
        avg_upside = sum(
            (math.exp((1 if t.get("direction") == "bullish" else -1) * t.get("conviction", 0.5) * 0.003 * 22) - 1)
            for t in top_picks
        ) / len(top_picks)
        ws3.cell(row=sum_row + 1, column=1, value="Avg Conviction")
        ws3.cell(row=sum_row + 1, column=2, value=avg_conv)
        ws3.cell(row=sum_row + 1, column=2).number_format = pct_fmt
        ws3.cell(row=sum_row + 2, column=1, value="Avg Projected Move")
        ws3.cell(row=sum_row + 2, column=2, value=avg_upside)
        ws3.cell(row=sum_row + 2, column=2).number_format = pct_fmt
        ws3.cell(row=sum_row + 3, column=1, value="Number of Picks")
        ws3.cell(row=sum_row + 3, column=2, value=len(top_picks))

    auto_width(ws3)

    # ════════════════════════════════════════
    # SHEET 4: Portfolio Sizing
    # ════════════════════════════════════════
    portfolio_size = data.get("portfolio_size")
    if portfolio_size and top_picks:
        ws4 = wb.create_sheet("Portfolio Sizing")
        ws4.sheet_properties.tabColor = "818CF8"

        ws4.cell(row=1, column=1, value="PORTFOLIO SIZING").font = Font(name='Calibri', bold=True, size=12, color='1B2A4A')
        ws4.cell(row=2, column=1, value="Portfolio Value")
        ws4.cell(row=2, column=2, value=portfolio_size)
        ws4.cell(row=2, column=2).number_format = usd_whole

        ps_headers = ["Ticker", "Company", "Action", "Conviction", "Weight",
                      "Dollar Allocation", "Shares", "Price", "Projected Return", "Dollar P&L"]
        for c, h in enumerate(ps_headers, 1):
            ws4.cell(row=4, column=c, value=h)
        style_header_row(ws4, 4, len(ps_headers))

        valid_picks = [t for t in top_picks if t.get("current_price", 0) > 0]
        total_conv = sum(t.get("conviction", 0.5) for t in valid_picks) or 1

        for i, t in enumerate(valid_picks, 5):
            conv = t.get("conviction", 0.5)
            weight = conv / total_conv
            dollars = portfolio_size * weight
            price = t.get("current_price", 0)
            shares = int(dollars / price) if price else 0
            proj_sign = 1 if t.get("direction") == "bullish" else -1
            proj_ret = math.exp(proj_sign * conv * 0.003 * 22) - 1
            dollar_pnl = dollars * proj_ret

            ws4.cell(row=i, column=1, value=t.get("ticker", "")).font = Font(name='Consolas', bold=True, size=10)
            ws4.cell(row=i, column=2, value=t.get("company", ""))
            ws4.cell(row=i, column=3, value=t.get("action", ""))
            ws4.cell(row=i, column=4, value=conv)
            ws4.cell(row=i, column=4).number_format = pct_fmt
            ws4.cell(row=i, column=5, value=weight)
            ws4.cell(row=i, column=5).number_format = pct_fmt
            ws4.cell(row=i, column=6, value=dollars)
            ws4.cell(row=i, column=6).number_format = usd_whole
            ws4.cell(row=i, column=7, value=shares)
            ws4.cell(row=i, column=8, value=price)
            ws4.cell(row=i, column=8).number_format = usd_fmt
            ws4.cell(row=i, column=9, value=proj_ret)
            ws4.cell(row=i, column=9).number_format = pct_fmt
            ws4.cell(row=i, column=10, value=dollar_pnl)
            ws4.cell(row=i, column=10).number_format = usd_whole
            ws4.cell(row=i, column=10).font = Font(name='Consolas', size=10,
                color='10B981' if dollar_pnl >= 0 else 'EF4444')
            for c in range(1, len(ps_headers) + 1):
                ws4.cell(row=i, column=c).border = thin_border

        # Totals row
        tr = 5 + len(valid_picks)
        ws4.cell(row=tr, column=1, value="TOTAL").font = sec_font
        ws4.cell(row=tr, column=6, value=portfolio_size)
        ws4.cell(row=tr, column=6).number_format = usd_whole
        ws4.cell(row=tr, column=6).font = sec_font
        total_pnl = sum(
            (portfolio_size * (t.get("conviction", 0.5) / total_conv)) *
            (math.exp((1 if t.get("direction") == "bullish" else -1) * t.get("conviction", 0.5) * 0.003 * 22) - 1)
            for t in valid_picks
        )
        ws4.cell(row=tr, column=10, value=total_pnl)
        ws4.cell(row=tr, column=10).number_format = usd_whole
        ws4.cell(row=tr, column=10).font = Font(name='Consolas', bold=True, size=10,
            color='10B981' if total_pnl >= 0 else 'EF4444')

        auto_width(ws4)

    # ════════════════════════════════════════
    # SHEET 5: Risk Assessment
    # ════════════════════════════════════════
    ws5 = wb.create_sheet("Risk")
    ws5.sheet_properties.tabColor = "FBBF24"

    ws5.cell(row=1, column=1, value="RISK ASSESSMENT").font = Font(name='Calibri', bold=True, size=12, color='1B2A4A')

    r = 3
    risk_factors = data.get("risk_factors", [])
    if risk_factors:
        ws5.cell(row=r, column=1, value="Risk Factors").font = sec_font
        r += 1
        for rf in risk_factors:
            ws5.cell(row=r, column=1, value=rf)
            r += 1
        r += 1

    contrarian = data.get("contrarian_view", "")
    if contrarian:
        ws5.cell(row=r, column=1, value="Contrarian View").font = sec_font
        r += 1
        ws5.cell(row=r, column=1, value=contrarian)
        ws5.merge_cells(start_row=r, start_column=1, end_row=r, end_column=4)
        r += 2

    catalysts = data.get("active_catalysts", [])
    if catalysts:
        ws5.cell(row=r, column=1, value="Active Cross-Catalysts").font = sec_font
        r += 1
        for cat in catalysts:
            ws5.cell(row=r, column=1, value=cat)
            r += 1
        r += 1

    precedents = data.get("historical_precedents", [])
    if precedents:
        ws5.cell(row=r, column=1, value="Historical Precedents").font = sec_font
        r += 1
        for p in precedents:
            ws5.cell(row=r, column=1, value=f"{p.get('event', '')} ({p.get('date', '')})")
            r += 1
            for o in p.get("outcomes", []):
                ws5.cell(row=r, column=1, value=f"  {o.get('ticker', '')}: {o.get('move', '')} over {o.get('period', '')}")
                r += 1

    auto_width(ws5)

    # ── Write to bytes and return ──
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    headline_slug = re.sub(r'[^a-zA-Z0-9]', '_', data.get("headline", "analysis")[:30]).strip('_')
    filename = f"thetaflow_{headline_slug}_{datetime.utcnow().strftime('%Y%m%d')}.xlsx"

    return Response(
        output.getvalue(),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={"Content-Disposition": f"attachment;filename={filename}"}
    )


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
