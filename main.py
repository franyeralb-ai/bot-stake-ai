import asyncio
import csv
import io
import json
import os
import re
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional, Tuple
from curl_cffi import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

# ==================== CONFIGURACIÓN BASE ====================
PORT = int(os.environ.get("PORT", 6111))
INITIAL_BALANCE = 30000.0  # 30,000 USDT
DEFAULT_STAKE = 500.0      # 500 USDT por entrada (configurable en la UI)
DEFAULT_MIN_ODDS = 1.01
DEFAULT_MAX_ODDS = 1.05    # Rango ultra-seguro por defecto para entrar (configurable en UI)
AI_CAPTURE_MAX_ODDS = 1.45 # El motor IA guarda TODAS las cuotas <= 1.45 (entradas y NO entradas)
SCAN_INTERVAL_SEC = 6      # Escaneo rápido cada 6 segundos

STAKE_GQL_URL = os.environ.get("STAKE_GQL_URL", "https://stake.com/_api/graphql")
PROXY_URL = os.environ.get("PROXY_URL", "")
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "")
CLOUD_BRIDGE_URL = os.environ.get("CLOUD_BRIDGE_URL", "https://bot-stake-ai.onrender.com").strip()
KAMBI_LIVE_OPEN_URL = "https://eu-offering-api.kambicdn.com/offering/v2018/ub/event/live/open.json?lang=en_GB&market=GB"
KAMBI_EVENT_BO_URL = "https://eu-offering-api.kambicdn.com/offering/v2018/ub/betoffer/event/{event_id}.json?lang=en_GB&market=GB"
KAMBI_EVENT_LIVEDATA_URL = "https://eu-offering-api.kambicdn.com/offering/v2018/ub/event/{event_id}/livedata.json?lang=en_GB&market=GB"
DB_NAME = os.environ.get("DB_NAME", "stake_paper_30k.db")
DB_LOCK = threading.RLock()

BLACKLIST = [
    "srl", "simulated", "esoccer", "ebasketball", "cyber",
    "virtual", "2x4", "2x5", "2x6", "3x3", "battle"
]

SPORTS_CONFIG = [
    {"slug": "soccer", "label": "⚽ Fútbol", "limit": 35},
    {"slug": "tennis", "label": "🎾 Tenis", "limit": 25},
    {"slug": "basketball", "label": "🏀 Baloncesto", "limit": 20},
    {"slug": "ice-hockey", "label": "🏒 Hockey", "limit": 20},
    {"slug": "american-football", "label": "🏈 Fútbol Amer.", "limit": 15},
    {"slug": "volleyball", "label": "🏐 Voleibol", "limit": 15},
    {"slug": "table-tennis", "label": "🏓 Ping Pong", "limit": 15},
    {"slug": "baseball", "label": "⚾ Béisbol", "limit": 15},
]

KAMBI_SPORT_MAP = {
    "FOOTBALL": "soccer",
    "TENNIS": "tennis",
    "BASKETBALL": "basketball",
    "ICE_HOCKEY": "ice-hockey",
    "AMERICAN_FOOTBALL": "american-football",
    "VOLLEYBALL": "volleyball",
    "TABLE_TENNIS": "table-tennis",
    "BASEBALL": "baseball",
}

runtime_state = {
    "is_running": True,
    "stake_amount": DEFAULT_STAKE,
    "min_odds": DEFAULT_MIN_ODDS,
    "max_odds": DEFAULT_MAX_ODDS,
    "last_scan_time": "Iniciando...",
    "connection_status": "Conectando a Motor Multi-Deporte en Vivo...",
    "live_radar": [],
    "sport_counts": {},
    "active_live_slugs": set(),
    "last_settle_check": {},
    "stoppage_start": {},
    "score_memory": {},
    "stake_blocked_until": 0,
    "bridge_data": None,
    "recent_settled_map": {}
}

# ==================== BASE DE DATOS (WAL + TELEMETRÍA IA) ====================
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME, timeout=30.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn

def init_db():
    with DB_LOCK:
        conn = get_db()
        c = conn.cursor()
        # 1. Tabla de Operaciones Ejecutadas por el Bot (Banca 30,000 USDT)
        c.execute("""
            CREATE TABLE IF NOT EXISTS bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fixture_id TEXT UNIQUE,
                slug TEXT,
                sport TEXT DEFAULT '⚽ Fútbol',
                match_name TEXT,
                league TEXT,
                market_name TEXT DEFAULT 'Ganador',
                market_type TEXT DEFAULT 'WINNER_HOME',
                line REAL DEFAULT 0.0,
                selection TEXT,
                side TEXT,
                odds REAL,
                stake REAL,
                minute TEXT,
                score_at_bet TEXT,
                final_score TEXT,
                live_clock TEXT DEFAULT '',
                remaining_text TEXT DEFAULT '',
                rem_seconds INTEGER DEFAULT -1,
                elapsed_seconds INTEGER DEFAULT -1,
                clock_updated_at INTEGER DEFAULT 0,
                safety_reason TEXT DEFAULT '',
                ai_confidence REAL DEFAULT 98.0,
                last_h INTEGER DEFAULT 0,
                last_a INTEGER DEFAULT 0,
                status TEXT DEFAULT 'PENDING',
                pnl REAL DEFAULT 0.0,
                timestamp INTEGER
            )
        """)
        existing_cols = {row[1] for row in c.execute("PRAGMA table_info(bets)").fetchall()}
        migrations = [
            ("sport", "TEXT DEFAULT '⚽ Fútbol'"),
            ("market_name", "TEXT DEFAULT 'Ganador'"),
            ("market_type", "TEXT DEFAULT 'WINNER_HOME'"),
            ("line", "REAL DEFAULT 0.0"),
            ("live_clock", "TEXT DEFAULT ''"),
            ("remaining_text", "TEXT DEFAULT ''"),
            ("rem_seconds", "INTEGER DEFAULT -1"),
            ("elapsed_seconds", "INTEGER DEFAULT -1"),
            ("clock_updated_at", "INTEGER DEFAULT 0"),
            ("safety_reason", "TEXT DEFAULT ''"),
            ("ai_confidence", "REAL DEFAULT 98.0"),
            ("last_h", "INTEGER DEFAULT 0"),
            ("last_a", "INTEGER DEFAULT 0"),
        ]
        for col, col_def in migrations:
            if col not in existing_cols:
                c.execute(f"ALTER TABLE bets ADD COLUMN {col} {col_def}")

        # 2. Tabla Maestra de Entrenamiento IA (Guarda TODAS las apuestas: Entradas y NO Entradas)
        c.execute("""
            CREATE TABLE IF NOT EXISTS ai_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                obs_key TEXT UNIQUE,
                fixture_id TEXT,
                slug TEXT,
                sport_slug TEXT,
                sport TEXT,
                category TEXT,
                tournament TEXT,
                league TEXT,
                home_team TEXT,
                away_team TEXT,
                match_name TEXT,
                match_status TEXT,
                stage_bucket TEXT,
                clock_display TEXT,
                remaining_text TEXT DEFAULT '',
                rem_seconds INTEGER DEFAULT -1,
                elapsed_minute INTEGER,
                remaining_minute INTEGER,
                home_score INTEGER,
                away_score INTEGER,
                score_diff INTEGER,
                total_score INTEGER,
                current_period_h INTEGER,
                current_period_a INTEGER,
                home_game_score TEXT,
                away_game_score TEXT,
                period_scores_json TEXT,
                market_name TEXT,
                market_type TEXT,
                line REAL,
                cushion REAL,
                selection TEXT,
                side TEXT,
                odds REAL,
                implied_prob REAL,
                market_overround REAL,
                ai_confidence REAL,
                was_entered INTEGER DEFAULT 0,
                reject_code TEXT,
                reject_reason TEXT,
                all_markets_json TEXT,
                last_h INTEGER DEFAULT 0,
                last_a INTEGER DEFAULT 0,
                final_h INTEGER,
                final_a INTEGER,
                final_score TEXT,
                status TEXT DEFAULT 'PENDING',
                target_win INTEGER,
                hypothetical_roi REAL,
                timestamp INTEGER,
                settled_at INTEGER
            )
        """)
        existing_ai_cols = {row[1] for row in c.execute("PRAGMA table_info(ai_observations)").fetchall()}
        for col, col_def in [("remaining_text", "TEXT DEFAULT ''"), ("rem_seconds", "INTEGER DEFAULT -1")]:
            if col not in existing_ai_cols:
                c.execute(f"ALTER TABLE ai_observations ADD COLUMN {col} {col_def}")

        c.execute("CREATE INDEX IF NOT EXISTS idx_ai_obs_status ON ai_observations(status);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_ai_obs_slug ON ai_observations(slug);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_ai_obs_fixture ON ai_observations(fixture_id);")

        c.execute("""
            CREATE TABLE IF NOT EXISTS account (
                key TEXT PRIMARY KEY,
                val REAL
            )
        """)
        c.execute("INSERT OR IGNORE INTO account (key, val) VALUES ('equity', ?)", (INITIAL_BALANCE,))
        # Reparar artefactos históricos de sets empatados (1-1 en Tenis / 2-2 en Ping Pong) al cerrarse el último punto en Kambi
        c.execute("""
            UPDATE ai_observations
            SET status = 'WON',
                target_win = 1,
                hypothetical_roi = ROUND(odds - 1.0, 4),
                final_h = CASE WHEN side = 'HOME' THEN final_h + 1 ELSE final_h END,
                final_a = CASE WHEN side = 'AWAY' THEN final_a + 1 ELSE final_a END,
                final_score = CASE
                    WHEN side = 'HOME' THEN CAST(final_h + 1 AS TEXT) || '-' || CAST(final_a AS TEXT)
                    ELSE CAST(final_h AS TEXT) || '-' || CAST(final_a + 1 AS TEXT)
                END
            WHERE sport_slug != 'soccer'
              AND status = 'LOST'
              AND final_h = final_a
              AND (
                  (side = 'HOME' AND current_period_h > current_period_a)
                  OR (side = 'AWAY' AND current_period_a > current_period_h)
                  OR odds <= 1.05
              )
        """)
        # Reparar liquidación prematura de Zhang, Boxiong vs Linang Xiao (donde Stake envió 0-1 en el segundo de transición antes de actualizar a 2-1 [2-6, 7-6, 6-2])
        c.execute("""
            SELECT id, stake, odds, pnl FROM bets
            WHERE match_name LIKE '%Zhang, Boxiong%' AND status = 'LOST' AND final_score = '0-1'
        """)
        for z_id, z_stake, z_odds, z_old_pnl in c.fetchall():
            new_pnl = round(float(z_stake) * (float(z_odds) - 1.0), 2)
            diff_pnl = round(new_pnl - float(z_old_pnl), 2)
            c.execute("UPDATE account SET val = val + ? WHERE key = 'equity'", (diff_pnl,))
            c.execute("""
                UPDATE bets
                SET status = 'WON', pnl = ?, final_score = '2-1', last_h = 2, last_a = 1
                WHERE id = ?
            """, (new_pnl, z_id))
        c.execute("""
            UPDATE ai_observations
            SET status = 'WON', target_win = 1, hypothetical_roi = ROUND(odds - 1.0, 4),
                final_h = 2, final_a = 1, final_score = '2-1'
            WHERE match_name LIKE '%Zhang, Boxiong%' AND side = 'HOME' AND status = 'LOST' AND final_score = '0-1'
        """)
        conn.commit()
        conn.close()

def _compute_stats_from_cursor(c: sqlite3.Cursor) -> Dict[str, Any]:
    c.execute("SELECT val FROM account WHERE key = 'equity'")
    row = c.fetchone()
    equity = row[0] if row else INITIAL_BALANCE

    c.execute("""
        SELECT 
            COUNT(*),
            SUM(CASE WHEN status='WON' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='LOST' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='PENDING' THEN stake ELSE 0 END),
            SUM(pnl)
        FROM bets
    """)
    total, won, lost, pending, locked_stake, net_pnl = c.fetchone()

    total = total or 0
    won = won or 0
    lost = lost or 0
    pending = pending or 0
    locked_stake = locked_stake or 0.0
    net_pnl = net_pnl or 0.0
    settled = won + lost
    win_rate = (won / settled * 100.0) if settled > 0 else 0.0
    available = max(0.0, equity - locked_stake)

    c.execute("""
        SELECT
            COUNT(*),
            SUM(CASE WHEN was_entered=1 THEN 1 ELSE 0 END),
            SUM(CASE WHEN was_entered=0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN status IN ('WON', 'LOST', 'PUSH') THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END),
            SUM(CASE WHEN was_entered=0 AND status='LOST' THEN 1 ELSE 0 END),
            SUM(CASE WHEN was_entered=0 AND status='WON' THEN 1 ELSE 0 END),
            SUM(CASE WHEN was_entered=1 AND status='WON' THEN 1 ELSE 0 END),
            SUM(CASE WHEN was_entered=1 AND status='LOST' THEN 1 ELSE 0 END)
        FROM ai_observations
    """)
    ai_row = c.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0, 0)
    ai_total = ai_row[0] or 0
    ai_entered = ai_row[1] or 0
    ai_shadow = ai_row[2] or 0
    ai_settled = ai_row[3] or 0
    ai_pending = ai_row[4] or 0
    ai_saved_losses = ai_row[5] or 0
    ai_shadow_wins = ai_row[6] or 0
    ai_ent_wins = ai_row[7] or 0
    ai_ent_losses = ai_row[8] or 0

    shadow_settled = ai_saved_losses + ai_shadow_wins
    shadow_win_rate = (ai_shadow_wins / shadow_settled * 100.0) if shadow_settled > 0 else 0.0
    ent_settled = ai_ent_wins + ai_ent_losses
    ent_win_rate = (ai_ent_wins / ent_settled * 100.0) if ent_settled > 0 else win_rate

    return {
        "equity": round(equity, 2),
        "available": round(available, 2),
        "locked": round(locked_stake, 2),
        "net_pnl": round(net_pnl, 2),
        "total_bets": total,
        "won": won,
        "lost": lost,
        "pending": pending,
        "win_rate": round(win_rate, 2),
        "is_running": runtime_state["is_running"],
        "stake_amount": runtime_state["stake_amount"],
        "min_odds": runtime_state["min_odds"],
        "max_odds": runtime_state["max_odds"],
        "last_scan_time": runtime_state["last_scan_time"],
        "connection_status": runtime_state["connection_status"],
        "sport_counts": runtime_state["sport_counts"],
        "ai_stats": {
            "total_captured": ai_total,
            "entered_count": ai_entered,
            "shadow_count": ai_shadow,
            "settled_count": ai_settled,
            "pending_count": ai_pending,
            "saved_losses": ai_saved_losses,
            "shadow_wins": ai_shadow_wins,
            "shadow_win_rate": round(shadow_win_rate, 2),
            "entered_win_rate": round(ent_win_rate, 2)
        }
    }

def _compute_ai_model_insights(c: sqlite3.Cursor) -> List[Dict[str, Any]]:
    c.execute("""
        SELECT
            sport,
            market_name,
            COUNT(*) as total_obs,
            SUM(CASE WHEN status IN ('WON','LOST','PUSH') THEN 1 ELSE 0 END) as settled,
            SUM(CASE WHEN status='WON' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN status='LOST' THEN 1 ELSE 0 END) as losses,
            AVG(odds) as avg_odds,
            AVG(CASE WHEN status IN ('WON','LOST','PUSH') THEN hypothetical_roi ELSE NULL END) as avg_roi
        FROM ai_observations
        GROUP BY sport, market_name
        ORDER BY settled DESC, total_obs DESC
        LIMIT 15
    """)
    insights = []
    for sp, m_name, tot, settled, wins, losses, avg_odds, avg_roi in c.fetchall():
        settled = settled or 0
        wins = wins or 0
        losses = losses or 0
        wr = (wins / (wins + losses) * 100.0) if (wins + losses) > 0 else None
        insights.append({
            "sport": sp,
            "market_name": m_name,
            "total_obs": tot,
            "settled": settled,
            "wins": wins,
            "losses": losses,
            "win_rate": round(wr, 2) if wr is not None else None,
            "avg_odds": round(avg_odds or 1.0, 3),
            "avg_roi_pct": round((avg_roi or 0.0) * 100.0, 2) if avg_roi is not None else None
        })
    return insights

# ==================== CONSULTAS GRAPHQL ====================
LIVE_SPORT_QUERY = """
query LiveSportFixtures($sportSlug: String!, $limit: Int!) {
  slugSport(sport: $sportSlug) {
    fixtureList(type: live, limit: $limit, offset: 0) {
      id
      slug
      status
      tournament {
        name
        category { name }
      }
      data {
        ... on SportFixtureDataMatch {
          competitors { name }
        }
      }
      eventStatus {
        ... on SportFixtureEventStatusData {
          homeScore
          awayScore
          matchStatus
          homeGameScore
          awayGameScore
          clock {
            matchTime
            remainingTime
          }
          periodScores {
            homeScore
            awayScore
            matchStatus
          }
        }
      }
      groups(groups: ["main", "1x2", "winner", "totals", "double_chance"]) {
        name
        templates(limit: 12, includeEmpty: false) {
          name
          markets(limit: 6) {
            id
            name
            status
            outcomes {
              id
              name
              odds
              active
            }
          }
        }
      }
    }
  }
}
"""

SLUG_FIXTURE_QUERY = """
query SlugFixtureResult($slug: String!) {
  slugFixture(fixture: $slug) {
    id
    slug
    status
    eventStatus {
      ... on SportFixtureEventStatusData {
        homeScore
        awayScore
        matchStatus
        homeGameScore
        awayGameScore
        clock { matchTime remainingTime }
        periodScores {
          homeScore
          awayScore
          matchStatus
        }
      }
    }
  }
}
"""

def parse_mmss_to_seconds(clock_str: Optional[str]) -> Optional[int]:
    """Convierte '56:10' o '10:50' o '90:00+3:00' a segundos totales."""
    if not clock_str:
        return None
    s = str(clock_str).strip()
    if "+" in s:
        parts = s.split("+")
        s1 = parse_mmss_to_seconds(parts[0]) or 0
        s2 = parse_mmss_to_seconds(parts[1]) or 0
        return s1 + s2
    m = re.match(r"^(\d+):(\d{1,2})$", s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m2 = re.search(r"(\d+)", s)
    if m2:
        return int(m2.group(1)) * 60
    return None

def parse_minute(clock_str: Optional[str]) -> int:
    sec = parse_mmss_to_seconds(clock_str)
    return (sec // 60) if sec is not None else 0

def format_mmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    return f"{seconds // 60:02d}:{seconds % 60:02d}"

def compute_live_timing(sport_slug: str, ev: dict, fid: str = "") -> Dict[str, Any]:
    """
    Calcula tanto el reloj actual como el TIEMPO RESTANTE EN VIVO (y cuando Stake congela
    el reloj en 90:00 durante el descuento/tiempo añadido, calcula y cronometra en vivo
    los minutos y segundos de descuento transcurridos: '🔥 Descuento 90' +MM:SS').
    """
    now_ts = int(time.time())
    stoppage_map = runtime_state["stoppage_start"]
    h_score = int(ev.get("homeScore") or 0)
    a_score = int(ev.get("awayScore") or 0)
    match_status = str(ev.get("matchStatus") or "").strip()
    status_lower = match_status.lower()
    clock_obj = ev.get("clock") or {}
    match_time = clock_obj.get("matchTime")
    rem_time = clock_obj.get("remainingTime")
    periods = ev.get("periodScores") or []
    is_paused_break = any(k in status_lower for k in ["halftime", "half time", "break", "intermission", "pause", "suspended"])
    is_overtime = any(k in status_lower for k in ["awaiting extra", "overtime", "extra time", "penalties", "shootout", "ot", "ap"])

    if sport_slug in ["tennis", "volleyball", "table-tennis"]:
        has_active_set = (len(periods) > (h_score + a_score)) and (not is_paused_break)
    else:
        has_active_set = bool(periods)
    cur_p = periods[-1] if (periods and has_active_set) else {}
    cur_h = int(cur_p.get("homeScore") or 0)
    cur_a = int(cur_p.get("awayScore") or 0)

    rem_sec = parse_mmss_to_seconds(rem_time)
    elap_sec = parse_mmss_to_seconds(match_time)
    stop_key = fid or f"{sport_slug}_{h_score}_{a_score}_{match_status}"

    if sport_slug == "soccer":
        score_display = f"{h_score} - {a_score}"
        if is_paused_break:
            clock_display = "Descanso (HT)"
            remaining_text = "⏸️ Descanso (Restan 45:00)"
            rem_sec = -1
            elap_sec = 45 * 60
        elif is_overtime:
            clock_display = f"Prórroga ({match_status})"
            remaining_text = f"⚡ En Prórroga / Penaltis ({match_status})"
            rem_sec = -1
        elif elap_sec is not None:
            if elap_sec < 90 * 60:
                stoppage_map.pop(stop_key, None)
                clock_display = f"{format_mmss(elap_sec)}'"
                rem_sec = 90 * 60 - elap_sec
                remaining_text = f"⏱️ Restan {format_mmss(rem_sec)}"
            else:
                # Stake congela matchTime en 90:00 durante el descuento; cronometramos el tiempo añadido real
                if stop_key not in stoppage_map:
                    stoppage_map[stop_key] = now_ts
                extra = max(0, elap_sec - 90 * 60) + (now_ts - stoppage_map[stop_key])
                clock_display = f"90:00' (+{format_mmss(extra)})"
                remaining_text = f"🔥 Descuento 90' +{format_mmss(extra)}"
                # Codificamos como negativo <= -10000 para que el navegador sume segundo a segundo el descuento
                rem_sec = -(10000 + extra)
        else:
            clock_display = match_status or "En vivo"
            remaining_text = f"⏱️ {clock_display}"
            rem_sec = -1

    elif sport_slug in ["ice-hockey", "basketball", "american-football"]:
        score_display = f"{h_score} - {a_score}"
        if is_overtime:
            if rem_sec is not None and rem_sec > 0:
                clock_display = f"Prórroga OT ({format_mmss(rem_sec)})"
                remaining_text = f"⚡ Prórroga OT: Restan {format_mmss(rem_sec)}"
            else:
                clock_display = f"Prórroga ({match_status})"
                remaining_text = f"⚡ En Prórroga / Penaltis ({match_status})"
                rem_sec = -1
        elif rem_sec is not None and rem_sec > 0:
            stoppage_map.pop(stop_key, None)
            clock_display = f"{match_status} ({format_mmss(rem_sec)})"
            remaining_text = f"⏱️ Restan {format_mmss(rem_sec)} ({match_status})"
        elif rem_sec == 0:
            if stop_key not in stoppage_map:
                stoppage_map[stop_key] = now_ts
            extra = now_ts - stoppage_map[stop_key]
            clock_display = f"{match_status} (Cierre +{format_mmss(extra)})"
            remaining_text = f"🔥 Cerrando período (+{format_mmss(extra)})"
            rem_sec = -(10000 + extra)
        elif is_paused_break:
            clock_display = match_status
            remaining_text = f"⏸️ {match_status}"
            rem_sec = -1
        elif elap_sec is not None:
            total_reg = 60 * 60 if sport_slug in ["ice-hockey", "american-football"] else 48 * 60
            rem_sec = max(1, total_reg - elap_sec)
            clock_display = f"{match_status} ({format_mmss(elap_sec)}')"
            remaining_text = f"⏱️ Restan ~{format_mmss(rem_sec)} ({match_status})"
        else:
            clock_display = match_status or "En vivo"
            remaining_text = f"⏱️ {clock_display}"
            rem_sec = -1

    elif sport_slug == "tennis":
        p_str = " ".join(f"{p.get('homeScore',0)}-{p.get('awayScore',0)}" for p in periods)
        g_str = f" ({ev.get('homeGameScore')}-{ev.get('awayGameScore')})" if ev.get("homeGameScore") is not None else ""
        clock_display = f"{match_status} [{p_str}]{g_str}".strip()
        score_display = f"{h_score}-{a_score} sets"
        max_g = max(cur_h, cur_a)
        min_g = min(cur_h, cur_a)
        target_g = 7 if (max_g >= 6 and max_g - min_g <= 1) else 6
        games_left = max(1, target_g - max_g)
        sets_lead = max(h_score, a_score)
        sets_needed = max(0, 2 - sets_lead - 1)
        if sets_needed == 0:
            remaining_text = f"🎾 Set final: a {games_left} game(s) del cierre [{cur_h}-{cur_a}]{g_str}"
        else:
            remaining_text = f"🎾 {match_status}: Set [{cur_h}-{cur_a}]{g_str}"
        rem_sec = -1

    elif sport_slug in ["volleyball", "table-tennis"]:
        p_str = " ".join(f"{p.get('homeScore',0)}-{p.get('awayScore',0)}" for p in periods)
        clock_display = f"{match_status} [{p_str}]".strip()
        score_display = f"{h_score}-{a_score} sets"
        target_pts = 11 if sport_slug == "table-tennis" else (15 if len(periods) == 5 else 25)
        pts_left = max(1, target_pts - max(cur_h, cur_a))
        remaining_text = f"⏱️ Set actual [{cur_h}-{cur_a}] (A {pts_left} pts del set)"
        rem_sec = -1

    else:  # baseball, etc.
        score_display = f"{h_score} - {a_score}"
        clock_display = match_status or "En vivo"
        remaining_text = f"⚾ {clock_display}"
        rem_sec = -1

    return {
        "clock_display": clock_display,
        "remaining_text": remaining_text,
        "rem_seconds": rem_sec if (rem_sec is not None and not is_paused_break) else -1,
        "elapsed_seconds": elap_sec if (elap_sec is not None and not is_paused_break) else -1,
        "score_display": score_display,
        "minute": (elap_sec // 60) if elap_sec is not None else 0,
        "rem_min": (rem_sec // 60) if (rem_sec is not None and rem_sec >= 0) else None
    }

def extract_active_markets(fixture: dict) -> List[dict]:
    seen = set()
    markets = []
    for g in fixture.get("groups") or []:
        for tmpl in g.get("templates") or []:
            for mkt in tmpl.get("markets") or []:
                if mkt.get("status") != "active":
                    continue
                m_name = (mkt.get("name") or tmpl.get("name") or "").strip()
                outcomes = [
                    {
                        "name": (o.get("name") or "").strip(),
                        "odds": float(o.get("odds") or 0.0)
                    }
                    for o in (mkt.get("outcomes") or [])
                    if o.get("active") and float(o.get("odds") or 0.0) > 1.0
                ]
                if not outcomes:
                    continue
                sig = (m_name, tuple((o["name"], o["odds"]) for o in outcomes))
                if sig not in seen:
                    seen.add(sig)
                    inv_sum = sum(1.0 / o["odds"] for o in outcomes if o["odds"] > 0)
                    overround = round(max(0.0, inv_sum - 1.0), 4)
                    markets.append({
                        "name": m_name,
                        "outcomes": outcomes,
                        "overround": overround
                    })
    return markets

def is_team_match(team_name: str, outcome_name: str, side: str) -> bool:
    t = team_name.lower().strip()
    o = outcome_name.lower().strip()
    if side == "HOME" and o in ["1", "home", "local"]:
        return True
    if side == "AWAY" and o in ["2", "away", "visitante"]:
        return True
    if t and (t == o or (len(t) >= 4 and (t in o or o in t))):
        return True
    return False

def compute_stage_bucket(sport_slug: str, minute: int, rem_min: Optional[int], match_status: str, h_score: int, a_score: int, cur_h: int, cur_a: int) -> str:
    st = match_status.lower()
    if sport_slug == "soccer":
        if "half" in st and ("time" in st or st == "ht"):
            return "HT"
        if minute < 45:
            return "M00_44"
        elif minute < 65:
            return "M45_64"
        elif minute < 75:
            return "M65_74"
        elif minute < 83:
            return "M75_82"
        else:
            return "M83_PLUS"
    elif sport_slug in ["basketball", "american-football"]:
        if "1st" in st or "q1" in st:
            return "Q1"
        elif "2nd" in st or "q2" in st or "half" in st:
            return "Q2"
        elif "3rd" in st or "q3" in st:
            return "Q3"
        elif rem_min is not None and rem_min <= 4:
            return "Q4_CLUTCH"
        return "Q4"
    elif sport_slug == "ice-hockey":
        if "1st" in st:
            return "P1"
        elif "2nd" in st:
            return "P2"
        elif rem_min is not None and rem_min <= 4:
            return "P3_FINAL"
        elif rem_min is not None and rem_min <= 10:
            return "P3_LATE"
        return "P3"
    elif sport_slug in ["tennis", "volleyball", "table-tennis"]:
        return f"S{h_score}_{a_score}_G{cur_h//2}_{cur_a//2}"
    return f"{match_status[:10]}_{h_score}_{a_score}"

def compute_ai_confidence(odds: float, overround: float, lock_ok: bool, diff: int, cushion: float, market_type: str) -> float:
    raw_prob = (1.0 / odds) / (1.0 + max(0.0, overround)) if odds > 1.0 else 0.95
    base_pct = min(99.2, raw_prob * 100.0)

    bonus = 0.0
    if market_type.startswith("DNB_"):
        bonus += 1.4
    elif market_type.startswith("DC_"):
        bonus += 1.6
    elif market_type == "UNDER_LINE" and cushion >= 2.5:
        bonus += 1.2
    if diff >= 3:
        bonus += 1.0

    if not lock_ok:
        base_pct -= 4.5

    return round(max(50.0, min(99.8, base_pct + bonus)), 2)

def evaluate_fixture_safety(
    sport_slug: str,
    sport_label: str,
    home_team: str,
    away_team: str,
    ev: dict,
    markets: List[dict],
    min_odds: float,
    max_odds: float
) -> Dict[str, Any]:
    h_score = int(ev.get("homeScore") or 0)
    a_score = int(ev.get("awayScore") or 0)
    diff = abs(h_score - a_score)
    total_score = h_score + a_score
    leader_side = "HOME" if h_score > a_score else ("AWAY" if a_score > h_score else None)
    leader_team = home_team if leader_side == "HOME" else (away_team if leader_side == "AWAY" else None)

    match_status = str(ev.get("matchStatus") or "").strip()
    status_lower = match_status.lower()
    periods = ev.get("periodScores") or []
    is_paused_break = any(k in status_lower for k in ["halftime", "half time", "break", "intermission", "pause", "suspended"])
    if sport_slug in ["tennis", "volleyball", "table-tennis"]:
        has_active_set = (len(periods) > (h_score + a_score)) and (not is_paused_break)
    else:
        has_active_set = bool(periods)
    cur_p = periods[-1] if (periods and has_active_set) else {}
    cur_h = int(cur_p.get("homeScore") or 0)
    cur_a = int(cur_p.get("awayScore") or 0)

    timing = compute_live_timing(sport_slug, ev)
    clock_display = timing["clock_display"]
    remaining_text = timing["remaining_text"]
    rem_seconds = timing["rem_seconds"]
    elapsed_seconds = timing["elapsed_seconds"]
    score_display = timing["score_display"]
    minute = timing["minute"]
    rem_min = timing["rem_min"]

    stage_bucket = compute_stage_bucket(sport_slug, minute, rem_min, match_status, h_score, a_score, cur_h, cur_a)
    candidates = []

    def add_candidate(market_name: str, market_type: str, line: float, cushion: float,
                      selection: str, side: str, odds: float, overround: float,
                      lock_ok: bool, priority: int, safety_reason: str, wait_note: str, reject_code: str):
        odds_ok = min_odds <= odds <= max_odds
        is_ready = lock_ok and odds_ok
        if is_ready:
            final_code = "ENTERED_CANDIDATE"
            final_reason = safety_reason
        elif not odds_ok:
            final_code = "ODDS_OUT_OF_RANGE"
            final_reason = f"Cuota {odds:.2f} fuera de rango [{min_odds:.2f}-{max_odds:.2f}] | {wait_note}"
        else:
            final_code = reject_code
            final_reason = wait_note

        ai_conf = compute_ai_confidence(odds, overround, lock_ok, diff, cushion, market_type)
        candidates.append((is_ready, priority if lock_ok else priority - 40, {
            "market_name": market_name,
            "market_type": market_type,
            "line": line,
            "cushion": cushion,
            "selection": selection,
            "side": side,
            "odds": odds,
            "overround": overround,
            "ai_confidence": ai_conf,
            "lock_ok": lock_ok,
            "odds_ok": odds_ok,
            "safety_reason": safety_reason,
            "wait_note": final_reason,
            "reject_code": final_code
        }))

    # ==================== 1. FÚTBOL (SOCCER) ====================
    if sport_slug == "soccer":
        for mkt in markets:
            m_low = mkt["name"].lower()
            ovr = mkt["overround"]

            if m_low in ["1x2", "match result", "winner"] and leader_side:
                for out in mkt["outcomes"]:
                    if is_team_match(leader_team, out["name"], leader_side) and "&" not in out["name"]:
                        odds = out["odds"]
                        lock_ok = (diff == 2 and minute >= 83) or (diff >= 3 and minute >= 72)
                        r_code = "WAIT_SCORE_DIFF" if diff < 2 else "WAIT_MINUTE"
                        add_candidate(
                            "Ganador (1X2)", f"WINNER_{leader_side}", 0.0, float(diff),
                            out["name"], leader_side, odds, ovr, lock_ok, 95,
                            f"Fútbol 1X2 | Min {minute}' | Ventaja +{diff} goles",
                            f"1X2 @{odds:.2f} (Min {minute}', +{diff} gol | Req: +2 al 83' o +3 al 72')",
                            r_code
                        )

            elif "draw no bet" in m_low and leader_side:
                for out in mkt["outcomes"]:
                    if is_team_match(leader_team, out["name"], leader_side):
                        odds = out["odds"]
                        lock_ok = (diff >= 2 and minute >= 74) or (diff == 1 and minute >= 82 and odds <= 1.04)
                        add_candidate(
                            "Sin Empate (DNB)", f"DNB_{leader_side}", 0.0, float(diff),
                            f"{out['name']} (DNB)", leader_side, odds, ovr, lock_ok, 98,
                            f"Sin Empate (DNB) | Min {minute}' | +{diff} gol(es)",
                            f"DNB @{odds:.2f} (Min {minute}', +{diff} gol | Req: Min 74+ con +2 o 82+ con +1)",
                            "WAIT_MINUTE"
                        )

            elif "double chance" in m_low and leader_side:
                for out in mkt["outcomes"]:
                    o_low = out["name"].lower()
                    if "draw" in o_low and leader_team.lower() in o_low:
                        odds = out["odds"]
                        lock_ok = (diff >= 1 and minute >= 80) or (diff >= 2 and minute >= 68)
                        dc_type = "DC_1X" if leader_side == "HOME" else "DC_X2"
                        add_candidate(
                            "Doble Oportunidad", dc_type, 0.0, float(diff + 1),
                            out["name"], leader_side, odds, ovr, lock_ok, 99,
                            f"Doble Oportunidad (Cubre empate) | Min {minute}' | +{diff} gol(es)",
                            f"Doble Oportunidad @{odds:.2f} (Min {minute}', +{diff} gol | Req: Min 80+ con +1 o 68+ con +2)",
                            "WAIT_MINUTE"
                        )

            elif m_low in ["total goals", "total", "asian total"]:
                for out in mkt["outcomes"]:
                    m_under = re.match(r"^under\s+(\d+\.5)$", out["name"].strip(), re.IGNORECASE)
                    if m_under:
                        line = float(m_under.group(1))
                        cushion = line - total_score
                        odds = out["odds"]
                        # Calibrado con +11,000 muestras IA: prohibido entrar Under con colchón 1.5 en empates (1-1), partidos abiertos (>=3 goles) o descuento (>=90')
                        lock_ok = (
                            (cushion >= 2.5 and 78 <= minute <= 89 and total_score <= 2)
                            or (cushion >= 1.5 and 86 <= minute <= 89 and diff >= 1 and total_score <= 2 and odds <= 1.03)
                        )
                        goals_needed = int(cushion + 0.5)
                        r_code = "LOW_CUSHION" if cushion < 1.5 else "WAIT_MINUTE"
                        add_candidate(
                            f"Under {line} Goles", "UNDER_LINE", line, cushion,
                            f"Under {line}", "UNDER", odds, ovr, lock_ok, 96,
                            f"Under {line} | Min {minute}' ({h_score}-{a_score}) | Colchón: {goals_needed} goles",
                            f"Under {line} @{odds:.2f} (Min {minute}', colchón {goals_needed} goles | Req: Min 78-89, máx 2 goles)",
                            r_code
                        )

            elif "both teams to score" in m_low and (h_score == 0 or a_score == 0):
                for out in mkt["outcomes"]:
                    if out["name"].lower().strip() == "no":
                        odds = out["odds"]
                        # Regla Anti-Muerte Súbita de 1 Gol: BTTS: No se guarda solo en modo sombra para IA
                        # porque 1 solo gol (ej. 0-1 -> 1-1) hace perder -500 USDT, mientras que DNB devuelve y 1X/X2 gana.
                        lock_ok = False
                        add_candidate(
                            "Ambos Anotan: NO", "BTTS_NO", 0.0, 1.0,
                            "BTTS: No", "NO", odds, ovr, lock_ok, 80,
                            f"Ambos Anotan: NO | Min {minute}' ({h_score}-{a_score})",
                            f"BTTS No @{odds:.2f} (Solo Sombra IA: riesgo de muerte súbita por 1 gol)",
                            "SINGLE_GOAL_RISK"
                        )

    # ==================== 2. BALONCESTO (BASKETBALL) ====================
    elif sport_slug == "basketball":
        is_q4 = any(k in status_lower for k in ["4th", "q4", "overtime", "ot"])
        rem_s = timing["rem_seconds"]
        for mkt in markets:
            m_low = mkt["name"].lower()
            ovr = mkt["overround"]
            if "winner" in m_low and "quarter" not in m_low and "half" not in m_low and leader_side:
                for out in mkt["outcomes"]:
                    if is_team_match(leader_team, out["name"], leader_side):
                        odds = out["odds"]
                        # Exigir tiempo restante real en el 4to cuarto (<=3:00 min con +12 pts, <=5:00 min con +18 pts, o +24 pts en Q4)
                        lock_ok = is_q4 and (
                            (diff >= 12 and 0 <= rem_s <= 180)
                            or (diff >= 18 and 0 <= rem_s <= 300)
                            or (diff >= 24)
                        )
                        add_candidate(
                            "Winner (Incl. OT)", f"WINNER_{leader_side}", 0.0, float(diff),
                            out["name"], leader_side, odds, ovr, lock_ok, 95,
                            f"Basket Winner | {remaining_text} | Ventaja +{diff} pts",
                            f"Basket Winner @{odds:.2f} (+{diff} pts | Req: Q4 <=3:00m +12 pts o <=5:00m +18 pts)",
                            "WAIT_Q4_MARGIN"
                        )

    # ==================== 3. TENIS (TENNIS) ====================
    elif sport_slug == "tennis":
        game_diff = abs(cur_h - cur_a)
        cur_leader_games = cur_h if (cur_h > cur_a) else cur_a
        # Prohibido entrar en el Set 1 (0-0 sets); exigir que el jugador ya tenga 1 set ganado y esté cerrando el set definitivo (5-2 / 5-3)
        same_leader = (
            (h_score > a_score and cur_h > cur_a)
            or (a_score > h_score and cur_a > cur_h)
            or (h_score >= 1 and h_score == a_score and cur_h != cur_a)
        )
        effective_side = "HOME" if (h_score > a_score or (h_score >= 1 and h_score == a_score and cur_h > cur_a)) else (
            "AWAY" if (a_score > h_score or (a_score >= 1 and h_score == a_score and cur_a > cur_h)) else None
        )
        effective_team = home_team if effective_side == "HOME" else (away_team if effective_side == "AWAY" else None)

        for mkt in markets:
            m_low = mkt["name"].lower()
            ovr = mkt["overround"]
            if m_low == "winner" and effective_side:
                for out in mkt["outcomes"]:
                    if is_team_match(effective_team, out["name"], effective_side):
                        odds = out["odds"]
                        # Calibrado con dataset nocturno: bloquear 4-2 (un solo quiebre con 2 games por jugar); exigir 5-2/5-3/5-1/5-0 o doble quiebre 4-1/4-0
                        lock_ok = has_active_set and same_leader and (
                            (diff >= 1 and ((cur_leader_games >= 5 and game_diff >= 2) or (cur_leader_games >= 4 and game_diff >= 3)))
                            or (h_score >= 1 and h_score == a_score and cur_leader_games >= 5 and game_diff >= 2)
                        )
                        add_candidate(
                            "Match Winner", f"WINNER_{effective_side}", 0.0, float(game_diff),
                            out["name"], effective_side, odds, ovr, lock_ok, 94,
                            f"Tenis Match Winner | Sets {h_score}-{a_score} (Games {cur_h}-{cur_a})",
                            f"Tenis Winner @{odds:.2f} (Sets {h_score}-{a_score}, Games {cur_h}-{cur_a} | Req: Set 2/3 cierre 5-2/5-3 o 4-1)",
                            "WAIT_SET_CLOSING"
                        )

    # ==================== 4. HOCKEY SOBRE HIELO (ICE HOCKEY) ====================
    elif sport_slug == "ice-hockey":
        is_p3 = any(k in status_lower for k in ["3rd", "p3", "overtime", "ot"])
        rem_s = timing["rem_seconds"]
        for mkt in markets:
            m_low = mkt["name"].lower()
            ovr = mkt["overround"]
            if ("winner" in m_low or m_low == "1x2") and "&" not in m_low and "period" not in m_low and leader_side:
                is_ot_included = "overtime" in m_low or "ot" in m_low
                for out in mkt["outcomes"]:
                    if is_team_match(leader_team, out["name"], leader_side) and "&" not in out["name"]:
                        odds = out["odds"]
                        # PROHIBIDO entrar con +1 gol en Hockey (por el 6v5 sin portero al final).
                        # Solo +3 goles (últimos 12 min) o +2 goles con <= 3:30 restantes y Prórroga Incluida.
                        lock_ok = is_p3 and (
                            (diff >= 3 and (minute >= 48 or (rem_min is not None and rem_min <= 12)))
                            or (diff >= 2 and is_ot_included and (minute >= 56 or (0 <= rem_s <= 210)))
                        )
                        add_candidate(
                            mkt["name"], f"WINNER_{leader_side}", 0.0, float(diff),
                            out["name"], leader_side, odds, ovr, lock_ok, 97 if is_ot_included else 92,
                            f"Hockey Winner | {remaining_text} | Ventaja +{diff} goles",
                            f"Hockey Winner @{odds:.2f} (+{diff} gol | Req: +3 goles o +2 goles <=3:30m Incl. OT)",
                            "WAIT_P3_MARGIN"
                        )

    # ==================== 5. FÚTBOL AMERICANO (NFL / NCAA) ====================
    elif sport_slug == "american-football":
        is_q4 = any(k in status_lower for k in ["4th", "q4", "overtime"])
        is_q3 = any(k in status_lower for k in ["3rd", "q3"])
        for mkt in markets:
            m_low = mkt["name"].lower()
            ovr = mkt["overround"]
            if "winner" in m_low and "quarter" not in m_low and "half" not in m_low and leader_side:
                for out in mkt["outcomes"]:
                    if is_team_match(leader_team, out["name"], leader_side):
                        odds = out["odds"]
                        lock_ok = (is_q4 and diff >= 14) or (is_q3 and diff >= 21)
                        add_candidate(
                            "Winner (Incl. OT)", f"WINNER_{leader_side}", 0.0, float(diff),
                            out["name"], leader_side, odds, ovr, lock_ok, 95,
                            f"Fútbol Amer. Winner | {remaining_text} | +{diff} pts",
                            f"NFL/NCAAF Winner @{odds:.2f} (+{diff} pts | Req: Q4 +14 pts)",
                            "WAIT_Q4_MARGIN"
                        )

    # ==================== 6. VOLEIBOL / TENIS DE MESA / BÉISBOL ====================
    else:
        sets_won = h_score if leader_side == "HOME" else a_score
        cur_leader_pts = cur_h if leader_side == "HOME" else cur_a
        cur_trailer_pts = cur_a if leader_side == "HOME" else cur_h
        pts_lead = cur_leader_pts - cur_trailer_pts
        for mkt in markets:
            m_low = mkt["name"].lower()
            ovr = mkt["overround"]
            if ("winner" in m_low or m_low == "1x2") and "set" not in m_low and "inning" not in m_low and leader_side:
                for out in mkt["outcomes"]:
                    if is_team_match(leader_team, out["name"], leader_side):
                        odds = out["odds"]
                        if sport_slug == "volleyball":
                            # Calibrado con CSV IA (100% WR): el líder del partido (2+ sets) también debe ir ganando el set actual EN JUEGO (18+ pts, ventaja +4)
                            lock_ok = has_active_set and (sets_won >= 2 and diff >= 1 and cur_leader_pts >= 18 and pts_lead >= 4)
                        elif sport_slug == "table-tennis":
                            # Calibrado con CSV IA (100% WR): 2 sets ganados + liderando el set actual EN JUEGO con >=8 pts y ventaja >=3 pts (bloquea descansos Second break)
                            lock_ok = has_active_set and (sets_won >= 2 and diff >= 1 and cur_leader_pts >= 8 and pts_lead >= 3)
                        else:
                            lock_ok = diff >= 3 and any(k in status_lower for k in ["8th", "9th"])
                        add_candidate(
                            "Match Winner", f"WINNER_{leader_side}", 0.0, float(max(diff, pts_lead)),
                            out["name"], leader_side, odds, ovr, lock_ok, 94,
                            f"{sport_label} Winner | {remaining_text} | Sets {h_score}-{a_score} [{cur_h}-{cur_a}]",
                            f"Winner @{odds:.2f} (Sets {h_score}-{a_score}, Pts {cur_h}-{cur_a} | Req: 2 sets + cierre dominante)",
                            "WAIT_CLOSING_LEAD"
                        )

    best_cand = None
    best_ready = False
    if candidates:
        ready_cands = [c for c in candidates if c[0]]
        if ready_cands:
            ready_cands.sort(key=lambda x: (x[1], x[2]["ai_confidence"]), reverse=True)
            best_cand = ready_cands[0][2]
            best_ready = True
        else:
            candidates.sort(key=lambda x: abs(x[2]["odds"] - 1.03))
            best_cand = candidates[0][2]

    ai_candidates = [c[2] for c in candidates if c[2]["odds"] <= AI_CAPTURE_MAX_ODDS]

    return {
        "sport": sport_label,
        "sport_slug": sport_slug,
        "match": f"{home_team} vs {away_team}",
        "score": score_display,
        "minute": clock_display,
        "remaining_text": remaining_text,
        "rem_seconds": rem_seconds,
        "elapsed_seconds": elapsed_seconds,
        "match_status": match_status,
        "stage_bucket": stage_bucket,
        "elapsed_minute": minute,
        "remaining_minute": rem_min if rem_min is not None else -1,
        "h_score": h_score,
        "a_score": a_score,
        "score_diff": diff,
        "total_score": total_score,
        "cur_h": cur_h,
        "cur_a": cur_a,
        "home_game_score": str(ev.get("homeGameScore") or ""),
        "away_game_score": str(ev.get("awayGameScore") or ""),
        "period_scores_json": json.dumps(periods, ensure_ascii=False),
        "all_markets_json": json.dumps(markets, ensure_ascii=False),
        "ready": best_ready,
        "candidate": best_cand,
        "ai_candidates": ai_candidates
    }

def _parse_kambi_livedata_to_ev(sport_slug: str, ld: dict) -> dict:
    score = ld.get("score") or {}
    mc = ld.get("matchClock") or {}
    stats = ld.get("statistics") or {}
    sets_obj = stats.get("sets") or {}

    if sport_slug == "tennis":
        h_sets_arr = [int(x) for x in (sets_obj.get("home") or []) if int(x) >= 0]
        a_sets_arr = [int(x) for x in (sets_obj.get("away") or []) if int(x) >= 0]
        n_sets = min(len(h_sets_arr), len(a_sets_arr))
        period_scores = [{"homeScore": h_sets_arr[i], "awayScore": a_sets_arr[i]} for i in range(n_sets)]
        if n_sets > 0:
            lh, la = h_sets_arr[-1], a_sets_arr[-1]
            last_set_done = (max(lh, la) >= 6 and abs(lh - la) >= 2) or max(lh, la) >= 7
            done_sets = n_sets if last_set_done else max(0, n_sets - 1)
        else:
            done_sets = 0
        h_score = sum(1 for i in range(done_sets) if h_sets_arr[i] > a_sets_arr[i])
        a_score = sum(1 for i in range(done_sets) if a_sets_arr[i] > h_sets_arr[i])
        return {
            "homeScore": h_score,
            "awayScore": a_score,
            "matchStatus": f"Set {max(1, n_sets)}",
            "homeGameScore": score.get("home"),
            "awayGameScore": score.get("away"),
            "clock": {"matchTime": None, "remainingTime": None},
            "periodScores": period_scores
        }

    elif sport_slug in ["volleyball", "table-tennis"]:
        h_sets_arr = [int(x) for x in (sets_obj.get("home") or []) if int(x) >= 0]
        a_sets_arr = [int(x) for x in (sets_obj.get("away") or []) if int(x) >= 0]
        n_sets = min(len(h_sets_arr), len(a_sets_arr))
        period_scores = [{"homeScore": h_sets_arr[i], "awayScore": a_sets_arr[i]} for i in range(n_sets)]
        h_score = sum(1 for i in range(n_sets) if h_sets_arr[i] > a_sets_arr[i])
        a_score = sum(1 for i in range(n_sets) if a_sets_arr[i] > h_sets_arr[i])
        cur_pts_h = int(score.get("home") or 0) if str(score.get("home") or "").isdigit() else 0
        cur_pts_a = int(score.get("away") or 0) if str(score.get("away") or "").isdigit() else 0
        period_scores.append({"homeScore": cur_pts_h, "awayScore": cur_pts_a})
        return {
            "homeScore": h_score,
            "awayScore": a_score,
            "matchStatus": f"Set {n_sets + 1}",
            "homeGameScore": None,
            "awayGameScore": None,
            "clock": {"matchTime": None, "remainingTime": None},
            "periodScores": period_scores
        }

    else:
        h_raw = str(score.get("home") or "0")
        a_raw = str(score.get("away") or "0")
        h_score = int(h_raw) if h_raw.lstrip("-").isdigit() else 0
        a_score = int(a_raw) if a_raw.lstrip("-").isdigit() else 0
        minute = mc.get("minute")
        second = mc.get("second")
        match_time = f"{int(minute):02d}:{int(second or 0):02d}" if minute is not None else None
        rem_m = mc.get("minutesLeftInPeriod")
        rem_s = mc.get("secondsLeftInMinute")
        rem_time = f"{int(rem_m):02d}:{int(rem_s or 0):02d}" if rem_m is not None else None
        period_str = str(mc.get("period") or mc.get("periodId") or "En vivo")
        return {
            "homeScore": h_score,
            "awayScore": a_score,
            "matchStatus": period_str,
            "homeGameScore": None,
            "awayGameScore": None,
            "clock": {"matchTime": match_time, "remainingTime": rem_time},
            "periodScores": []
        }

def _convert_kambi_betoffer_to_market(bo: dict, home_name: str, away_name: str) -> Optional[dict]:
    if not bo:
        return None
    crit = bo.get("criterion") or {}
    cl = str(crit.get("englishLabel") or crit.get("label") or "").strip()
    cl_low = cl.lower()

    if cl_low in ["full time", "match odds - regular time", "regular time"]:
        m_name = "1x2"
    elif cl_low in ["match odds", "moneyline"]:
        m_name = "Winner"
    elif "moneyline" in cl_low and "overtime" in cl_low:
        m_name = "Winner (Incl. Overtime)"
    elif cl_low in ["draw no bet", "draw no bet - regular time"]:
        m_name = "Draw No Bet"
    elif cl_low in ["double chance", "double chance - regular time"]:
        m_name = "Double Chance"
    elif cl_low in ["total goals", "total goals - regular time"]:
        m_name = "Total"
    elif cl_low in ["both teams to score", "both teams to score - regular time"]:
        m_name = "Both Teams to Score"
    else:
        return None

    outcomes = []
    for o in bo.get("outcomes") or []:
        if o.get("status") != "OPEN" or o.get("odds") is None:
            continue
        odds_val = round(float(o["odds"]) / 1000.0, 3)
        if odds_val <= 1.0:
            continue
        raw_lbl = str(o.get("englishLabel") or o.get("label") or o.get("participant") or "").strip()
        if m_name in ["1x2", "Draw No Bet", "Winner", "Winner (Incl. Overtime)"]:
            if raw_lbl == "1":
                out_name = home_name
            elif raw_lbl == "2":
                out_name = away_name
            elif raw_lbl.upper() == "X":
                out_name = "Draw"
            else:
                out_name = raw_lbl
        elif m_name == "Double Chance":
            if raw_lbl.upper() == "1X":
                out_name = f"{home_name} or Draw"
            elif raw_lbl.upper() == "X2":
                out_name = f"Draw or {away_name}"
            elif raw_lbl.upper() == "12":
                out_name = f"{home_name} or {away_name}"
            else:
                out_name = raw_lbl
        elif m_name == "Total":
            line_raw = o.get("line")
            if line_raw is None:
                continue
            line_f = float(line_raw) / 1000.0
            out_name = f"{raw_lbl} {line_f:.1f}"
        else:
            out_name = raw_lbl

        outcomes.append({"name": out_name, "odds": odds_val, "active": True})

    if not outcomes:
        return None
    return {"name": m_name, "status": "active", "outcomes": outcomes}

def fetch_kambi_cloud_fixtures(session: requests.Session) -> List[Tuple[dict, list]]:
    res = session.get(KAMBI_LIVE_OPEN_URL, impersonate="chrome124", timeout=10)
    if res.status_code != 200:
        return []
    live_events = (res.json() or {}).get("liveEvents") or []
    by_slug: Dict[str, list] = {sp["slug"]: [] for sp in SPORTS_CONFIG}

    late_event_ids = []
    parsed_items = []
    for item in live_events:
        ev_info = item.get("event") or {}
        if ev_info.get("state") != "STARTED":
            continue
        k_sport = str(ev_info.get("sport") or "").upper()
        sp_slug = KAMBI_SPORT_MAP.get(k_sport)
        if not sp_slug:
            continue
        eid = ev_info.get("id")
        home_name = str(ev_info.get("homeName") or "Local").strip()
        away_name = str(ev_info.get("awayName") or "Visitante").strip()
        ld = item.get("liveData") or {}
        mc = ld.get("matchClock") or {}
        minute = int(mc.get("minute") or 0)
        period_id = str(mc.get("periodId") or "").upper()

        if (sp_slug == "soccer" and minute >= 65) or (sp_slug == "ice-hockey" and "PERIOD3" in period_id):
            late_event_ids.append(eid)
        parsed_items.append((sp_slug, eid, ev_info, ld, item.get("mainBetOffer"), home_name, away_name))

    extra_markets_map: Dict[Any, List[dict]] = {}
    for eid in late_event_ids[:10]:
        try:
            r_bo = session.get(KAMBI_EVENT_BO_URL.format(event_id=eid), impersonate="chrome124", timeout=5)
            if r_bo.status_code == 200:
                extra_markets_map[eid] = (r_bo.json() or {}).get("betOffers") or []
        except Exception:
            pass

    for sp_slug, eid, ev_info, ld, main_bo, home_name, away_name in parsed_items:
        path = ev_info.get("path") or []
        cat_name = path[1].get("englishName", "") if len(path) > 1 else str(ev_info.get("sport") or "")
        t_name = str(ev_info.get("group") or "")
        ev_status = _parse_kambi_livedata_to_ev(sp_slug, ld)

        mkts = []
        if eid in extra_markets_map:
            for bo in extra_markets_map[eid]:
                m_conv = _convert_kambi_betoffer_to_market(bo, home_name, away_name)
                if m_conv:
                    mkts.append(m_conv)
        if main_bo:
            m_main = _convert_kambi_betoffer_to_market(main_bo, home_name, away_name)
            if m_main:
                mkts.append(m_main)

        fixture_obj = {
            "id": f"kambi_{eid}",
            "slug": f"kambi-{eid}",
            "status": "live",
            "tournament": {"name": t_name, "category": {"name": cat_name}},
            "data": {"competitors": [{"name": home_name}, {"name": away_name}]},
            "eventStatus": ev_status,
            "groups": [{"name": "Main", "templates": [{"name": "Main", "markets": mkts}]}]
        }
        by_slug[sp_slug].append(fixture_obj)

    result = []
    for sp in SPORTS_CONFIG:
        f_list = by_slug.get(sp["slug"], [])[:sp["limit"]]
        result.append((sp, f_list))
    return result

def push_bridge_to_cloud(session: requests.Session, fetched_sports: List[Tuple[dict, list]]):
    if RENDER_EXTERNAL_URL or not CLOUD_BRIDGE_URL:
        return
    try:
        payload = {
            "ts": int(time.time()),
            "fetched_sports": [{"sport": sp, "fixtures": f_list} for sp, f_list in fetched_sports],
            "settled_map": runtime_state.get("recent_settled_map", {})
        }
        session.post(
            f"{CLOUD_BRIDGE_URL.rstrip('/')}/api/bridge_push",
            json=payload,
            timeout=6
        )
    except Exception:
        pass

def process_fetched_sports(fetched_sports: List[Tuple[dict, list]], source_label: str):
    all_radar = []
    sport_counts = {}
    total_live = 0
    live_slugs = set()

    min_odds = runtime_state["min_odds"]
    max_odds = runtime_state["max_odds"]

    for sp, fixtures in fetched_sports:
        sport_counts[sp["label"]] = len(fixtures)
        total_live += len(fixtures)

    bet_live_updates = []
    obs_live_updates = []
    potential_bets = []
    ai_obs_records = []
    now_ts = int(time.time())

    for sp, fixtures in fetched_sports:
        for f in fixtures:
            fid = f.get("id")
            slug = f.get("slug", "")
            if slug:
                live_slugs.add(slug)
            tourney = f.get("tournament") or {}
            cat_name = (tourney.get("category") or {}).get("name", "")
            t_name = tourney.get("name", "")
            league_full = f"{cat_name} - {t_name}".strip(" -")

            if any(b in f"{league_full} {slug}".lower() for b in BLACKLIST):
                continue

            competitors = (f.get("data") or {}).get("competitors") or []
            if len(competitors) < 2:
                continue
            home_team = competitors[0].get("name", "Local")
            away_team = competitors[1].get("name", "Visitante")

            ev = f.get("eventStatus") or {}
            h_score = int(ev.get("homeScore") or 0)
            a_score = int(ev.get("awayScore") or 0)

            markets = extract_active_markets(f)
            eval_res = evaluate_fixture_safety(
                sp["slug"], sp["label"], home_team, away_team, ev, markets, min_odds, max_odds
            )

            bet_live_updates.append((
                h_score, a_score, f"{eval_res['score']} (En vivo)",
                eval_res["minute"], eval_res["remaining_text"],
                eval_res["rem_seconds"], eval_res["elapsed_seconds"], now_ts, fid
            ))
            obs_live_updates.append((
                h_score, a_score, f"{eval_res['score']} (En vivo)",
                eval_res["remaining_text"], eval_res["rem_seconds"], fid
            ))

            cand = eval_res["candidate"]
            all_radar.append({
                "sport": sp["label"],
                "sport_slug": sp["slug"],
                "match": eval_res["match"],
                "league": league_full,
                "score": eval_res["score"],
                "minute": eval_res["minute"],
                "remaining_text": eval_res["remaining_text"],
                "rem_seconds": eval_res["rem_seconds"],
                "elapsed_seconds": eval_res["elapsed_seconds"],
                "clock_updated_at": now_ts,
                "market_name": cand["market_name"] if cand else "Monitoreando",
                "selection": cand["selection"] if cand else "-",
                "leader_odds": cand["odds"] if cand else None,
                "ai_confidence": cand["ai_confidence"] if cand else None,
                "ready": eval_res["ready"],
                "status_note": ("✅ ¡ENTRADA SEGURA! " + cand["safety_reason"]) if eval_res["ready"] else (
                    cand["wait_note"] if cand else "Monitoreando mercados..."
                )
            })

            for ac in eval_res["ai_candidates"]:
                obs_key = f"{fid}|{ac['market_type']}|{ac['line']}|{eval_res['stage_bucket']}"
                ai_obs_records.append({
                    "obs_key": obs_key,
                    "fixture_id": fid,
                    "slug": slug,
                    "sport_slug": sp["slug"],
                    "sport": sp["label"],
                    "category": cat_name,
                    "tournament": t_name,
                    "league": league_full,
                    "home_team": home_team,
                    "away_team": away_team,
                    "match_name": eval_res["match"],
                    "match_status": eval_res["match_status"],
                    "stage_bucket": eval_res["stage_bucket"],
                    "clock_display": eval_res["minute"],
                    "remaining_text": eval_res["remaining_text"],
                    "rem_seconds": eval_res["rem_seconds"],
                    "elapsed_minute": eval_res["elapsed_minute"],
                    "remaining_minute": eval_res["remaining_minute"],
                    "home_score": h_score,
                    "away_score": a_score,
                    "score_diff": eval_res["score_diff"],
                    "total_score": eval_res["total_score"],
                    "current_period_h": eval_res["cur_h"],
                    "current_period_a": eval_res["cur_a"],
                    "home_game_score": eval_res["home_game_score"],
                    "away_game_score": eval_res["away_game_score"],
                    "period_scores_json": eval_res["period_scores_json"],
                    "market_name": ac["market_name"],
                    "market_type": ac["market_type"],
                    "line": ac["line"],
                    "cushion": ac["cushion"],
                    "selection": ac["selection"],
                    "side": ac["side"],
                    "odds": ac["odds"],
                    "implied_prob": round(1.0 / ac["odds"], 4) if ac["odds"] > 0 else 0.0,
                    "market_overround": ac["overround"],
                    "ai_confidence": ac["ai_confidence"],
                    "was_entered": 1 if (runtime_state["is_running"] and ac["lock_ok"] and ac["odds_ok"] and cand == ac) else 0,
                    "reject_code": "ENTERED" if (runtime_state["is_running"] and ac["lock_ok"] and ac["odds_ok"] and cand == ac) else (
                        "BOT_PAUSED" if (not runtime_state["is_running"] and ac["lock_ok"] and ac["odds_ok"]) else ac["reject_code"]
                    ),
                    "reject_reason": ac["safety_reason"] if (runtime_state["is_running"] and ac["lock_ok"] and ac["odds_ok"] and cand == ac) else ac["wait_note"],
                    "all_markets_json": eval_res["all_markets_json"],
                    "last_h": h_score,
                    "last_a": a_score,
                    "timestamp": now_ts
                })

            if runtime_state["is_running"] and eval_res["ready"] and cand:
                potential_bets.append({
                    "fid": fid,
                    "slug": slug,
                    "sport": sp["label"],
                    "match": eval_res["match"],
                    "league": league_full,
                    "cand": cand,
                    "minute": eval_res["minute"],
                    "remaining_text": eval_res["remaining_text"],
                    "rem_seconds": eval_res["rem_seconds"],
                    "elapsed_seconds": eval_res["elapsed_seconds"],
                    "score_display": eval_res["score"],
                    "h_score": h_score,
                    "a_score": a_score
                })

    runtime_state["active_live_slugs"] = live_slugs

    with DB_LOCK:
        conn = get_db()
        try:
            c = conn.cursor()
            if bet_live_updates:
                c.executemany("""
                    UPDATE bets
                    SET last_h = CASE WHEN (? + ?) >= (last_h + last_a) THEN ? ELSE last_h END,
                        last_a = CASE WHEN (? + ?) >= (last_h + last_a) THEN ? ELSE last_a END,
                        final_score = ?,
                        live_clock = ?, remaining_text = ?, rem_seconds = ?,
                        elapsed_seconds = ?, clock_updated_at = ?
                    WHERE fixture_id = ? AND status = 'PENDING'
                """, [
                    (u[0], u[1], u[0], u[0], u[1], u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8])
                    for u in bet_live_updates
                ])
            if obs_live_updates:
                c.executemany("""
                    UPDATE ai_observations
                    SET last_h = CASE WHEN (? + ?) >= (last_h + last_a) THEN ? ELSE last_h END,
                        last_a = CASE WHEN (? + ?) >= (last_h + last_a) THEN ? ELSE last_a END,
                        final_score = ?,
                        remaining_text = ?, rem_seconds = ?
                    WHERE fixture_id = ? AND status = 'PENDING'
                """, [
                    (u[0], u[1], u[0], u[0], u[1], u[1], u[2], u[3], u[4], u[5])
                    for u in obs_live_updates
                ])

            for pb in potential_bets:
                c.execute("SELECT id FROM bets WHERE fixture_id = ?", (pb["fid"],))
                if c.fetchone():
                    continue
                stats = _compute_stats_from_cursor(c)
                stake_to_use = runtime_state["stake_amount"]
                if stats["available"] >= stake_to_use:
                    cand = pb["cand"]
                    c.execute("""
                        INSERT INTO bets (
                            fixture_id, slug, sport, match_name, league,
                            market_name, market_type, line, selection, side,
                            odds, stake, minute, score_at_bet, final_score,
                            live_clock, remaining_text, rem_seconds, elapsed_seconds, clock_updated_at,
                            safety_reason, ai_confidence, last_h, last_a, timestamp
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        pb["fid"], pb["slug"], pb["sport"], pb["match"], pb["league"],
                        cand["market_name"], cand["market_type"], cand["line"],
                        cand["selection"], cand["side"], cand["odds"], stake_to_use,
                        pb["minute"], pb["score_display"], f"{pb['score_display']} (En vivo)",
                        pb["minute"], pb["remaining_text"], pb["rem_seconds"], pb["elapsed_seconds"], now_ts,
                        cand["safety_reason"], cand["ai_confidence"], pb["h_score"], pb["a_score"], now_ts
                    ))

            for obs in ai_obs_records:
                c.execute("""
                    INSERT OR IGNORE INTO ai_observations (
                        obs_key, fixture_id, slug, sport_slug, sport, category, tournament, league,
                        home_team, away_team, match_name, match_status, stage_bucket, clock_display,
                        remaining_text, rem_seconds, elapsed_minute, remaining_minute,
                        home_score, away_score, score_diff, total_score,
                        current_period_h, current_period_a, home_game_score, away_game_score, period_scores_json,
                        market_name, market_type, line, cushion, selection, side, odds, implied_prob,
                        market_overround, ai_confidence, was_entered, reject_code, reject_reason,
                        all_markets_json, last_h, last_a, final_score, timestamp
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?
                    )
                """, (
                    obs["obs_key"], obs["fixture_id"], obs["slug"], obs["sport_slug"], obs["sport"],
                    obs["category"], obs["tournament"], obs["league"], obs["home_team"], obs["away_team"],
                    obs["match_name"], obs["match_status"], obs["stage_bucket"], obs["clock_display"],
                    obs["remaining_text"], obs["rem_seconds"], obs["elapsed_minute"], obs["remaining_minute"],
                    obs["home_score"], obs["away_score"], obs["score_diff"], obs["total_score"],
                    obs["current_period_h"], obs["current_period_a"], obs["home_game_score"], obs["away_game_score"],
                    obs["period_scores_json"], obs["market_name"], obs["market_type"], obs["line"], obs["cushion"],
                    obs["selection"], obs["side"], obs["odds"], obs["implied_prob"], obs["market_overround"],
                    obs["ai_confidence"], obs["was_entered"], obs["reject_code"], obs["reject_reason"],
                    obs["all_markets_json"], obs["last_h"], obs["last_a"], f"{obs['last_h']}-{obs['last_a']} (En vivo)",
                    obs["timestamp"]
                ))
            conn.commit()
        finally:
            conn.close()

    def radar_sort_key(item):
        odds = item["leader_odds"]
        if item["ready"]:
            return (0, odds or 99.0)
        if odds is not None and min_odds <= odds <= max_odds:
            return (1, odds)
        if odds is not None and 1.01 <= odds <= 1.25:
            return (2, odds)
        if odds is not None:
            return (3, odds)
        return (4, 99.0)

    all_radar.sort(key=radar_sort_key)
    runtime_state["live_radar"] = all_radar[:80]
    runtime_state["sport_counts"] = sport_counts
    runtime_state["last_scan_time"] = time.strftime("%H:%M:%S")
    runtime_state["connection_status"] = f"Conectado a {source_label} ({total_live} eventos en vivo en {len(SPORTS_CONFIG)} deportes)"

def scan_live_matches(session: requests.Session):
    now_ts = int(time.time())
    headers = {
        "Content-Type": "application/json",
        "Origin": "https://stake.com",
        "Referer": "https://stake.com/sports/live"
    }

    # 1. Si hay datos frescos del Bridge Stake GraphQL (< 25s), usarlos prioritariamente en la nube
    bridge = runtime_state.get("bridge_data")
    if bridge and (now_ts - int(bridge.get("ts") or 0) <= 25):
        raw_list = bridge.get("fetched_sports") or []
        fetched_sports = [(item["sport"], item["fixtures"]) for item in raw_list if "sport" in item and "fixtures" in item]
        if fetched_sports:
            process_fetched_sports(fetched_sports, "Stake GraphQL (Bridge en Vivo)")
            return

    # 2. Intentar Stake GraphQL directo (si no está en cooldown por bloqueo de IP de datacenter)
    fetched_sports = []
    stake_ok = False
    if now_ts >= runtime_state.get("stake_blocked_until", 0):
        for idx, sp in enumerate(SPORTS_CONFIG):
            payload = {
                "operationName": "LiveSportFixtures",
                "variables": {"sportSlug": sp["slug"], "limit": sp["limit"]},
                "query": LIVE_SPORT_QUERY
            }
            try:
                res = session.post(STAKE_GQL_URL, json=payload, headers=headers, impersonate="chrome124", timeout=8)
                if res.status_code != 200:
                    if idx == 0 and res.status_code in [403, 429, 503]:
                        # IP de datacenter bloqueada por Cloudflare en stake.com -> activar motor Cloud 24/7 sin demora
                        runtime_state["stake_blocked_until"] = now_ts + 120
                        break
                    continue
                fixtures = ((res.json().get("data") or {}).get("slugSport") or {}).get("fixtureList") or []
                fetched_sports.append((sp, fixtures))
                stake_ok = True
            except Exception:
                if idx == 0:
                    runtime_state["stake_blocked_until"] = now_ts + 60
                    break
                continue

    if stake_ok and fetched_sports:
        process_fetched_sports(fetched_sports, "Stake GraphQL")
        push_bridge_to_cloud(session, fetched_sports)
        return

    # 3. Motor Cloud 24/7 (Kambi Live Sportsbook CDN) — funciona 100% en Render sin bloqueos Cloudflare
    try:
        cloud_sports = fetch_kambi_cloud_fixtures(session)
        if cloud_sports:
            process_fetched_sports(cloud_sports, "Motor Multi-Deporte Cloud 24/7")
            return
    except Exception:
        pass

    runtime_state["connection_status"] = "Reconectando con Motor Multi-Deporte..."

def resolve_market_outcome(market_type: str, line: float, final_h: int, final_a: int) -> str:
    if market_type == "WINNER_HOME":
        return "WON" if final_h > final_a else "LOST"
    elif market_type == "WINNER_AWAY":
        return "WON" if final_a > final_h else "LOST"
    elif market_type == "DNB_HOME":
        if final_h > final_a:
            return "WON"
        elif final_h == final_a:
            return "PUSH"
        return "LOST"
    elif market_type == "DNB_AWAY":
        if final_a > final_h:
            return "WON"
        elif final_h == final_a:
            return "PUSH"
        return "LOST"
    elif market_type == "DC_1X":
        return "WON" if final_h >= final_a else "LOST"
    elif market_type == "DC_X2":
        return "WON" if final_a >= final_h else "LOST"
    elif market_type == "UNDER_LINE":
        return "WON" if (final_h + final_a) < line else "LOST"
    elif market_type == "BTTS_NO":
        return "WON" if (final_h == 0 or final_a == 0) else "LOST"
    return "WON" if final_h != final_a else "LOST"

def infer_sport_slug_from_label(label: str) -> str:
    for sp in SPORTS_CONFIG:
        if sp["label"] == label:
            return sp["slug"]
    return "soccer"

def _extract_set_counts_from_periods(periods: list) -> Tuple[int, int]:
    """Cuenta los sets ganados por Local y Visitante a partir de periodScores (evita el bug 0-0 de Stake al finalizar Tenis/Voleibol/Ping Pong)."""
    p_h = 0
    p_a = 0
    for p in (periods or []):
        ph = int(p.get("homeScore") or 0)
        pa = int(p.get("awayScore") or 0)
        if ph > pa:
            p_h += 1
        elif pa > ph:
            p_a += 1
    return p_h, p_a

def settle_pending_bets(session: requests.Session):
    with DB_LOCK:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("""
                SELECT id, fixture_id, slug, sport, market_type, line, side, odds, stake, last_h, last_a, timestamp, score_at_bet
                FROM bets WHERE status = 'PENDING'
            """)
            pending_bets = c.fetchall()

            c.execute("""
                SELECT id, fixture_id, slug, sport_slug, market_type, line, side, odds, last_h, last_a, timestamp
                FROM ai_observations WHERE status = 'PENDING'
            """)
            pending_obs = c.fetchall()
        finally:
            conn.close()

    if not pending_bets and not pending_obs:
        return

    now_ts = int(time.time())
    bet_settlements = []
    obs_settlements = []
    in_play_bet_clock_updates = []

    real_bets_by_slug: Dict[str, list] = {}
    for b in pending_bets:
        b_id, f_id, slug, sp_label, m_type, line, side, odds, stake, last_h, last_a, ts, sc_bet = b
        if str(f_id).startswith("sim_test_"):
            elapsed_sim = now_ts - ts
            if elapsed_sim >= 15:
                pnl = round(stake * (odds - 1.0), 2)
                bet_settlements.append(("WON", pnl, sc_bet, b_id))
            else:
                rem_sim = max(0, 15 - elapsed_sim)
                in_play_bet_clock_updates.append((
                    last_h, last_a, f"{sc_bet} (En vivo)",
                    f"Simulación ({rem_sim}s)", f"⏱️ Restan 00:{rem_sim:02d}",
                    rem_sim, elapsed_sim, now_ts, b_id
                ))
        elif slug:
            real_bets_by_slug.setdefault(slug, []).append(b)

    obs_by_slug: Dict[str, list] = {}
    for o in pending_obs:
        o_id, f_id, slug, sp_slug, m_type, line, side, odds, last_h, last_a, ts = o
        if str(f_id).startswith("sim_test_"):
            if now_ts - ts >= 15:
                roi = round(odds - 1.0, 4)
                obs_settlements.append(("WON", 1, roi, last_h, last_a, f"{last_h}-{last_a}", now_ts, o_id))
        elif slug:
            obs_by_slug.setdefault(slug, []).append(o)

    all_slugs = set(real_bets_by_slug.keys()) | set(obs_by_slug.keys())
    live_slugs = runtime_state.get("active_live_slugs", set())
    last_check = runtime_state["last_settle_check"]

    slugs_to_query = []
    for sl in all_slugs:
        is_in_bets = sl in real_bets_by_slug
        is_still_live = sl in live_slugs
        last_t = last_check.get(sl, 0)
        if is_in_bets or (not is_still_live and now_ts - last_t >= 12) or (now_ts - last_t >= 45):
            slugs_to_query.append((0 if is_in_bets else (1 if not is_still_live else 2), last_t, sl))

    slugs_to_query.sort(key=lambda x: (x[0], x[1]))
    selected_slugs = [x[2] for x in slugs_to_query[:18]]

    headers = {"Content-Type": "application/json", "Origin": "https://stake.com"}
    settled_map = runtime_state.get("recent_settled_map", {})

    for slug in selected_slugs:
        last_check[slug] = now_ts
        try:
            data = None
            if slug in settled_map:
                data = settled_map[slug]
            elif slug.startswith("kambi-"):
                eid = slug.replace("kambi-", "")
                # Determinar el sport_slug para parsear sets o goles
                sample_sp_slug = "soccer"
                if slug in real_bets_by_slug:
                    sample_sp_slug = infer_sport_slug_from_label(real_bets_by_slug[slug][0][3] or "⚽ Fútbol")
                elif slug in obs_by_slug:
                    sample_sp_slug = obs_by_slug[slug][0][3] or "soccer"

                r_live = session.get(KAMBI_EVENT_LIVEDATA_URL.format(event_id=eid), impersonate="chrome124", timeout=6)
                r_bo = session.get(KAMBI_EVENT_BO_URL.format(event_id=eid), impersonate="chrome124", timeout=6)

                if r_live.status_code == 404 and slug not in live_slugs:
                    # El partido ya terminó y salió del buffer en vivo de Kambi; liquidar con último marcador conocido
                    data = {"status": "ended", "eventStatus": {"homeScore": None, "awayScore": None, "matchStatus": "ended"}}
                elif r_live.status_code == 200:
                    ld = (r_live.json() or {}).get("liveData") or {}
                    ev_k = _parse_kambi_livedata_to_ev(sample_sp_slug, ld)
                    is_k_done = False
                    if r_bo.status_code == 404 and slug not in live_slugs:
                        is_k_done = True
                    elif r_bo.status_code == 200:
                        bo_json = r_bo.json() or {}
                        evs_arr = bo_json.get("events") or []
                        ev_state = str(evs_arr[0].get("state") or "") if evs_arr else ""
                        bos_arr = bo_json.get("betOffers") or []
                        if ev_state == "FINISHED" or (slug not in live_slugs and len(bos_arr) == 0):
                            is_k_done = True
                    data = {
                        "status": "ended" if is_k_done else "live",
                        "eventStatus": ev_k
                    }
            else:
                if now_ts < runtime_state.get("stake_blocked_until", 0):
                    continue
                payload = {"operationName": "SlugFixtureResult", "variables": {"slug": slug}, "query": SLUG_FIXTURE_QUERY}
                res = session.post(STAKE_GQL_URL, json=payload, headers=headers, impersonate="chrome124", timeout=8)
                if res.status_code != 200:
                    if res.status_code in [403, 429, 503]:
                        runtime_state["stake_blocked_until"] = now_ts + 90
                    continue
                data = (res.json().get("data") or {}).get("slugFixture")

            if not data:
                continue

            ev = data.get("eventStatus") or {}
            match_status = str(ev.get("matchStatus") or "").lower()
            fixture_status = str(data.get("status") or "").lower()

            is_finished = fixture_status in ["ended", "closed", "settled"]
            ev_h = ev.get("homeScore")
            ev_a = ev.get("awayScore")
            p_h, p_a = _extract_set_counts_from_periods(ev.get("periodScores") or [])

            if not is_finished:
                # Actualizar el reloj restante en vivo de las apuestas activas aunque no estén en top fixtures
                for b in real_bets_by_slug.get(slug, []):
                    b_id, f_id, _, sp_label, m_type, line, side, odds, stake, last_h, last_a, ts, sc_bet = b
                    sp_slug = infer_sport_slug_from_label(sp_label or "⚽ Fútbol")
                    tm = compute_live_timing(sp_slug, ev, f_id)
                    raw_h = int(ev_h) if ev_h is not None else int(last_h or 0)
                    raw_a = int(ev_a) if ev_a is not None else int(last_a or 0)
                    if (raw_h + raw_a) < (int(last_h or 0) + int(last_a or 0)):
                        cur_h, cur_a = int(last_h or 0), int(last_a or 0)
                    else:
                        cur_h, cur_a = raw_h, raw_a
                    in_play_bet_clock_updates.append((
                        cur_h, cur_a, f"{tm['score_display']} (En vivo)",
                        tm["clock_display"], tm["remaining_text"],
                        tm["rem_seconds"], tm["elapsed_seconds"], now_ts, b_id
                    ))
                continue

            can_cache_settled = True
            for b in real_bets_by_slug.get(slug, []):
                b_id, f_id, _, sp_label, m_type, line, side, odds, stake, last_h, last_a, ts, sc_bet = b
                raw_h = int(ev_h) if ev_h is not None else int(last_h or 0)
                raw_a = int(ev_a) if ev_a is not None else int(last_a or 0)
                final_h = raw_h if (raw_h + raw_a) >= (int(last_h or 0) + int(last_a or 0)) else int(last_h or 0)
                final_a = raw_a if (raw_h + raw_a) >= (int(last_h or 0) + int(last_a or 0)) else int(last_a or 0)

                if sp_label in ["🎾 Tenis", "🏐 Voleibol", "🏓 Ping Pong"]:
                    if (p_h + p_a) >= (final_h + final_a) and p_h != p_a:
                        final_h, final_a = p_h, p_a
                    elif slug.startswith("kambi-") and final_h == final_a:
                        p_scores = ev.get("periodScores") or []
                        if p_scores:
                            lp = p_scores[-1]
                            lph, lpa = int(lp.get("homeScore") or 0), int(lp.get("awayScore") or 0)
                            if lph > lpa:
                                final_h += 1
                            elif lpa > lph:
                                final_a += 1
                        elif side == "HOME":
                            final_h += 1
                        elif side == "AWAY":
                            final_a += 1
                    # Evitar liquidar en el segundo de transición donde Stake aún no subió el 2do/3er set ganador (< 2 sets)
                    if max(final_h, final_a) < 2 and (now_ts - ts) < 7200:
                        can_cache_settled = False
                        continue
                # Si es un deporte con prórroga obligatoria y el marcador aún aparece empatado, esperar al resultado final de OT/Penaltis
                if sp_label != "⚽ Fútbol" and m_type in ["WINNER_HOME", "WINNER_AWAY"] and final_h == final_a:
                    can_cache_settled = False
                    continue
                suffix = " (OT)" if match_status in ["aet", "ap", "overtime", "penalties"] else ""
                final_str = f"{final_h}-{final_a}{suffix}"
                outcome = resolve_market_outcome(m_type or f"WINNER_{side}", float(line or 0.0), final_h, final_a)
                if outcome == "WON":
                    pnl = round(stake * (odds - 1.0), 2)
                elif outcome == "PUSH":
                    pnl = 0.0
                else:
                    pnl = -stake
                bet_settlements.append((outcome, pnl, final_str, b_id))

            for o in obs_by_slug.get(slug, []):
                o_id, f_id, _, sp_slug, m_type, line, side, odds, last_h, last_a, ts = o
                raw_h = int(ev_h) if ev_h is not None else int(last_h or 0)
                raw_a = int(ev_a) if ev_a is not None else int(last_a or 0)
                final_h = raw_h if (raw_h + raw_a) >= (int(last_h or 0) + int(last_a or 0)) else int(last_h or 0)
                final_a = raw_a if (raw_h + raw_a) >= (int(last_h or 0) + int(last_a or 0)) else int(last_a or 0)

                if sp_slug in ["tennis", "volleyball", "table-tennis"]:
                    if (p_h + p_a) >= (final_h + final_a) and p_h != p_a:
                        final_h, final_a = p_h, p_a
                    elif slug.startswith("kambi-") and final_h == final_a:
                        p_scores = ev.get("periodScores") or []
                        if p_scores:
                            lp = p_scores[-1]
                            lph, lpa = int(lp.get("homeScore") or 0), int(lp.get("awayScore") or 0)
                            if lph > lpa:
                                final_h += 1
                            elif lpa > lph:
                                final_a += 1
                        elif side == "HOME":
                            final_h += 1
                        elif side == "AWAY":
                            final_a += 1
                    if max(final_h, final_a) < 2 and (now_ts - ts) < 7200:
                        can_cache_settled = False
                        continue
                if sp_slug != "soccer" and m_type in ["WINNER_HOME", "WINNER_AWAY"] and final_h == final_a:
                    can_cache_settled = False
                    continue
                final_str = f"{final_h}-{final_a}"
                outcome = resolve_market_outcome(m_type or f"WINNER_{side}", float(line or 0.0), final_h, final_a)
                if outcome == "WON":
                    target_win = 1
                    roi = round(odds - 1.0, 4)
                elif outcome == "PUSH":
                    target_win = None
                    roi = 0.0
                else:
                    target_win = 0
                    roi = -1.0
                obs_settlements.append((outcome, target_win, roi, final_h, final_a, final_str, now_ts, o_id))

            if is_finished and can_cache_settled and not slug.startswith("kambi-"):
                settled_map[slug] = data

        except Exception:
            pass

    if bet_settlements or obs_settlements or in_play_bet_clock_updates:
        with DB_LOCK:
            conn = get_db()
            try:
                c = conn.cursor()
                if in_play_bet_clock_updates:
                    c.executemany("""
                        UPDATE bets
                        SET last_h = ?, last_a = ?, final_score = ?,
                            live_clock = ?, remaining_text = ?, rem_seconds = ?,
                            elapsed_seconds = ?, clock_updated_at = ?
                        WHERE id = ? AND status = 'PENDING'
                    """, in_play_bet_clock_updates)
                for status, pnl, final_str, b_id in bet_settlements:
                    c.execute("UPDATE account SET val = val + ? WHERE key = 'equity'", (pnl,))
                    c.execute("""
                        UPDATE bets
                        SET status = ?, pnl = ?, final_score = ?,
                            remaining_text = '🏁 Partido Finalizado', rem_seconds = -1
                        WHERE id = ? AND status = 'PENDING'
                    """, (status, pnl, final_str, b_id))
                for status, target_win, roi, fh, fa, final_str, sat, o_id in obs_settlements:
                    c.execute("""
                        UPDATE ai_observations
                        SET status = ?, target_win = ?, hypothetical_roi = ?,
                            final_h = ?, final_a = ?, final_score = ?,
                            remaining_text = '🏁 Finalizado', rem_seconds = -1, settled_at = ?
                        WHERE id = ? AND status = 'PENDING'
                    """, (status, target_win, roi, fh, fa, final_str, sat, o_id))
                conn.commit()
            finally:
                conn.close()

# ==================== WORKER EN SEGUNDO PLANO ====================
async def background_worker():
    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
    session = requests.Session(proxies=proxies)
    last_ping = 0
    while True:
        try:
            await asyncio.to_thread(scan_live_matches, session)
            await asyncio.to_thread(settle_pending_bets, session)
            # Auto-KeepAlive para Render Free Tier (evita que se duerma a los 15 minutos)
            now_ts = int(time.time())
            if RENDER_EXTERNAL_URL and (now_ts - last_ping >= 240):
                last_ping = now_ts
                await asyncio.to_thread(
                    lambda: session.get(f"{RENDER_EXTERNAL_URL.rstrip('/')}/api/health", timeout=8)
                )
        except Exception as e:
            runtime_state["connection_status"] = f"Error ciclo: {e}"
        await asyncio.sleep(SCAN_INTERVAL_SEC)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(background_worker())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

# ==================== ENDPOINTS REST ====================
class StakeConfig(BaseModel):
    stake_amount: float
    min_odds: Optional[float] = None
    max_odds: Optional[float] = None

@app.get("/api/health")
def api_health():
    return {"status": "ok", "timestamp": int(time.time())}

@app.get("/api/dashboard")
def api_dashboard():
    now_ts = int(time.time())
    with DB_LOCK:
        conn = get_db()
        try:
            c = conn.cursor()
            stats = _compute_stats_from_cursor(c)
            ai_insights = _compute_ai_model_insights(c)

            c.execute("""
                SELECT id, sport, match_name, league, market_name, selection, odds, stake,
                       minute, score_at_bet, final_score, live_clock, remaining_text,
                       rem_seconds, elapsed_seconds, clock_updated_at,
                       safety_reason, ai_confidence, status, pnl
                FROM bets ORDER BY id DESC LIMIT 50
            """)
            bet_rows = c.fetchall()

            c.execute("""
                SELECT id, sport, match_name, league, stage_bucket, clock_display,
                       remaining_text, rem_seconds,
                       home_score, away_score, market_name, selection, odds, implied_prob,
                       market_overround, ai_confidence, was_entered, reject_code, reject_reason,
                       final_score, status, hypothetical_roi, timestamp
                FROM ai_observations
                ORDER BY id DESC LIMIT 120
            """)
            obs_rows = c.fetchall()
        finally:
            conn.close()

    bet_keys = [
        "id", "sport", "match", "league", "market_name", "selection",
        "odds", "stake", "minute", "score_at_bet", "final_score",
        "live_clock", "remaining_text", "rem_seconds", "elapsed_seconds", "clock_updated_at",
        "safety_reason", "ai_confidence", "status", "pnl"
    ]
    obs_keys = [
        "id", "sport", "match", "league", "stage_bucket", "clock_display",
        "remaining_text", "rem_seconds",
        "home_score", "away_score", "market_name", "selection", "odds", "implied_prob",
        "market_overround", "ai_confidence", "was_entered", "reject_code", "reject_reason",
        "final_score", "status", "hypothetical_roi", "timestamp"
    ]
    return {
        "server_ts": now_ts,
        "stats": stats,
        "radar": runtime_state["live_radar"],
        "bets": [dict(zip(bet_keys, r)) for r in bet_rows],
        "ai_observations": [dict(zip(obs_keys, r)) for r in obs_rows],
        "ai_insights": ai_insights
    }

@app.get("/api/export_ai_csv")
def export_ai_csv():
    with DB_LOCK:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM ai_observations ORDER BY id DESC")
            cols = [desc[0] for desc in c.description]
            rows = c.fetchall()
        finally:
            conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(cols)
    writer.writerows(rows)
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=stake_ai_dataset_full.csv"}
    )

@app.get("/api/export_ai_json")
def export_ai_json():
    with DB_LOCK:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM ai_observations ORDER BY id DESC LIMIT 5000")
            cols = [desc[0] for desc in c.description]
            rows = [dict(zip(cols, r)) for r in c.fetchall()]
        finally:
            conn.close()
    return JSONResponse(
        content={"total_exported": len(rows), "dataset": rows},
        headers={"Content-Disposition": "attachment; filename=stake_ai_dataset_full.json"}
    )

@app.post("/api/toggle")
def api_toggle():
    runtime_state["is_running"] = not runtime_state["is_running"]
    return {"is_running": runtime_state["is_running"]}

@app.post("/api/stake")
def api_set_stake(cfg: StakeConfig):
    if 1.0 <= cfg.stake_amount <= 30000.0:
        runtime_state["stake_amount"] = round(cfg.stake_amount, 2)
    if cfg.min_odds is not None and 1.001 <= cfg.min_odds <= 1.25:
        runtime_state["min_odds"] = round(cfg.min_odds, 3)
    if cfg.max_odds is not None and runtime_state["min_odds"] <= cfg.max_odds <= 1.35:
        runtime_state["max_odds"] = round(cfg.max_odds, 3)
    return {
        "stake_amount": runtime_state["stake_amount"],
        "min_odds": runtime_state["min_odds"],
        "max_odds": runtime_state["max_odds"]
    }

@app.post("/api/simulate_test")
def api_simulate_test():
    with DB_LOCK:
        conn = get_db()
        try:
            c = conn.cursor()
            stats = _compute_stats_from_cursor(c)
            stake = runtime_state["stake_amount"]
            if stats["available"] < stake:
                return {"error": "Capital disponible insuficiente"}
            fid = f"sim_test_{int(time.time() * 1000)}"
            now_ts = int(time.time())
            c.execute("""
                INSERT INTO bets (
                    fixture_id, slug, sport, match_name, league, market_name, market_type,
                    line, selection, side, odds, stake, minute, score_at_bet, final_score,
                    live_clock, remaining_text, rem_seconds, elapsed_seconds, clock_updated_at,
                    safety_reason, ai_confidence, last_h, last_a, timestamp
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fid, "test-match", "🏒 Hockey", "Washington Capitals vs Minnesota Wild (Sim)",
                "USA - NHL", "Winner (Incl. OT & Pen)", "WINNER_AWAY", 0.0,
                "Minnesota Wild", "AWAY", 1.02, stake, "3rd period (00:15)",
                "1 - 3", "1 - 3 (En vivo)", "3rd period (00:15)", "⏱️ Restan 00:15 (3rd period)", 15, 3585, now_ts,
                "Hockey 3er Período | Ventaja +2 goles restando 00:15",
                98.7, 1, 3, now_ts
            ))
            c.execute("""
                INSERT OR IGNORE INTO ai_observations (
                    obs_key, fixture_id, slug, sport_slug, sport, category, tournament, league,
                    home_team, away_team, match_name, match_status, stage_bucket, clock_display,
                    remaining_text, rem_seconds, elapsed_minute, remaining_minute,
                    home_score, away_score, score_diff, total_score,
                    current_period_h, current_period_a, home_game_score, away_game_score, period_scores_json,
                    market_name, market_type, line, cushion, selection, side, odds, implied_prob,
                    market_overround, ai_confidence, was_entered, reject_code, reject_reason,
                    all_markets_json, last_h, last_a, final_score, timestamp
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?
                )
            """, (
                f"{fid}|WINNER_AWAY|0.0|P3_FINAL", fid, "test-match", "ice-hockey", "🏒 Hockey",
                "USA", "NHL", "USA - NHL", "Washington Capitals", "Minnesota Wild",
                "Washington Capitals vs Minnesota Wild (Sim)", "3rd period", "P3_FINAL", "3rd period (00:15)",
                "⏱️ Restan 00:15 (3rd period)", 15, 59, 0,
                1, 3, 2, 4, 0, 1, "", "", "[]",
                "Winner (Incl. OT & Pen)", "WINNER_AWAY", 0.0, 2.0, "Minnesota Wild", "AWAY",
                1.02, 0.9804, 0.032, 98.7, 1, "ENTERED", "Hockey 3er Período | Ventaja +2 goles restando 00:15",
                "[]", 1, 3, "1 - 3 (En vivo)", now_ts
            ))
            conn.commit()
        finally:
            conn.close()
    return {"status": "ok"}

class BridgePayload(BaseModel):
    ts: int
    fetched_sports: List[Dict[str, Any]]
    settled_map: Optional[Dict[str, Any]] = None

@app.post("/api/bridge_push")
def api_bridge_push(payload: BridgePayload):
    runtime_state["bridge_data"] = {
        "ts": int(time.time()),
        "fetched_sports": payload.fetched_sports
    }
    if payload.settled_map:
        runtime_state["recent_settled_map"].update(payload.settled_map)
    return {"status": "ok"}

@app.post("/api/reset")
def api_reset():
    with DB_LOCK:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("DELETE FROM bets")
            c.execute("UPDATE account SET val = ? WHERE key = 'equity'", (INITIAL_BALANCE,))
            conn.commit()
        finally:
            conn.close()
    return {"status": "ok"}

# ==================== FRONTEND EMBEBIDO ====================
HTML_PAGE = """
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <title>Stake Multi-Sport Ultra-Safe Engine + AI Data Lake</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 font-sans p-4 md:p-6 min-h-screen">
  <div class="max-w-[1520px] mx-auto space-y-5">

    <!-- Header & Controls -->
    <div class="flex flex-wrap items-center justify-between gap-4 bg-slate-900/90 border border-slate-800 p-4 rounded-xl shadow-lg">
      <div>
        <div class="flex flex-wrap items-center gap-2.5">
          <h1 class="text-xl font-bold text-emerald-400">Stake Multi-Sport Ultra-Safe + AI Data Lake</h1>
          <span id="badge-status" class="px-2.5 py-0.5 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">ACTIVO</span>
          <span class="px-2 py-0.5 rounded text-[11px] font-mono bg-indigo-500/10 text-indigo-300 border border-indigo-500/20">Reloj Restante en Vivo (1s) + Telemetría IA</span>
        </div>
        <p id="conn-info" class="text-xs text-slate-400 mt-1">Sincronizando todos los deportes en vivo desde Stake GraphQL...</p>
      </div>

      <div class="flex flex-wrap items-center gap-2">
        <div class="flex items-center bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1">
          <span class="text-xs text-slate-400 mr-1.5">Stake (USDT):</span>
          <input id="input-stake" type="number" value="500" class="w-20 bg-transparent text-sm font-bold text-white focus:outline-none" onchange="updateConfig()">
        </div>
        <div class="flex items-center bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1">
          <span class="text-xs text-slate-400 mr-1.5">Cuota Entrada:</span>
          <input id="input-min-odds" type="number" step="0.01" value="1.01" class="w-14 bg-transparent text-sm font-bold text-emerald-400 focus:outline-none" onchange="updateConfig()">
          <span class="text-xs text-slate-500 mx-1">a</span>
          <input id="input-max-odds" type="number" step="0.01" value="1.05" class="w-14 bg-transparent text-sm font-bold text-emerald-400 focus:outline-none" onchange="updateConfig()">
        </div>
        <button onclick="toggleScanner()" id="btn-toggle" class="px-3 py-1.5 text-xs font-semibold bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg transition">Pausar Bot</button>
        <button onclick="simulateBet()" class="px-3 py-1.5 text-xs font-semibold bg-indigo-500/20 hover:bg-indigo-500/30 text-indigo-300 border border-indigo-500/30 rounded-lg transition">Probar Entrada (15s)</button>
        <a href="/api/export_ai_csv" class="px-3 py-1.5 text-xs font-semibold bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-300 border border-emerald-500/30 rounded-lg transition">⬇️ Exportar CSV IA</a>
        <a href="/api/export_ai_json" target="_blank" class="px-3 py-1.5 text-xs font-semibold bg-cyan-500/20 hover:bg-cyan-500/30 text-cyan-300 border border-cyan-500/30 rounded-lg transition">⬇️ JSON IA</a>
        <button onclick="resetDemo()" class="px-3 py-1.5 text-xs font-semibold bg-rose-500/20 hover:bg-rose-500/30 text-rose-300 border border-rose-500/30 rounded-lg transition">Reset Banca</button>
      </div>
    </div>

    <!-- KPI Cards: Banca + Dataset IA -->
    <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-3">
      <div class="bg-slate-900 border border-slate-800 p-3.5 rounded-xl">
        <span class="text-[11px] text-slate-400">Equity Total (Banca)</span>
        <p id="kpi-equity" class="text-lg font-bold mt-1 text-white">30,000.00 USDT</p>
      </div>
      <div class="bg-slate-900 border border-slate-800 p-3.5 rounded-xl">
        <span class="text-[11px] text-slate-400">Disponible / En Juego</span>
        <p id="kpi-avail" class="text-xs font-mono font-semibold mt-2 text-slate-300">30,000.00 / 0.00</p>
      </div>
      <div class="bg-slate-900 border border-slate-800 p-3.5 rounded-xl">
        <span class="text-[11px] text-slate-400">Ganancia Neta (PnL)</span>
        <p id="kpi-pnl" class="text-lg font-bold mt-1 text-emerald-400">+0.00 USDT</p>
      </div>
      <div class="bg-slate-900 border border-slate-800 p-3.5 rounded-xl">
        <span class="text-[11px] text-slate-400">Win Rate Bot (G/P/Curso)</span>
        <p id="kpi-winrate" class="text-sm font-bold mt-1.5 text-white">100.0% <span id="kpi-record" class="text-xs text-slate-400 font-mono">(0/0/0)</span></p>
      </div>

      <!-- KPIs IA Data Lake -->
      <div class="bg-indigo-950/30 border border-indigo-500/30 p-3.5 rounded-xl">
        <span class="text-[11px] text-indigo-300">Dataset IA Total Capturado</span>
        <p id="kpi-ai-total" class="text-lg font-bold mt-1 text-indigo-200">0 muestras</p>
      </div>
      <div class="bg-indigo-950/30 border border-indigo-500/30 p-3.5 rounded-xl">
        <span class="text-[11px] text-indigo-300">Entradas vs No Entradas</span>
        <p id="kpi-ai-split" class="text-sm font-mono font-bold mt-1.5 text-slate-200">0 Ent. / 0 Sombra</p>
      </div>
      <div class="bg-indigo-950/30 border border-indigo-500/30 p-3.5 rounded-xl">
        <span class="text-[11px] text-indigo-300">Muestras IA Ya Liquidadas</span>
        <p id="kpi-ai-settled" class="text-lg font-bold mt-1 text-cyan-300">0 resueltas</p>
      </div>
      <div class="bg-indigo-950/30 border border-indigo-500/30 p-3.5 rounded-xl">
        <span class="text-[11px] text-indigo-300">Derrotas Evitadas / Sombra G</span>
        <p id="kpi-ai-saved" class="text-sm font-mono font-bold mt-1.5 text-emerald-300">0 Evitadas / 0 G</p>
      </div>
    </div>

    <!-- Main Grid: Bets & Live Radar -->
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-5">
      
      <!-- Historial de Operaciones Ejecutadas -->
      <div class="lg:col-span-7 bg-slate-900 border border-slate-800 rounded-xl overflow-hidden flex flex-col">
        <div class="p-3.5 border-b border-slate-800 flex justify-between items-center">
          <h2 class="text-sm font-semibold text-slate-200">1. Operaciones Ejecutadas en Banca (30,000 USDT)</h2>
          <span class="text-xs text-emerald-400 font-mono">⏱️ Tiempo restante en vivo segundo a segundo</span>
        </div>
        <div class="overflow-x-auto max-h-[440px]">
          <table class="w-full text-left text-xs text-slate-300">
            <thead class="bg-slate-950 text-slate-400 uppercase text-[10px] sticky top-0">
              <tr>
                <th class="p-2.5">Deporte / Partido</th>
                <th class="p-2.5">Mercado &amp; Selección</th>
                <th class="p-2.5">Tiempo Restante (En Vivo)</th>
                <th class="p-2.5">Cuota / IA</th>
                <th class="p-2.5">Stake</th>
                <th class="p-2.5">Marcador</th>
                <th class="p-2.5">Estado</th>
                <th class="p-2.5">PnL</th>
              </tr>
            </thead>
            <tbody id="bets-tbody" class="divide-y divide-slate-800/60 font-mono">
              <tr><td colspan="8" class="p-8 text-center text-slate-500 font-sans">Escaneando los 8 deportes en busca de bloqueos matemáticos con cuota segura...</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Radar en Vivo Multi-Deporte -->
      <div class="lg:col-span-5 bg-slate-900 border border-slate-800 rounded-xl overflow-hidden flex flex-col">
        <div class="p-3.5 border-b border-slate-800 space-y-2">
          <div class="flex justify-between items-center">
            <h2 class="text-sm font-semibold text-emerald-400">2. Radar Multi-Deporte en Vivo (Stake)</h2>
            <span id="scan-time" class="text-[11px] text-slate-400">--:--:--</span>
          </div>
          <div id="sport-filters" class="flex flex-wrap gap-1.5 text-[11px]">
            <button onclick="setSportFilter('ALL')" class="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 font-semibold">Todos</button>
          </div>
        </div>
        <div class="overflow-y-auto max-h-[395px] divide-y divide-slate-800/60" id="radar-list">
          <div class="p-6 text-center text-xs text-slate-500">Cargando partidos en vivo...</div>
        </div>
      </div>

    </div>

    <!-- Sección 3: Laboratorio IA & Dataset Completo (Entradas + No Entradas) -->
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-5">
      
      <!-- Tabla de Todas las Observaciones Capturadas (Entradas y No Entradas) -->
      <div class="lg:col-span-8 bg-slate-900 border border-indigo-500/30 rounded-xl overflow-hidden">
        <div class="p-3.5 border-b border-slate-800 flex flex-wrap justify-between items-center gap-2">
          <div>
            <h2 class="text-sm font-semibold text-indigo-300">3. Base de Datos de Entrenamiento IA (Apuestas Entradas + No Entradas y su Resultado Final)</h2>
            <p class="text-[11px] text-slate-400">Guarda cada mercado candidato (hasta cuota 1.45), todos sus indicadores (tiempo restante, diferencia, overround, sets/games, JSON de mercados) y liquida su resultado al terminar el partido.</p>
          </div>
          <div class="flex flex-wrap gap-1.5 text-[11px]">
            <button onclick="setObsFilter('ALL')" id="obs-btn-ALL" class="px-2.5 py-1 rounded bg-indigo-500/20 text-indigo-300 border border-indigo-500/30 font-semibold">Todas</button>
            <button onclick="setObsFilter('SHADOW')" id="obs-btn-SHADOW" class="px-2.5 py-1 rounded bg-slate-800 text-slate-400 hover:text-slate-200">Solo No Entradas (Sombra)</button>
            <button onclick="setObsFilter('SETTLED')" id="obs-btn-SETTLED" class="px-2.5 py-1 rounded bg-slate-800 text-slate-400 hover:text-slate-200">Ya Liquidadas (Con Resultado)</button>
            <button onclick="setObsFilter('SAVED')" id="obs-btn-SAVED" class="px-2.5 py-1 rounded bg-slate-800 text-slate-400 hover:text-slate-200">Descartadas que Perdieron</button>
          </div>
        </div>
        <div class="overflow-x-auto max-h-[420px]">
          <table class="w-full text-left text-xs text-slate-300">
            <thead class="bg-slate-950 text-slate-400 uppercase text-[10px] sticky top-0">
              <tr>
                <th class="p-2.5">Deporte / Partido</th>
                <th class="p-2.5">Tiempo Restante &amp; Marcador</th>
                <th class="p-2.5">Mercado &amp; Selección</th>
                <th class="p-2.5">Cuota / Prob. / IA</th>
                <th class="p-2.5">Decisión del Bot &amp; Motivo</th>
                <th class="p-2.5">Resultado Real</th>
                <th class="p-2.5">ROI Hipotético</th>
              </tr>
            </thead>
            <tbody id="obs-tbody" class="divide-y divide-slate-800/60 font-mono">
              <tr><td colspan="7" class="p-6 text-center text-slate-500 font-sans">Recolectando telemetría de todos los partidos en vivo...</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Panel Derecho: Matriz de Efectividad Aprendida por Deporte & Mercado -->
      <div class="lg:col-span-4 bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">
        <div class="p-3.5 border-b border-slate-800">
          <h2 class="text-sm font-semibold text-cyan-400">4. Matriz de Aprendizaje IA por Mercado</h2>
          <p class="text-[11px] text-slate-400">Efectividad empírica real (Entradas + Sombra) acumulada en SQLite</p>
        </div>
        <div class="overflow-x-auto max-h-[420px]">
          <table class="w-full text-left text-xs text-slate-300">
            <thead class="bg-slate-950 text-slate-400 uppercase text-[10px] sticky top-0">
              <tr>
                <th class="p-2.5">Deporte / Mercado</th>
                <th class="p-2.5">Muestras</th>
                <th class="p-2.5">G / P</th>
                <th class="p-2.5">Win Rate</th>
                <th class="p-2.5">Cuota M.</th>
              </tr>
            </thead>
            <tbody id="insights-tbody" class="divide-y divide-slate-800/60 font-mono">
              <tr><td colspan="5" class="p-6 text-center text-slate-500 font-sans">Calculando estadísticas...</td></tr>
            </tbody>
          </table>
        </div>
      </div>

    </div>

  </div>

  <script>
    let currentSportFilter = 'ALL';
    let currentObsFilter = 'ALL';
    let lastRadarData = [];
    let lastObsData = [];
    let lastBetsData = [];
    let configInitialized = false;
    let pollTimestampMs = Date.now();

    const fmt = (n) => Number(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

    function fmtMMSS(sec) {
      const s = Math.max(0, Math.floor(sec));
      const mm = String(Math.floor(s / 60)).padStart(2, '0');
      const ss = String(s % 60).padStart(2, '0');
      return `${mm}:${ss}`;
    }

    function computeLiveRemainingBadge(item, isPending) {
      if (!isPending && item.status && item.status !== 'PENDING') {
        return `<span class="px-2 py-0.5 rounded bg-slate-800 text-slate-400 text-[10px]">🏁 Finalizado</span>`;
      }
      const deltaSec = Math.floor((Date.now() - pollTimestampMs) / 1000);
      // Caso 1: Partido en Tiempo Añadido / Descuento (codificado como <= -10000 para contar hacia arriba en vivo)
      if (item.rem_seconds !== undefined && item.rem_seconds !== null && item.rem_seconds <= -10000) {
        const extraSec = Math.abs(item.rem_seconds + 10000) + deltaSec;
        return `<span class="px-2 py-0.5 rounded font-bold text-[11px] bg-amber-500/20 text-amber-300 border border-amber-500/40 animate-pulse">🔥 Descuento 90' +${fmtMMSS(extraSec)}</span>`;
      }
      // Caso 2: Cuenta regresiva normal (> 0), y si llega a 0 pasa automáticamente a contar el descuento (+MM:SS)
      if (item.rem_seconds !== undefined && item.rem_seconds !== null && item.rem_seconds >= 0) {
        const rawRem = item.rem_seconds - deltaSec;
        if (rawRem > 0) {
          const urgentClass = rawRem <= 180 ? 'bg-emerald-500/25 text-emerald-300 border border-emerald-500/40 animate-pulse' : 'bg-cyan-500/15 text-cyan-300 border border-cyan-500/30';
          return `<span class="px-2 py-0.5 rounded font-bold text-[11px] ${urgentClass}">⏱️ Restan ${fmtMMSS(rawRem)}</span>`;
        } else {
          const extraSec = Math.abs(rawRem);
          return `<span class="px-2 py-0.5 rounded font-bold text-[11px] bg-amber-500/20 text-amber-300 border border-amber-500/40 animate-pulse">🔥 Tiempo Añadido (+${fmtMMSS(extraSec)})</span>`;
        }
      }
      if (item.remaining_text) {
        return `<span class="px-2 py-0.5 rounded bg-slate-800 text-amber-300 text-[10px] font-semibold">${item.remaining_text}</span>`;
      }
      return `<span class="text-slate-400 text-[11px]">${item.live_clock || item.minute || 'En vivo'}</span>`;
    }

    function setSportFilter(sp) {
      currentSportFilter = sp;
      renderRadar(lastRadarData);
    }

    function setObsFilter(flt) {
      currentObsFilter = flt;
      ['ALL', 'SHADOW', 'SETTLED', 'SAVED'].forEach(k => {
        const el = document.getElementById('obs-btn-' + k);
        if (!el) return;
        if (k === flt) {
          el.className = 'px-2.5 py-1 rounded bg-indigo-500/20 text-indigo-300 border border-indigo-500/30 font-semibold';
        } else {
          el.className = 'px-2.5 py-1 rounded bg-slate-800 text-slate-400 hover:text-slate-200';
        }
      });
      renderObservations(lastObsData);
    }

    function renderSportPills(counts) {
      const container = document.getElementById('sport-filters');
      const entries = Object.entries(counts || {});
      const total = entries.reduce((acc, [, c]) => acc + c, 0);
      let html = `<button onclick="setSportFilter('ALL')" class="px-2 py-0.5 rounded transition ${currentSportFilter === 'ALL' ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 font-semibold' : 'bg-slate-800 text-slate-400 hover:text-slate-200'}">Todos (${total})</button>`;
      for (const [label, count] of entries) {
        if (count === 0) continue;
        const active = currentSportFilter === label;
        html += `<button onclick="setSportFilter('${label}')" class="px-2 py-0.5 rounded transition ${active ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 font-semibold' : 'bg-slate-800 text-slate-400 hover:text-slate-200'}">${label} (${count})</button>`;
      }
      container.innerHTML = html;
    }

    function renderBets(bets) {
      const tbody = document.getElementById('bets-tbody');
      if (!bets || bets.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="p-8 text-center text-slate-500 font-sans">Escaneando los 8 deportes en busca de bloqueos matemáticos con cuota segura...</td></tr>';
        return;
      }
      tbody.innerHTML = bets.map(b => {
        let badgeHtml = '<span class="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300">PENDING</span>';
        let pnlClass = 'text-slate-400';
        if (b.status === 'WON') {
          badgeHtml = '<span class="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300">WON</span>';
          pnlClass = 'text-emerald-400';
        } else if (b.status === 'PUSH') {
          badgeHtml = '<span class="px-2 py-0.5 rounded bg-slate-700 text-slate-300">REEMBOLSO</span>';
          pnlClass = 'text-slate-300';
        } else if (b.status === 'LOST') {
          badgeHtml = '<span class="px-2 py-0.5 rounded bg-rose-500/20 text-rose-300">LOST</span>';
          pnlClass = 'text-rose-400';
        }
        const liveRemHtml = computeLiveRemainingBadge(b, b.status === 'PENDING');
        return `
          <tr class="hover:bg-slate-800/40">
            <td class="p-2.5 font-sans">
              <div class="flex items-center gap-1.5">
                <span class="text-[10px] px-1.5 py-0.2 rounded bg-slate-800 text-slate-300">${b.sport || '⚽'}</span>
                <span class="font-medium text-slate-200">${b.match}</span>
              </div>
              <div class="text-[10px] text-slate-500 mt-0.5">${b.league}</div>
            </td>
            <td class="p-2.5 font-sans">
              <div class="text-indigo-300 font-semibold">${b.selection}</div>
              <div class="text-[10px] text-emerald-400/80">${b.market_name || 'Ganador'}</div>
              <div class="text-[10px] text-slate-500">${b.safety_reason || ''}</div>
            </td>
            <td class="p-2.5">
              <div>${liveRemHtml}</div>
              <div class="text-[10px] text-slate-500 mt-1">Entrada: ${b.minute} (${b.score_at_bet})</div>
            </td>
            <td class="p-2.5">
              <div class="text-amber-400 font-bold">@${Number(b.odds).toFixed(2)}</div>
              <div class="text-[10px] text-cyan-400">IA: ${Number(b.ai_confidence || 98).toFixed(1)}%</div>
            </td>
            <td class="p-2.5">${fmt(b.stake)}</td>
            <td class="p-2.5 text-[11px] font-bold text-slate-200">${b.final_score || 'En juego'}</td>
            <td class="p-2.5">${badgeHtml}</td>
            <td class="p-2.5 font-bold ${pnlClass}">${b.pnl > 0 ? '+' : ''}${fmt(b.pnl)}</td>
          </tr>
        `;
      }).join('');
    }

    function renderRadar(radar) {
      const radarEl = document.getElementById('radar-list');
      const filtered = currentSportFilter === 'ALL' ? radar : radar.filter(r => r.sport === currentSportFilter);
      if (filtered.length === 0) {
        radarEl.innerHTML = '<div class="p-6 text-center text-xs text-slate-500">No hay partidos en vivo para este filtro en este instante.</div>';
        return;
      }
      radarEl.innerHTML = filtered.map(r => {
        const isReady = r.ready;
        const rowBg = isReady ? 'bg-emerald-950/30 border-l-2 border-l-emerald-400' : 'hover:bg-slate-800/30';
        const oddsBadge = r.leader_odds
          ? `<span class="px-1.5 py-0.5 rounded font-bold ${isReady ? 'bg-emerald-500/20 text-emerald-300' : 'bg-amber-500/10 text-amber-400'}">@${r.leader_odds.toFixed(2)}</span>`
          : `<span class="text-slate-600">Sin cuota</span>`;
        const aiBadge = r.ai_confidence
          ? `<span class="text-[10px] text-cyan-400 font-mono">IA:${r.ai_confidence.toFixed(1)}%</span>`
          : '';
        const remBadge = computeLiveRemainingBadge(r, true);
        return `
          <div class="p-3 ${rowBg} flex justify-between items-start gap-3 text-xs">
            <div class="min-w-0 flex-1">
              <div class="flex items-center gap-1.5">
                <span class="px-1.5 py-0.2 rounded bg-slate-800 text-[10px] text-slate-300">${r.sport}</span>
                <span class="font-semibold text-slate-200 truncate">${r.match}</span>
              </div>
              <div class="text-[10px] text-slate-500 truncate mt-0.5">${r.league}</div>
              <div class="text-[11px] ${isReady ? 'text-emerald-400 font-semibold' : 'text-slate-400'} mt-1">${r.status_note}</div>
            </div>
            <div class="text-right font-mono shrink-0 space-y-1">
              <div><span class="px-2 py-0.5 rounded bg-slate-800 text-emerald-400 font-bold">${r.score}</span></div>
              <div>${remBadge}</div>
              <div class="flex items-center justify-end gap-1.5">
                ${aiBadge}
                <span class="text-[10px] text-indigo-300 font-sans truncate max-w-[100px]">${r.market_name}</span>
                ${oddsBadge}
              </div>
            </div>
          </div>
        `;
      }).join('');
    }

    function renderObservations(obsList) {
      const tbody = document.getElementById('obs-tbody');
      let filtered = obsList || [];
      if (currentObsFilter === 'SHADOW') {
        filtered = filtered.filter(o => o.was_entered === 0);
      } else if (currentObsFilter === 'SETTLED') {
        filtered = filtered.filter(o => o.status !== 'PENDING');
      } else if (currentObsFilter === 'SAVED') {
        filtered = filtered.filter(o => o.was_entered === 0 && o.status === 'LOST');
      }

      if (filtered.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="p-6 text-center text-slate-500 font-sans">No hay registros para este filtro aún.</td></tr>';
        return;
      }

      tbody.innerHTML = filtered.map(o => {
        const decisionBadge = o.was_entered === 1
          ? '<span class="px-1.5 py-0.5 rounded bg-emerald-500/20 text-emerald-300 text-[10px] font-bold">ENTRADA</span>'
          : '<span class="px-1.5 py-0.5 rounded bg-slate-800 text-slate-300 text-[10px]">NO ENTRADA (SOMBRA)</span>';

        let resBadge = '<span class="px-2 py-0.5 rounded bg-amber-500/15 text-amber-300 text-[10px]">EN JUEGO</span>';
        let roiHtml = '<span class="text-slate-500">--</span>';
        if (o.status === 'WON') {
          resBadge = `<span class="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300 font-bold">GANÓ (${o.final_score})</span>`;
          roiHtml = `<span class="text-emerald-400 font-bold">+${((o.hypothetical_roi || 0) * 100).toFixed(2)}%</span>`;
        } else if (o.status === 'LOST') {
          resBadge = `<span class="px-2 py-0.5 rounded bg-rose-500/20 text-rose-300 font-bold">PERDIÓ (${o.final_score})</span>`;
          roiHtml = `<span class="text-rose-400 font-bold">-100.00%</span>`;
        } else if (o.status === 'PUSH') {
          resBadge = `<span class="px-2 py-0.5 rounded bg-slate-700 text-slate-200">REEMBOLSO (${o.final_score})</span>`;
          roiHtml = `<span class="text-slate-300">0.00%</span>`;
        }
        const remBadge = computeLiveRemainingBadge(o, o.status === 'PENDING');

        return `
          <tr class="hover:bg-slate-800/40">
            <td class="p-2.5 font-sans">
              <div class="flex items-center gap-1">
                <span class="text-[10px] px-1 rounded bg-slate-800 text-slate-300">${o.sport}</span>
                <span class="font-medium text-slate-200">${o.match}</span>
              </div>
              <div class="text-[10px] text-slate-500">${o.league}</div>
            </td>
            <td class="p-2.5 text-[11px]">
              <div>${remBadge}</div>
              <div class="text-[10px] text-slate-400 mt-1">Al capturar: ${o.clock_display} (${o.home_score}-${o.away_score})</div>
            </td>
            <td class="p-2.5 font-sans">
              <div class="text-indigo-300 font-semibold">${o.selection}</div>
              <div class="text-[10px] text-slate-400">${o.market_name}</div>
            </td>
            <td class="p-2.5">
              <div class="text-amber-400 font-bold">@${Number(o.odds).toFixed(2)} <span class="text-[10px] text-slate-400">(${(Number(o.implied_prob)*100).toFixed(1)}%)</span></div>
              <div class="text-[10px] text-cyan-400">Conf. IA: ${Number(o.ai_confidence).toFixed(1)}% | Margen: ${(Number(o.market_overround)*100).toFixed(1)}%</div>
            </td>
            <td class="p-2.5 font-sans max-w-[260px]">
              <div>${decisionBadge} <span class="text-[10px] font-mono text-slate-400">${o.reject_code}</span></div>
              <div class="text-[10px] text-slate-400 mt-0.5 truncate" title="${o.reject_reason}">${o.reject_reason}</div>
            </td>
            <td class="p-2.5 text-[11px]">${resBadge}</td>
            <td class="p-2.5">${roiHtml}</td>
          </tr>
        `;
      }).join('');
    }

    function renderInsights(insights) {
      const tbody = document.getElementById('insights-tbody');
      if (!insights || insights.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="p-6 text-center text-slate-500 font-sans">Sin datos aún.</td></tr>';
        return;
      }
      tbody.innerHTML = insights.map(i => {
        const wrText = i.win_rate !== null ? `${i.win_rate.toFixed(1)}%` : 'En curso';
        const wrColor = i.win_rate === null ? 'text-slate-400' : (i.win_rate >= 97 ? 'text-emerald-400 font-bold' : 'text-amber-400');
        return `
          <tr class="hover:bg-slate-800/40">
            <td class="p-2.5 font-sans">
              <div class="text-slate-200 font-medium">${i.sport}</div>
              <div class="text-[10px] text-indigo-300">${i.market_name}</div>
            </td>
            <td class="p-2.5">${i.total_obs} <span class="text-[10px] text-slate-500">(${i.settled} liq)</span></td>
            <td class="p-2.5 text-slate-300">${i.wins}G / ${i.losses}P</td>
            <td class="p-2.5 ${wrColor}">${wrText}</td>
            <td class="p-2.5 text-amber-400">@${i.avg_odds.toFixed(2)}</td>
          </tr>
        `;
      }).join('');
    }

    async function refresh() {
      try {
        const res = await fetch('/api/dashboard');
        const data = await res.json();
        pollTimestampMs = Date.now();
        const s = data.stats;
        const ai = s.ai_stats || {};

        if (!configInitialized) {
          document.getElementById('input-stake').value = s.stake_amount;
          document.getElementById('input-min-odds').value = s.min_odds;
          document.getElementById('input-max-odds').value = s.max_odds;
          configInitialized = true;
        }

        document.getElementById('kpi-equity').textContent = `${fmt(s.equity)} USDT`;
        document.getElementById('kpi-avail').textContent = `${fmt(s.available)} / ${fmt(s.locked)}`;
        
        const pnlEl = document.getElementById('kpi-pnl');
        pnlEl.textContent = `${s.net_pnl >= 0 ? '+' : ''}${fmt(s.net_pnl)} USDT`;
        pnlEl.className = `text-lg font-bold mt-1 ${s.net_pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;

        document.getElementById('kpi-winrate').innerHTML = `${s.win_rate.toFixed(2)}% <span class="text-xs text-slate-400 font-mono">(${s.won}/${s.lost}/${s.pending})</span>`;
        document.getElementById('conn-info').textContent = s.connection_status;
        document.getElementById('scan-time').textContent = `Escaneo: ${s.last_scan_time}`;

        document.getElementById('kpi-ai-total').textContent = `${ai.total_captured || 0} muestras`;
        document.getElementById('kpi-ai-split').textContent = `${ai.entered_count || 0} Ent. / ${ai.shadow_count || 0} Sombra`;
        document.getElementById('kpi-ai-settled').textContent = `${ai.settled_count || 0} resueltas (${ai.pending_count || 0} vivas)`;
        document.getElementById('kpi-ai-saved').textContent = `${ai.saved_losses || 0} Evitadas / ${ai.shadow_wins || 0} G`;

        const badge = document.getElementById('badge-status');
        const btnToggle = document.getElementById('btn-toggle');
        if (s.is_running) {
          badge.textContent = 'ACTIVO';
          badge.className = 'px-2.5 py-0.5 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20';
          btnToggle.textContent = 'Pausar Bot';
        } else {
          badge.textContent = 'PAUSADO';
          badge.className = 'px-2.5 py-0.5 rounded-full text-xs font-semibold bg-amber-500/10 text-amber-400 border border-amber-500/20';
          btnToggle.textContent = 'Reanudar Bot';
        }

        lastBetsData = data.bets || [];
        lastRadarData = data.radar || [];
        lastObsData = data.ai_observations || [];

        renderBets(lastBetsData);
        renderSportPills(s.sport_counts);
        renderRadar(lastRadarData);
        renderObservations(lastObsData);
        renderInsights(data.ai_insights || []);
      } catch (e) {
        console.error(e);
      }
    }

    // Reloj en vivo segundo a segundo para actualizar la cuenta regresiva sin esperar al poll
    setInterval(() => {
      if (lastBetsData.length > 0) renderBets(lastBetsData);
      if (lastRadarData.length > 0) renderRadar(lastRadarData);
    }, 1000);

    async function toggleScanner() {
      await fetch('/api/toggle', { method: 'POST' });
      refresh();
    }

    async function updateConfig() {
      const stake = parseFloat(document.getElementById('input-stake').value);
      const minOdds = parseFloat(document.getElementById('input-min-odds').value);
      const maxOdds = parseFloat(document.getElementById('input-max-odds').value);
      if (!isNaN(stake)) {
        await fetch('/api/stake', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            stake_amount: stake,
            min_odds: isNaN(minOdds) ? 1.01 : minOdds,
            max_odds: isNaN(maxOdds) ? 1.05 : maxOdds
          })
        });
        refresh();
      }
    }

    async function simulateBet() {
      await fetch('/api/simulate_test', { method: 'POST' });
      refresh();
    }

    async function resetDemo() {
      if (!confirm('¿Reiniciar balance de la banca a 30,000.00 USDT? (El Dataset histórico de la IA se conservará intacto)')) return;
      await fetch('/api/reset', { method: 'POST' });
      location.reload();
    }

    setInterval(refresh, 3000);
    refresh();
  </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def serve_ui():
    return HTML_PAGE

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
