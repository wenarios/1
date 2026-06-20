import asyncio
import inspect
import json
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import websockets


PROJECT_ROOT = Path("/root/saba_value_bot")
SESSION_PATH = PROJECT_ROOT / "configs/sessions/saba_current_session.secret.json"
OUT_DIR = PROJECT_ROOT / "data/instances/acc1/normalized"
RAW_DIR = PROJECT_ROOT / "data/instances/acc1/raw"
STATUS_PATH = PROJECT_ROOT / "data/instances/acc1/status/saba_socket_collector_status.json"
WATCHDOG_CONFIG_PATH = PROJECT_ROOT / "configs/saba_socket_watchdog.json"

COLLECT_SECONDS = int(os.getenv("SABA_COLLECT_SECONDS", "60"))

WATCHDOG_DEFAULTS = {
    "socket_watchdog_enabled": True,
    "socket_receive_idle_timeout_sec": 150,
    "socket_reconnect_backoff_sec": 10,
    "socket_max_reconnects_per_cycle": 4,
    "socket_status_path": "data/instances/acc1/status/saba_socket_collector_status.json",
}


class SocketIdleTimeout(Exception):
    pass


def load_watchdog_config():
    cfg = dict(WATCHDOG_DEFAULTS)
    try:
        loaded = json.loads(WATCHDOG_CONFIG_PATH.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            cfg.update(loaded)
    except Exception:
        pass
    return cfg

FOOTBALL_BETTYPES = [
    1, 2, 3, 5, 7, 8, 15, 22, 301, 302, 303, 304, 394, 396, 400, 470, 471, 461, 462, 24, 448, 393, 390, 381, 382, 482, 483, 413, 6, 159, 406, 467, 469, 1206,
]

SUPPORTED_BETTYPES_INITIAL = {1, 3, 5, 7, 8, 13, 20, 24, 25, 28, 171, 461, 462}
GOAL_BAND_CANDIDATE_BETTYPES = {6, 159, 406, 467, 469, 1206}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def hour_path(prefix, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    return out_dir / f"{prefix}_{stamp}.jsonl"


def write_jsonl(path, obj):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")


def write_status(obj):
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(STATUS_PATH)


def redact_secret_only(text: str) -> str:
    if text is None:
        return ""

    text = str(text)
    text = re.sub(r'("token"\s*:\s*")[^"]+', r'\1[REDACTED]', text, flags=re.I)
    text = re.sub(r'token=[^&\s]+', 'token=[REDACTED]', text, flags=re.I)

    return text


def session_expired(session):
    safe = session.get("jwt_payload_safe") or {}
    exp = safe.get("exp")

    if not exp:
        return True, "missing exp"

    left = int(exp) - int(time.time())

    if left <= 0:
        return True, f"expired {abs(left)} sec ago"

    return False, f"{left} sec left"


def sio_packet(event_name, payload):
    return "42" + json.dumps([event_name, payload], ensure_ascii=False, separators=(",", ":"))


def parse_sio_packet(raw):
    if not isinstance(raw, str):
        return None

    if not raw.startswith("42"):
        return None

    try:
        arr = json.loads(raw[2:])
        if isinstance(arr, list) and arr:
            return arr
    except Exception:
        return None

    return None


def make_subscribe_payload():
    return [
        [
            "spread",
            [
                {
                    "id": "c0",
                    "rev": "",
                    "sorting": 0,
                    "condition": {},
                }
            ],
        ],
        [
            "odds",
            [
                {
                    "id": "c1",
                    "rev": "",
                    "sorting": 0,
                    "condition": {
                        "sporttype": 1,
                        "no_stream": True,
                        "source": "hotleaguewall",
                        "mini": 1,
                        "bettype": [1, 3],
                    },
                },
                {
                    "id": "c2",
                    "rev": "",
                    "sorting": "n",
                    "condition": {
                        "sporttype": 1,
                        "marketid": "L",
                        "no_stream": True,
                        "bettype": FOOTBALL_BETTYPES,
                        "source": None,
                    },
                },
                {
                    "id": "c3",
                    "rev": "",
                    "sorting": "n",
                    "condition": {
                        "sporttype": 1,
                        "marketid": "T",
                        "no_stream": True,
                        "bettype": FOOTBALL_BETTYPES,
                        "source": None,
                    },
                },
            ],
        ],
    ]


def decode_pairs(row, field_map):
    out = {}
    i = 0

    while i + 1 < len(row):
        idx = row[i]
        val = row[i + 1]
        name = field_map.get(idx, f"field_{idx}")
        out[name] = val
        i += 2

    return out


def asian_malay_to_decimal(x):
    if x is None:
        return None

    try:
        x = float(x)
    except Exception:
        return None

    if x == 0:
        return None

    if x > 0:
        return round(1.0 + x, 6)

    return round(1.0 + (1.0 / abs(x)), 6)


BAD_NAME_PATTERNS = [
    " + ",
    "(V)",
    "(PG)",
    "Corner",
    "Corners",
    "Offside",
    "Offsides",
    "Booking",
    "Bookings",
    "Card",
    "Cards",
    "Throw In",
    "Throw-in",
    "Penalty",
    "Free Kick",
    "Goal Kick",
    "No.of",
    "No. of",
    "1st Corner",
    "2nd Half 1st Corner",
    "00:00-15:00",
    "15:01-30:00",
    "30:01-45:00",
    "45:01-60:00",
    "60:01-75:00",
    "75:01-90:00",
]


def match_filter_reason(match):
    if not match:
        return "missing_match"

    if match.get("sporttype") != 1:
        return "not_football"

    if match.get("eventstatus") not in ("running", "live"):
        return "not_running_event"

    names = " ".join(
        str(match.get(k) or "")
        for k in [
            "hteamnameen",
            "ateamnameen",
            "hteamnamevn",
            "ateamnamevn",
            "matchcode",
        ]
    )

    for p in BAD_NAME_PATTERNS:
        if p.lower() in names.lower():
            return f"bad_name:{p}"

    if match.get("ismainmarket") not in (1, True):
        return "not_main_market"

    return None



def decimal_passthrough(x):
    """For SABA combo markets like Double Chance, com1/comx/com2 are already decimal odds."""
    try:
        if x is None or x == "" or x == 0 or x == "0":
            return None
        v = float(x)
        if v <= 0:
            return None
        return round(v, 6)
    except Exception:
        return None



def is_goal_band_audit_candidate(bettype, row, match):
    if bettype not in GOAL_BAND_CANDIDATE_BETTYPES:
        return False

    texts = []
    if isinstance(row, dict):
        texts.extend(str(v) for v in row.values() if isinstance(v, (str, int, float)))
    if isinstance(match, dict):
        for k, v in match.items():
            if isinstance(v, (str, int, float)):
                texts.append(str(v))

    blob = " ".join(texts).lower()

    # Важно: 470/471 сейчас часто прилетает на child-markets по угловым.
    # Такие рынки не маппим как голы.
    bad_markers = [
        "corner", "corners", "no.of corners", "phạt góc",
        "booking", "card", "yellow", "red card",
        "goal kick", "offside", "throw in", "free kick",
        "penalty", "shot",
        "(pg)", "(v)", " virtual", "virtual ",
        "kèo chấp", "handicap - score box", "cách biệt", "thắng với", "thua ",
    ]

    if any(x in blob for x in bad_markers):
        return False

    return True


def normalize_market_type(bettype):
    if bettype in (1, 7):
        return "handicap"
    if bettype in (3, 8):
        return "total"
    if bettype == 5:
        # M88/SABA bettype 5 has only odds1a/odds2a in our feed, no draw.
        # Do not treat as football 1X2.
        return "unknown_2way"
    if bettype == 20:
        return "moneyline"
    if bettype == 24:
        return "double_chance"
    if bettype == 25:
        return "draw_no_bet"
    if bettype == 28:
        return "handicap_3way"
    if bettype == 13:
        return "clean_sheet"
    if bettype == 171:
        return "winning_margin"

    if bettype in (461, 462):
        return "team_total"
    return "unknown"


def normalize_odds_row(row, match):
    bettype = row.get("bettype")
    market_type = normalize_market_type(bettype)

    return {
        "source": "saba",
        "operator_id": "saba_vi_generic",
        "account_label": "acc1",

        "received_at": utc_now(),
        "source_time": None,

        "event_id": str(row.get("matchid")),
        "match_id": row.get("matchid"),
        "odds_id": row.get("oddsid"),

        "league_id": match.get("leagueid"),
        "market_id": match.get("marketid"),
        "sport_type": match.get("sporttype"),

        "home": match.get("hteamnameen"),
        "away": match.get("ateamnameen"),
        "home_vn": match.get("hteamnamevn"),
        "away_vn": match.get("ateamnamevn"),

        "score_home": match.get("livehomescore"),
        "score_away": match.get("liveawayscore"),
        "live_period": match.get("liveperiod"),
        "live_timer_raw": match.get("livetimer"),
        "event_status": match.get("eventstatus"),

        "market_type": market_type,
        "is_alt_line": bettype in (7, 8),
        "team_side": ("home" if bettype == 461 else "away" if bettype == 462 else None),
        "bettype": bettype,
        "parenttypeid": row.get("parenttypeid"),

        "odds_status": row.get("oddsstatus"),
        "is_suspended": row.get("oddsstatus") != "running",

        "hdp1": row.get("hdp1"),
        "hdp2": row.get("hdp2"),
        "oddsspreada": row.get("oddsspreada"),
        "raw_line": row.get("hdp1") if row.get("hdp1") is not None else row.get("oddsspreada"),

        "odds1a_raw": row.get("odds1a"),
        "odds2a_raw": row.get("odds2a"),
        "odds1a_decimal": asian_malay_to_decimal(row.get("odds1a")),
        "odds2a_decimal": asian_malay_to_decimal(row.get("odds2a")),

        "com1_raw": row.get("com1"),
        "comx_raw": row.get("comx"),
        "com2_raw": row.get("com2"),
        "com1_decimal": decimal_passthrough(row.get("com1")) if bettype == 24 else asian_malay_to_decimal(row.get("com1")),
        "comx_decimal": decimal_passthrough(row.get("comx")) if bettype == 24 else asian_malay_to_decimal(row.get("comx")),
        "com2_decimal": decimal_passthrough(row.get("com2")) if bettype == 24 else asian_malay_to_decimal(row.get("com2")),

        "min_odds_raw": row.get("minodds"),
        "max_stake_raw": row.get("maxbet"),

        "raw": row,
    }


def goal_decimal(x):
    try:
        if x is None:
            return None
        x = float(x)
    except Exception:
        return None

    # 0 = рынок недоступен / невозможен; отрицательные здесь не трогаем
    if x <= 1.0:
        return None

    return round(x, 6)


def parse_goal_label(label):
    """
    Универсальный parser:
    0-N  -> under N.5
    N+   -> over N-0.5
    A-B  -> range A..B
    N    -> exact N
    """
    if label is None:
        return None

    raw = str(label).strip()
    s = raw.lower().replace("goals", "").replace("goal", "").strip()
    s = s.replace("g", "", 1) if s.startswith("g") else s

    m = re.fullmatch(r"0\s*-\s*(\d+)", s)
    if m:
        n = int(m.group(1))
        return {
            "label": raw,
            "selection_type": "under",
            "range_min": 0,
            "range_max": n,
            "exact_goals": None,
            "equivalent_selection": "under",
            "effective_line": float(n) + 0.5,
        }

    m = re.fullmatch(r"(\d+)\s*\+", s)
    if m:
        n = int(m.group(1))
        return {
            "label": raw,
            "selection_type": "over",
            "range_min": n,
            "range_max": None,
            "exact_goals": None,
            "equivalent_selection": "over",
            "effective_line": float(n) - 0.5,
        }

    m = re.fullmatch(r"(\d+)\s*-\s*(\d+)", s)
    if m:
        a = int(m.group(1))
        b = int(m.group(2))
        if a == 0:
            return {
                "label": raw,
                "selection_type": "under",
                "range_min": 0,
                "range_max": b,
                "exact_goals": None,
                "equivalent_selection": "under",
                "effective_line": float(b) + 0.5,
            }
        return {
            "label": raw,
            "selection_type": "range",
            "range_min": a,
            "range_max": b,
            "exact_goals": None,
            "equivalent_selection": None,
            "effective_line": None,
        }

    m = re.fullmatch(r"(\d+)", s)
    if m:
        n = int(m.group(1))
        return {
            "label": raw,
            "selection_type": "exact",
            "range_min": n,
            "range_max": n,
            "exact_goals": n,
            "equivalent_selection": None,
            "effective_line": None,
        }

    return None


BETTYPE6_TOTAL_GOAL_FIELDS = {
    # Total Goal на M88/SABA
    # Если появятся 0-2 / 3+ в другой раскладке — parser ниже уже умеет,
    # останется только добавить field -> label.
    "cs00": "0-1",
    "cs01": "2-3",
    "cs10": "4-6",
    "cs11": "7+",
}

BETTYPE406_EXACT_TOTAL_FIELDS = {
    # Exact Total Goals, parenttypeid=159
    "cs01": "0",
    "cs02": "1",
    "cs10": "2",
    "cs04": "3",
    "cs03": "4",
    "cs20": "5",
    "cs30": "6+",
}


def make_goal_special_base(row, match):
    return {
        "source": "saba",
        "operator": "m88",
        "event_id": str(row.get("matchid")),
        "match_id": row.get("matchid"),
        "league_id": match.get("leagueid") if match else None,
        "league_name": match.get("leaguenameen") or match.get("leaguename") if match else None,
        "home": match.get("hteamnameen") if match else None,
        "away": match.get("ateamnameen") if match else None,
        "score_home": match.get("livehomescore") if match else None,
        "score_away": match.get("liveawayscore") if match else None,
        "period": "match",
        "scope": "full_match",
        "score_basis": "full_match",
        "source_time": None,
        "received_at": utc_now(),
        "latency_ms": None,
        "odds_format": "decimal",
        "is_suspended": row.get("oddsstatus") != "running",
        "oddsid": row.get("oddsid"),
        "bettype": row.get("bettype"),
        "parenttypeid": row.get("parenttypeid"),
        "max_stake_raw": row.get("maxbet"),
        "min_odds_raw": row.get("minodds"),
    }


def expand_goal_special_rows(row, match):
    """
    Возвращает несколько normalized rows из одного socket odds row:
    - bettype 6: Total Goal ranges: 0-1 / 2-3 / 4-6 / 7+
    - bettype 406: Exact Total Goals: 0 / 1 / 2 / 3 / 4 / 5 / 6+
    """
    if not match:
        return []

    try:
        reason = match_filter_reason(match)
        if reason:
            return []
    except Exception:
        return []

    bettype = row.get("bettype")
    if bettype == 6:
        field_map = BETTYPE6_TOTAL_GOAL_FIELDS
        market_type = "total_goal_range"
        market_name = "Total Goal"
    elif bettype == 406:
        field_map = BETTYPE406_EXACT_TOTAL_FIELDS
        market_type = "exact_total_goals"
        market_name = "Exact Total Goals"
    else:
        return []

    out = []
    base = make_goal_special_base(row, match)

    for field, label in field_map.items():
        odds = goal_decimal(row.get(field))
        if odds is None:
            continue

        parsed = parse_goal_label(label)
        if not parsed:
            continue

        selection_type = parsed["selection_type"]

        if selection_type == "under":
            selection = f'under_{parsed["effective_line"]}'
        elif selection_type == "over":
            selection = f'over_{parsed["effective_line"]}'
        elif selection_type == "range":
            selection = f'range_{parsed["range_min"]}_{parsed["range_max"]}'
        elif selection_type == "exact":
            selection = f'exact_{parsed["exact_goals"]}'
        else:
            selection = str(label)

        item = dict(base)
        item.update({
            "market_type": market_type,
            "market_name": market_name,
            "selection": selection,
            "selection_label": label,
            "selection_type": selection_type,

            "range_min": parsed["range_min"],
            "range_max": parsed["range_max"],
            "exact_goals": parsed["exact_goals"],

            # Для 0-N и N+ сразу даём эквивалент обычного тотала.
            # Например 0-2 -> under 2.5, 3+ -> over 2.5.
            "equivalent_market_type": "total" if parsed["equivalent_selection"] else None,
            "equivalent_selection": parsed["equivalent_selection"],
            "effective_line": parsed["effective_line"],

            "raw_line": label,
            "raw_odds": row.get(field),
            "odds_decimal": odds,
            "odds_field": field,

            "raw": row,
        })

        out.append(item)

    return out



async def main():
    global STATUS_PATH

    session = json.loads(SESSION_PATH.read_text(encoding="utf-8"))
    watchdog_cfg = load_watchdog_config()
    watchdog_enabled = bool(watchdog_cfg.get("socket_watchdog_enabled", True))
    receive_idle_timeout = max(30, int(watchdog_cfg.get("socket_receive_idle_timeout_sec", 150)))
    reconnect_backoff = max(1, int(watchdog_cfg.get("socket_reconnect_backoff_sec", 10)))
    max_reconnects = max(0, int(watchdog_cfg.get("socket_max_reconnects_per_cycle", 4)))
    status_cfg_path = Path(str(watchdog_cfg.get("socket_status_path") or WATCHDOG_DEFAULTS["socket_status_path"]))
    STATUS_PATH = status_cfg_path if status_cfg_path.is_absolute() else PROJECT_ROOT / status_cfg_path

    expired, reason = session_expired(session)

    print("============================================")
    print(" SABA Socket Collect Once")
    print("============================================")
    print(f"collect_seconds: {COLLECT_SECONDS}")
    print(f"watchdog_enabled: {watchdog_enabled}")
    print(f"receive_idle_timeout_sec: {receive_idle_timeout}")
    print(f"max_reconnects_per_cycle: {max_reconnects}")
    print(f"expired: {expired} ({reason})")

    if expired:
        print("NO_COLLECT: session expired")
        print("============================================")
        return

    token = session.get("token")
    gid = session.get("socket_gid")
    host = session.get("socket_host") or session.get("push_host")
    eio = session.get("socket_eio") or "3"
    ms2_id = session.get("ms2_id") or ""

    if not token or not gid or not host or not ms2_id:
        print("NO_COLLECT: missing token/gid/host/ms2_id")
        print("============================================")
        return

    socket_url = session.get("socket_url") or f"wss://{host}/socket.io/?gid={quote(gid)}&token={quote(token)}&id={quote(ms2_id)}&rid=jwt&EIO={eio}&transport=websocket"

    headers = {}

    if session.get("socket_origin"):
        headers["Origin"] = session["socket_origin"]
    elif session.get("origin"):
        headers["Origin"] = session["origin"]

    if session.get("socket_user_agent"):
        headers["User-Agent"] = session["socket_user_agent"]
    elif session.get("user_agent"):
        headers["User-Agent"] = session["user_agent"]

    if session.get("socket_accept_language"):
        headers["Accept-Language"] = session["socket_accept_language"]

    if session.get("socket_cache_control"):
        headers["Cache-Control"] = session["socket_cache_control"]

    if session.get("socket_pragma"):
        headers["Pragma"] = session["socket_pragma"]

    norm_path = hour_path("saba_live_odds", OUT_DIR)
    raw_path = hour_path("saba_raw_socket", RAW_DIR)

    print(f"norm_path: {norm_path}")
    print(f"raw_path: {raw_path}")

    connect_kwargs = {
        "ping_interval": None,
        "close_timeout": 5,
        "open_timeout": 25,
        "max_size": 32_000_000,
    }

    sig = inspect.signature(websockets.connect)

    if headers:
        if "extra_headers" in sig.parameters:
            connect_kwargs["extra_headers"] = headers
        elif "additional_headers" in sig.parameters:
            connect_kwargs["additional_headers"] = headers

    counters = Counter()
    filter_reasons = Counter()

    previous_reconnects = 0
    try:
        previous_reconnects = int(json.loads(STATUS_PATH.read_text(encoding="utf-8")).get("reconnect_count") or 0)
    except Exception:
        pass

    status = {
        "version": "saba_socket_collector_status_v1",
        "updated_at": utc_now(),
        "state": "starting",
        "connected": False,
        "subscribed": False,
        "session_expired": expired,
        "bootstrap_fields_present": bool(token and gid and host and ms2_id),
        "reconnect_count": 0,
        "lifetime_connection_count": previous_reconnects + 1,
        "connection_attempt_count": 0,
        "watchdog_trigger_count": 0,
        "watchdog_enabled": watchdog_enabled,
        "receive_idle_timeout_sec": receive_idle_timeout,
        "reconnect_backoff_sec": reconnect_backoff,
        "max_reconnects_per_cycle": max_reconnects,
        "current_connection_started_at": None,
        "received_message_count": 0,
        "parsed_market_count": 0,
        "written_raw_count": 0,
        "written_normalized_count": 0,
        "consecutive_receive_timeouts": 0,
        "last_socket_message_at": None,
        "last_application_frame_at": None,
        "last_engineio_heartbeat_at": None,
        "last_socket_write_at": None,
        "last_normalized_write_at": None,
        "no_data_reason": None,
    }

    def persist_status(**updates):
        status.update(updates)
        status["updated_at"] = utc_now()
        status["parsed_market_count"] = int(counters.get("odds_rows_total", 0))
        status["written_normalized_count"] = int(counters.get("normalized_written", 0) + counters.get("goal_special_normalized_written", 0))
        write_status(status)

    persist_status()

    cycle_started = time.monotonic()
    cycle_deadline = cycle_started + COLLECT_SECONDS
    matches = {}
    attempt = 0

    while time.monotonic() < cycle_deadline and attempt <= max_reconnects:
        if attempt > 0:
            status["reconnect_count"] = attempt
            persist_status(state="reconnect_backoff", connected=False)
            await asyncio.sleep(min(reconnect_backoff, max(0, cycle_deadline - time.monotonic())))

        attempt += 1
        status["connection_attempt_count"] = attempt
        status["current_connection_started_at"] = utc_now()
        status["consecutive_receive_timeouts"] = 0
        persist_status(state="connecting", connected=False, subscribed=False)

        field_maps_by_bid = defaultdict(dict)
        channel_by_bid = {}
        matches = {}
        sent_init = False
        sent_subscribe = False
        last_message_monotonic = time.monotonic()
        reconnect_needed = False

        try:
            async with websockets.connect(socket_url, **connect_kwargs) as ws:
                print(f"connected: yes attempt={attempt}")
                persist_status(state="connected", connected=True, no_data_reason=None)

                while time.monotonic() < cycle_deadline:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=min(12, receive_idle_timeout))
                    except asyncio.TimeoutError:
                        counters["timeout"] += 1
                        status["consecutive_receive_timeouts"] = int(status.get("consecutive_receive_timeouts", 0)) + 1
                        idle_for = int(time.monotonic() - last_message_monotonic)
                        persist_status(no_data_reason="socket_recv_timeout", socket_idle_sec=idle_for)
                        if watchdog_enabled and idle_for >= receive_idle_timeout:
                            counters["watchdog_idle_timeout"] += 1
                            status["watchdog_trigger_count"] = int(status.get("watchdog_trigger_count", 0)) + 1
                            persist_status(
                                state="watchdog_reconnect",
                                connected=False,
                                no_data_reason="SOCKET_IDLE_TIMEOUT",
                                socket_idle_sec=idle_for,
                            )
                            await ws.close(code=1000, reason="socket idle timeout")
                            raise SocketIdleTimeout()
                        continue

                    now_message = utc_now()
                    last_message_monotonic = time.monotonic()
                    write_jsonl(raw_path, {"ts": now_message, "dir": "in", "raw": redact_secret_only(msg)})
                    counters["received_message_count"] += 1
                    counters["written_raw_count"] += 1
                    status["received_message_count"] = int(counters["received_message_count"])
                    status["written_raw_count"] = int(counters["written_raw_count"])
                    status["consecutive_receive_timeouts"] = 0
                    status["last_socket_message_at"] = now_message
                    status["last_socket_write_at"] = now_message
                    status["no_data_reason"] = None

                    if msg == "2":
                        status["last_engineio_heartbeat_at"] = now_message
                        await ws.send("3")
                        counters["engineio_ping"] += 1
                        continue

                    if msg == "40" and not sent_init:
                        init_payload = {
                            "gid": gid,
                            "token": token,
                            "id": ms2_id,
                            "rid": "jwt",
                            "v": 2,
                        }

                        pkt = sio_packet("init", init_payload)
                        await ws.send(pkt)
                        write_jsonl(raw_path, {"ts": utc_now(), "dir": "out", "raw": redact_secret_only(pkt), "note": "init"})
                        counters["written_raw_count"] += 1
                        status["written_raw_count"] = int(counters["written_raw_count"])
                        status["last_socket_write_at"] = utc_now()
                        sent_init = True
                        counters["sent_init"] += 1
                        continue

                    parsed = parse_sio_packet(msg)

                    if not parsed:
                        counters["non_sio"] += 1
                        continue

                    status["last_application_frame_at"] = now_message

                    if parsed[0] == "init" and not sent_subscribe:
                        pkt = sio_packet("subscribe", make_subscribe_payload())
                        await ws.send(pkt)
                        write_jsonl(raw_path, {"ts": utc_now(), "dir": "out", "raw": redact_secret_only(pkt), "note": "subscribe_odds"})
                        counters["written_raw_count"] += 1
                        status["written_raw_count"] = int(counters["written_raw_count"])
                        status["last_socket_write_at"] = utc_now()
                        sent_subscribe = True
                        counters["sent_subscribe"] += 1
                        persist_status(state="subscribed", subscribed=True)
                        continue

                    if parsed[0] != "m" or len(parsed) < 3:
                        counters[f"event:{parsed[0]}"] += 1
                        continue

                    counters["m_events"] += 1

                    bid = parsed[1]
                    payload = parsed[2]

                    if not isinstance(payload, list):
                        counters["bad_payload"] += 1
                        continue

                    field_map = field_maps_by_bid[bid]

                    for part in payload:
                        if not isinstance(part, list) or not part:
                            counters["bad_part"] += 1
                            continue

                        if part[0] == "c":
                            if len(part) >= 2:
                                channel_by_bid[bid] = part[1]
                            counters["schema_c"] += 1
                            continue

                        if part[0] == "f":
                            if len(part) >= 3 and isinstance(part[1], int) and isinstance(part[2], list):
                                offset = part[1]
                                for idx, name in enumerate(part[2]):
                                    field_map[offset + idx] = name
                            counters["schema_f"] += 1
                            continue

                        row = decode_pairs(part, field_map)

                        row["_bid"] = bid
                        row["_channel"] = channel_by_bid.get(bid)

                        rtype = row.get("type")
                        counters[f"row_type:{rtype}"] += 1

                        if rtype == "m":
                            matches[row.get("matchid")] = row
                            continue

                        if rtype != "o":
                            continue

                        counters["odds_rows_total"] += 1

                        if row.get("oddsstatus") != "running":
                            filter_reasons["odds_not_running"] += 1
                            continue

                        bettype = row.get("bettype")

                        if bettype not in SUPPORTED_BETTYPES_INITIAL:
                            filter_reasons[f"unsupported_bettype:{bettype}"] += 1

                            match = matches.get(row.get("matchid"))

                        # Expand special goal markets from one socket row into many normalized rows.
                        # Safe mode: still counted as unsupported for audit, but also normalized for storage/replay.
                            try:
                                expanded_goal_rows = expand_goal_special_rows(row, match)
                                if expanded_goal_rows:
                                    for goal_obj in expanded_goal_rows:
                                        write_jsonl(norm_path, goal_obj)
                                    counters["goal_special_normalized_written"] += len(expanded_goal_rows)
                                    status["last_normalized_write_at"] = utc_now()
                                    counters[f"goal_special_bettype:{bettype}"] += len(expanded_goal_rows)
                            except Exception as e:
                                filter_reasons[f"goal_special_normalize_error:{type(e).__name__}"] += 1

                        # Audit decoded unsupported rows so market_registry can be expanded safely.
                        # This is collect-only: no betting, no signal, just saving decoded market rows.
                            try:
                                counters["unsupported_audit_seen"] += 1
                                if bettype not in (6, 406) and is_goal_band_audit_candidate(bettype, row, match) and counters["unsupported_audit_seen"] <= 5000:
                                    audit_dir = norm_path.parent.parent / "audit"
                                    audit_dir.mkdir(parents=True, exist_ok=True)
                                    audit_hour = utc_now()[:13].replace("-", "").replace("T", "_")
                                    audit_path = audit_dir / f"saba_goal_bands_candidates_{audit_hour}.jsonl"

                                    audit_obj = {
                                        "ts": utc_now(),
                                        "reason": f"unsupported_bettype:{bettype}",
                                        "bettype": bettype,
                                        "row": row,
                                    }

                                    try:
                                        audit_obj["match"] = match
                                    except Exception:
                                        pass

                                    write_jsonl(audit_path, audit_obj)
                                    counters["unsupported_audit_written"] += 1
                            except Exception as e:
                                filter_reasons[f"unsupported_audit_error:{type(e).__name__}"] += 1

                            continue

                        match = matches.get(row.get("matchid"))
                        reason = match_filter_reason(match)

                        if reason:
                            filter_reasons[reason] += 1
                            continue

                        normalized = normalize_odds_row(row, match)
                        write_jsonl(norm_path, normalized)
                        counters["normalized_written"] += 1
                        status["last_normalized_write_at"] = utc_now()

                    if counters["received_message_count"] % 100 == 0:
                        persist_status()

        except SocketIdleTimeout:
            reconnect_needed = True
            print(f"watchdog: SOCKET_IDLE_TIMEOUT attempt={attempt}")
        except Exception as e:
            reconnect_needed = True
            counters["connect_error"] += 1
            print(f"connect_error: {type(e).__name__}")
            persist_status(state="connection_error", connected=False, no_data_reason=type(e).__name__)

        if time.monotonic() >= cycle_deadline:
            break
        if reconnect_needed and attempt <= max_reconnects:
            continue
        if reconnect_needed:
            persist_status(state="max_reconnects_reached", connected=False, no_data_reason="MAX_RECONNECTS_REACHED")
        break

    print("")
    print("summary:")
    print("counters:", dict(counters))
    print("filter_reasons:", dict(filter_reasons))
    print("matches_state:", len(matches))
    print("norm_path:", norm_path)
    print("raw_path:", raw_path)
    persist_status(
        state="cycle_finished",
        connected=False,
        no_data_reason=status.get("no_data_reason") or "COLLECTOR_CYCLE_FINISHED",
    )
    print("============================================")


if __name__ == "__main__":
    asyncio.run(main())
