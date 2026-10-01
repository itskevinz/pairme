from gevent import monkey
monkey.patch_all()

import os
import re
import time
import uuid
import secrets
import logging
import hashlib
import ipaddress
from collections import defaultdict
from functools import wraps

from flask import Flask, Response, request
from flask_socketio import SocketIO, emit, join_room, leave_room

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pairme")

APP_VERSION = "2.5.0"
SECRET = os.environ.get("SECRET_KEY") or hashlib.sha256(os.urandom(32)).hexdigest()

app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET

ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*")
cors_origins = ALLOWED_ORIGINS.split(",") if ALLOWED_ORIGINS != "*" else "*"

socketio = SocketIO(
    app,
    cors_allowed_origins=cors_origins,
    async_mode="gevent",
    ping_timeout=60,
    ping_interval=25,
    max_http_buffer_size=2 * 1024 * 1024,
    engineio_logger=False,
    logger=False,
)

LOBBY_PREFIX = "lobby:"
LOBBY_SCOPE = os.environ.get("LOBBY_SCOPE", "ip").lower()

peers = {}
rooms_index = defaultdict(set)
peer_id_to_sid = {}
sessions = {}
rate_buckets = defaultdict(dict)
dirty_rooms = set()
broadcast_pending = False
services_started = False

CLEANUP_INTERVAL = 30
SESSION_TTL = 1800
BROADCAST_DEBOUNCE = 0.04

NAME_RE = re.compile(r"[\w \-.]{1,32}", re.UNICODE)
ROOM_CODE_RE = re.compile(r"[0-9]{6}")
MAX_TEXT_LEN = 20000
MAX_CHUNK_BYTES = 256 * 1024
MAX_CHUNK_B64_LEN = 350_000
RATE_LIMIT_WINDOW = 5.0
RATE_LIMIT_CAPACITY = 240
FILECHUNK_UNIT = 16384
FILECHUNK_CAPACITY = 3200
FILECHUNK_WINDOW = 5.0


def safe_int(value, default=0, low=0, high=2 ** 53):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return max(low, min(high, number))


def take_tokens(sid, bucket, cost, capacity, window):
    now = time.monotonic()
    buckets = rate_buckets[sid]
    state = buckets.get(bucket)
    if state is None:
        state = buckets[bucket] = [float(capacity), now]
    tokens = min(float(capacity), state[0] + (now - state[1]) * (capacity / window))
    state[1] = now
    if tokens < cost:
        state[0] = tokens
        return False
    state[0] = tokens - cost
    return True


def guarded(weight=1, bucket="default", capacity=RATE_LIMIT_CAPACITY, window=RATE_LIMIT_WINDOW):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            sid = request.sid
            if not take_tokens(sid, bucket, weight, capacity, window):
                log.warning("rate limit hit sid=%s event=%s bucket=%s", sid, fn.__name__, bucket)
                emit("rate_limited", {"event": fn.__name__})
                return False
            return fn(*args, **kwargs)
        return wrapper
    return deco


def client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    raw = forwarded.split(",")[0].strip() if forwarded else ""
    return raw or request.remote_addr or "unknown"


def normalize_ip(raw):
    try:
        address = ipaddress.ip_address(raw)
    except ValueError:
        return raw
    if address.version == 6:
        return str(ipaddress.ip_network(f"{address}/64", strict=False).network_address)
    return str(address)


def lobby_room_for_request():
    if LOBBY_SCOPE == "global":
        return LOBBY_PREFIX + "global"
    digest = hashlib.sha256((SECRET + normalize_ip(client_ip())).encode()).hexdigest()[:16]
    return LOBBY_PREFIX + digest


def is_lobby(room):
    return room.startswith(LOBBY_PREFIX)


def public_room_label(room):
    return "Lobby" if is_lobby(room) else room


def parse_fp(raw):
    cleaned = re.sub(r"[^a-f0-9]", "", (raw or "").lower())[:64]
    return cleaned if len(cleaned) >= 16 else ""


def allocate_peer_id(fp):
    candidate = fp[:8] if fp else uuid.uuid4().hex[:8]
    while candidate in peer_id_to_sid:
        candidate = uuid.uuid4().hex[:8]
    return candidate


def default_name(peer_id):
    return "Device " + peer_id[-4:].upper()


def new_room_code():
    while True:
        code = str(100000 + secrets.randbelow(900000))
        if code not in rooms_index:
            return code


def remember_session(info):
    fp = info.get("fp")
    if not fp:
        return
    sessions[fp] = {
        "room": None if is_lobby(info["room"]) else info["room"],
        "name": info["name"] if info["named"] else None,
        "seen": time.time(),
    }


def unindex(sid, room):
    members = rooms_index.get(room)
    if members is None:
        return
    members.discard(sid)
    if not members:
        del rooms_index[room]


def schedule_broadcast(room):
    global broadcast_pending
    if not room:
        return
    dirty_rooms.add(room)
    if not broadcast_pending:
        broadcast_pending = True
        socketio.start_background_task(flush_broadcasts)


def flush_broadcasts():
    global broadcast_pending
    socketio.sleep(BROADCAST_DEBOUNCE)
    rooms = list(dirty_rooms)
    dirty_rooms.clear()
    broadcast_pending = False
    for room in rooms:
        broadcast_peers(room)


def broadcast_peers(room):
    roster = [
        {"sid": sid, "id": peers[sid]["id"], "name": peers[sid]["name"]}
        for sid in rooms_index.get(room, ())
        if sid in peers
    ]
    if roster:
        socketio.emit("peers", roster, to=room)


def move_peer(sid, new_room):
    info = peers[sid]
    old_room = info["room"]
    if old_room == new_room:
        return False
    leave_room(old_room)
    unindex(sid, old_room)
    join_room(new_room)
    rooms_index[new_room].add(sid)
    info["room"] = new_room
    remember_session(info)
    schedule_broadcast(old_room)
    schedule_broadcast(new_room)
    return True


def remove_peer(sid):
    rate_buckets.pop(sid, None)
    info = peers.pop(sid, None)
    if not info:
        return
    unindex(sid, info["room"])
    remember_session(info)
    if peer_id_to_sid.get(info["id"]) == sid:
        del peer_id_to_sid[info["id"]]
    schedule_broadcast(info["room"])


def is_sid_connected(sid):
    try:
        return socketio.server.manager.is_connected(sid, "/")
    except Exception:
        return True


def drop_ghost_peers():
    for sid in [s for s in peers if not is_sid_connected(s)]:
        log.info("dropping ghost peer sid=%s", sid)
        remove_peer(sid)
    for sid in [s for s in rate_buckets if s not in peers]:
        rate_buckets.pop(sid, None)


def expire_sessions():
    live = {info["fp"] for info in peers.values() if info.get("fp")}
    cutoff = time.time() - SESSION_TTL
    for fp in [f for f, s in sessions.items() if s["seen"] < cutoff and f not in live]:
        del sessions[fp]


def housekeeping_loop():
    while True:
        socketio.sleep(CLEANUP_INTERVAL)
        try:
            drop_ghost_peers()
            expire_sessions()
        except Exception:
            log.exception("housekeeping failed")


def start_background_services():
    global services_started
    if services_started:
        return
    services_started = True
    socketio.start_background_task(housekeeping_loop)


def resolve_target_sid(to):
    if not isinstance(to, str):
        return None
    if to in peers:
        return to
    return peer_id_to_sid.get(to)


def same_room(sid_a, sid_b):
    a, b = peers.get(sid_a), peers.get(sid_b)
    return bool(a and b and a["room"] == b["room"])


def routable_target(sid, data):
    if not isinstance(data, dict):
        return None
    target = resolve_target_sid(data.get("to"))
    if target and target in peers and same_room(sid, target):
        return target
    return None


def transfer_meta(sid, data):
    info = peers[sid]
    return {
        "from": sid,
        "from_peer": info["id"],
        "from_name": info["name"],
        "file_name": str(data.get("file_name", "file"))[:255],
        "file_size": safe_int(data.get("file_size")),
        "file_type": str(data.get("file_type", ""))[:100],
        "transfer_id": str(data.get("transfer_id", ""))[:64],
        "batch_id": str(data.get("batch_id", ""))[:64],
        "batch_total": safe_int(data.get("batch_total"), 1, 1, 100000),
        "batch_index": safe_int(data.get("batch_index"), 0, 0, 100000),
    }


def chunk_byte_size(chunk):
    if isinstance(chunk, (bytes, bytearray)):
        return len(chunk) if len(chunk) <= MAX_CHUNK_BYTES else None
    if isinstance(chunk, str):
        return (len(chunk) * 3) // 4 if len(chunk) <= MAX_CHUNK_B64_LEN else None
    return None


@app.route("/")
def index():
    etag = '"' + INDEX_ETAG + '"'
    headers = {
        "ETag": etag,
        "Cache-Control": "no-cache",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
    }
    if request.headers.get("If-None-Match") == etag:
        return Response(status=304, headers=headers)
    response = Response(INDEX_HTML, mimetype="text/html")
    response.headers.update(headers)
    return response


@app.route("/health")
def health():
    return {"status": "ok", "peers": len(peers), "rooms": len(rooms_index), "version": APP_VERSION}, 200


@socketio.on("connect")
def handle_connect():
    sid = request.sid
    fp = parse_fp(request.args.get("fp"))
    saved = sessions.get(fp) if fp else None
    peer_id = allocate_peer_id(fp)
    lobby = lobby_room_for_request()
    room = saved["room"] if saved and saved["room"] else lobby
    named = bool(saved and saved["name"])
    name = saved["name"] if named else default_name(peer_id)

    peers[sid] = {
        "id": peer_id,
        "name": name,
        "named": named,
        "room": room,
        "lobby": lobby,
        "fp": fp,
    }
    peer_id_to_sid[peer_id] = sid
    join_room(room)
    rooms_index[room].add(sid)
    emit("init", {"peer_id": peer_id, "sid": sid, "room": public_room_label(room), "name": name})
    schedule_broadcast(room)


@socketio.on("disconnect")
def handle_disconnect():
    remove_peer(request.sid)


@socketio.on("set_name")
@guarded()
def handle_set_name(data):
    sid = request.sid
    if sid not in peers or not isinstance(data, dict):
        return
    raw = str(data.get("name", "")).strip()
    if raw and NAME_RE.fullmatch(raw):
        info = peers[sid]
        info["name"] = raw
        info["named"] = True
        remember_session(info)
        schedule_broadcast(info["room"])


@socketio.on("join_room_code")
@guarded(bucket="join", capacity=12, window=30)
def handle_join_room_code(data):
    sid = request.sid
    if sid not in peers or not isinstance(data, dict):
        return
    code = str(data.get("code", "")).strip()
    if not ROOM_CODE_RE.fullmatch(code):
        emit("room_error", {"msg": "Invalid code"})
        return
    move_peer(sid, code)
    emit("room_joined", {"code": code})


@socketio.on("create_room_code")
@guarded(bucket="create", capacity=10, window=30)
def handle_create_room_code():
    sid = request.sid
    if sid not in peers:
        return
    code = new_room_code()
    move_peer(sid, code)
    emit("room_joined", {"code": code, "created": True})


@socketio.on("leave_room_code")
@guarded()
def handle_leave_room_code():
    sid = request.sid
    if sid not in peers:
        return
    move_peer(sid, peers[sid]["lobby"])
    emit("room_left", {})


@socketio.on("signal")
@guarded(weight=2)
def handle_signal(data):
    sid = request.sid
    target = routable_target(sid, data)
    if target is None:
        return
    emit("signal", {
        "from": sid,
        "from_peer": peers[sid]["id"],
        "from_name": peers[sid]["name"],
        "signal": data.get("signal"),
    }, to=target)


@socketio.on("broadcast_request")
@guarded()
def handle_broadcast_request(data):
    sid = request.sid
    target = routable_target(sid, data)
    if target is not None:
        emit("transfer_request", transfer_meta(sid, data), to=target)


@socketio.on("broadcast_response")
@guarded()
def handle_broadcast_response(data):
    sid = request.sid
    target = routable_target(sid, data)
    if target is not None:
        emit("transfer_response", {
            "from": sid,
            "accepted": bool(data.get("accepted", False)),
            "transfer_id": str(data.get("transfer_id", ""))[:64],
        }, to=target)


@socketio.on("relay_text")
@guarded(weight=2)
def handle_relay_text(data):
    sid = request.sid
    target = routable_target(sid, data)
    if target is None:
        return
    emit("relay_text", {
        "from": sid,
        "from_name": peers[sid]["name"],
        "text": str(data.get("text", ""))[:MAX_TEXT_LEN],
    }, to=target)


@socketio.on("relay_file_start")
@guarded()
def handle_relay_file_start(data):
    sid = request.sid
    target = routable_target(sid, data)
    if target is not None:
        emit("relay_file_start", transfer_meta(sid, data), to=target)


@socketio.on("relay_file_chunk")
@guarded(weight=0)
def handle_relay_file_chunk(data):
    sid = request.sid
    target = routable_target(sid, data)
    if target is None:
        return False
    chunk = data.get("chunk")
    size = chunk_byte_size(chunk)
    if size is None:
        return False
    cost = max(1, size // FILECHUNK_UNIT)
    if not take_tokens(sid, "filechunk", cost, FILECHUNK_CAPACITY, FILECHUNK_WINDOW):
        return False
    emit("relay_file_chunk", {
        "from": sid,
        "transfer_id": str(data.get("transfer_id", ""))[:64],
        "chunk": bytes(chunk) if isinstance(chunk, bytearray) else chunk,
        "seq": safe_int(data.get("seq")),
    }, to=target)
    return True


@socketio.on("relay_file_done")
@guarded()
def handle_relay_file_done(data):
    sid = request.sid
    target = routable_target(sid, data)
    if target is not None:
        emit("relay_file_done", {
            "from": sid,
            "transfer_id": str(data.get("transfer_id", ""))[:64],
        }, to=target)


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
    <title>PairMe - Fast Cross-Device File & Text Sharing</title>
    <meta name="description" content="Seamless peer-to-peer file transfer and real-time text sharing between all your devices over local network and internet.">
    <meta name="theme-color" content="#0f172a" media="(prefers-color-scheme: dark)">
    <meta name="theme-color" content="#ffffff" media="(prefers-color-scheme: light)">
    <meta property="og:type" content="website">
    <meta property="og:title" content="PairMe - Fast Cross-Device Sharing">
    <meta property="og:description" content="Share text, links, code, and files instantly across mobile and desktop devices.">
    <meta property="og:image" content="https://imgg.fr/r/LkSsr60e.png">
    <link rel="icon" type="image/png" href="https://imgg.fr/r/LkSsr60e.png">
    <link rel="apple-touch-icon" href="https://imgg.fr/r/LkSsr60e.png">
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/heic2any@0.0.4/dist/heic2any.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcode-generator/1.4.4/qrcode.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/libheif-js@1.18.2/libheif-wasm/libheif-bundle.js"></script>
    <style>
        :root {
            --bg: #f8fafc;
            --card: #ffffff;
            --border: #e2e8f0;
            --text: #0f172a;
            --muted: #64748b;
            --muted2: #94a3b8;
            --accent: #0f172a;
            --accent-soft: #f1f5f9;
            --success: #15803d;
            --warn: #b45309;
            --error: #b91c1c;
            --p2p: #6b21a8;
            --live: #f59e0b;
            --shadow: 0 1px 2px rgba(0,0,0,0.04);
            --radius: 10px;
        }
        [data-theme="dark"] {
            --bg: #0b1220;
            --card: #111827;
            --border: #1f2937;
            --text: #f1f5f9;
            --muted: #94a3b8;
            --muted2: #64748b;
            --accent: #e2e8f0;
            --accent-soft: #1e293b;
            --success: #4ade80;
            --warn: #fbbf24;
            --error: #f87171;
            --p2p: #c084fc;
            --live: #fbbf24;
            --shadow: 0 1px 3px rgba(0,0,0,0.35);
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; -webkit-tap-highlight-color: transparent; }
        html, body { height: 100%; }
        body { background: var(--bg); color: var(--text); display: flex; flex-direction: column; overflow: hidden; -webkit-text-size-adjust: 100%; transition: background 0.2s, color 0.2s; }
        header { background: var(--card); padding: 10px 16px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; flex-shrink: 0; padding-top: max(10px, env(safe-area-inset-top)); }
        .brand { font-size: 16px; font-weight: 700; color: var(--text); letter-spacing: -0.3px; display: flex; align-items: center; gap: 8px; }
        .brand img { width: 22px; height: 22px; border-radius: 5px; object-fit: cover; }
        .version-badge { font-size: 10px; font-weight: 600; color: var(--muted); background: var(--accent-soft); border: 1px solid var(--border); padding: 1px 6px; border-radius: 5px; letter-spacing: 0; }
        .room-tag { background: var(--accent-soft); color: var(--muted); padding: 4px 10px; border-radius: 8px; font-size: 12px; font-weight: 600; border: 1px solid var(--border); display: flex; align-items: center; gap: 5px; }
        .theme-btn { background: transparent; border: 1px solid var(--border); color: var(--muted); border-radius: 8px; width: 34px; height: 34px; display: flex; align-items: center; justify-content: center; cursor: pointer; padding: 0; min-height: 34px; }
        .mobile-nav { display: none; background: var(--card); border-bottom: 1px solid var(--border); flex-shrink: 0; }
        .mobile-nav button { flex: 1; background: transparent; border: none; border-bottom: 2px solid transparent; padding: 12px 0; color: var(--muted); font-size: 13px; font-weight: 600; border-radius: 0; min-height: 44px; }
        .mobile-nav button.active { color: var(--text); border-bottom-color: var(--text); background: transparent; }
        .app-grid { display: grid; grid-template-columns: 280px 1fr 300px; gap: 12px; padding: 12px; height: calc(100vh - 53px); flex: 1; overflow: hidden; }
        .card { background: var(--card); border: 1px solid var(--border); border-radius: var(--radius); display: flex; flex-direction: column; overflow: hidden; height: 100%; box-shadow: var(--shadow); }
        .card-header { padding: 10px 14px; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; color: var(--muted); border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; background: var(--card); flex-shrink: 0; }
        .card-body { padding: 12px; flex: 1; overflow-y: auto; -webkit-overflow-scrolling: touch; display: flex; flex-direction: column; gap: 10px; }
        input, select, textarea { font-size: 14px; border-radius: 8px; border: 1px solid var(--border); background: var(--card); color: var(--text); padding: 10px 12px; outline: none; -webkit-appearance: none; appearance: none; transition: border-color 0.15s; }
        input:focus, select:focus, textarea:focus { border-color: var(--text); }
        button { background: var(--accent); color: var(--bg); border: 1px solid var(--accent); border-radius: 8px; font-size: 13px; font-weight: 500; padding: 9px 14px; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; gap: 6px; -webkit-appearance: none; min-height: 38px; transition: opacity 0.15s; }
        button:active { opacity: 0.8; }
        button:disabled { opacity: 0.55; cursor: default; }
        button.flat { background: var(--card); color: var(--text); border: 1px solid var(--border); }
        button.icon-only { padding: 8px; width: 38px; height: 38px; flex-shrink: 0; }
        button:focus-visible, input:focus-visible, select:focus-visible, textarea:focus-visible, a:focus-visible { outline: 2px solid var(--text); outline-offset: 2px; }
        .row { display: flex; gap: 8px; align-items: center; }
        .flex-1 { flex: 1; min-width: 0; }
        .peer-item { background: var(--card); border: 1px solid var(--border); padding: 10px 12px; border-radius: 8px; display: flex; justify-content: space-between; align-items: center; cursor: pointer; transition: border-color 0.15s, background 0.15s; min-height: 48px; }
        .peer-item:active, .peer-item.active { border-color: var(--text); background: var(--accent-soft); }
        .peer-info { display: flex; flex-direction: column; gap: 2px; }
        .peer-name { font-weight: 600; font-size: 13px; color: var(--text); }
        .peer-id { font-size: 11px; color: var(--muted2); font-family: ui-monospace, monospace; }
        .peer-status { font-size: 10px; font-weight: 600; padding: 2px 6px; border-radius: 4px; }
        .status-p2p { background: #f3e8ff; color: #6b21a8; }
        [data-theme="dark"] .status-p2p { background: #3b0764; color: #e9d5ff; }
        .status-relay { background: #e0f2fe; color: #0369a1; }
        [data-theme="dark"] .status-relay { background: #0c4a6e; color: #bae6fd; }
        .status-conn { background: #fef3c7; color: #b45309; }
        [data-theme="dark"] .status-conn { background: #78350f; color: #fde68a; }
        .drop-zone { border: 2px dashed var(--border); border-radius: 10px; padding: 20px 16px; text-align: center; color: var(--muted); cursor: pointer; background: var(--accent-soft); display: flex; flex-direction: column; align-items: center; gap: 8px; font-size: 13px; transition: border-color 0.15s, background 0.15s; min-height: 100px; }
        .drop-zone.dragover { border-color: var(--text); background: var(--card); }
        .feed-list { list-style: none; display: flex; flex-direction: column; gap: 10px; }
        .feed-item { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px; font-size: 13px; box-shadow: var(--shadow); display: flex; flex-direction: column; gap: 8px; }
        .feed-header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 6px; }
        .feed-author { font-weight: 600; font-size: 12px; color: var(--text); display: flex; align-items: center; gap: 6px; }
        .feed-time { font-size: 11px; color: var(--muted2); }
        .feed-actions { display: flex; gap: 6px; align-items: center; }
        .text-content { font-size: 13px; line-height: 1.55; color: var(--text); word-break: break-word; white-space: pre-wrap; }
        .text-link { color: #2563eb; text-decoration: underline; word-break: break-all; }
        [data-theme="dark"] .text-link { color: #60a5fa; }
        .code-wrapper { margin: 6px 0; border-radius: 8px; overflow: hidden; background: #0f172a; border: 1px solid #1e293b; }
        .code-header { display: flex; justify-content: space-between; align-items: center; background: #1e293b; padding: 5px 10px; font-size: 11px; color: #94a3b8; font-family: ui-monospace, monospace; }
        .copy-btn { background: transparent; border: 1px solid #475569; color: #cbd5e1; border-radius: 5px; padding: 3px 8px; font-size: 10px; cursor: pointer; min-height: 0; }
        .copy-btn:active { background: #334155; }
        .code-block { color: #f8fafc; padding: 10px; font-family: ui-monospace, Consolas, Monaco, monospace; font-size: 12px; line-height: 1.45; overflow-x: auto; max-height: 280px; white-space: pre; word-break: normal; -webkit-overflow-scrolling: touch; }
        .inline-code { background: var(--accent-soft); color: var(--text); border: 1px solid var(--border); padding: 1px 5px; border-radius: 4px; font-family: ui-monospace, monospace; font-size: 12px; word-break: break-all; }
        .expandable-block { position: relative; max-height: 220px; overflow: hidden; transition: max-height 0.2s ease; }
        .expandable-block.expanded { max-height: none !important; }
        .expandable-overlay { position: absolute; bottom: 0; left: 0; right: 0; height: 60px; background: linear-gradient(to bottom, transparent, var(--card)); pointer-events: none; display: flex; align-items: flex-end; justify-content: center; padding-bottom: 4px; }
        .expandable-block.expanded .expandable-overlay { display: none; }
        .expand-toggle-btn { background: var(--card); border: 1px solid var(--border); color: var(--text); font-size: 11px; font-weight: 600; padding: 4px 12px; border-radius: 14px; cursor: pointer; pointer-events: auto; box-shadow: var(--shadow); min-height: 0; }
        .file-card { display: flex; align-items: center; justify-content: space-between; gap: 10px; background: var(--accent-soft); border: 1px solid var(--border); padding: 10px; border-radius: 8px; }
        .file-meta { display: flex; flex-direction: column; min-width: 0; flex: 1; }
        .file-title { font-weight: 600; font-size: 12px; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .file-size { font-size: 11px; color: var(--muted); }
        .file-preview { margin-top: 4px; text-align: center; background: #0f172a; border-radius: 8px; overflow: hidden; max-height: 240px; display: flex; align-items: center; justify-content: center; }
        .preview-img { max-width: 100%; max-height: 240px; object-fit: contain; display: block; }
        .action-btn { background: var(--card); border: 1px solid var(--border); color: var(--text); padding: 5px 10px; font-size: 11px; border-radius: 6px; font-weight: 500; height: 28px; min-height: 28px; text-decoration: none; display: inline-flex; align-items: center; justify-content: center; gap: 4px; cursor: pointer; }
        .action-btn:active { background: var(--accent-soft); }
        .action-btn.primary { background: var(--accent); color: var(--bg); border-color: var(--accent); }
        .media-gallery { display: grid; grid-template-columns: repeat(auto-fill, minmax(88px, 1fr)); gap: 6px; margin-top: 4px; }
        .media-thumb { position: relative; aspect-ratio: 1; border-radius: 8px; overflow: hidden; background: #0f172a; cursor: pointer; border: 1px solid var(--border); }
        .media-thumb img, .media-thumb video { width: 100%; height: 100%; object-fit: cover; display: block; }
        .media-thumb .play-badge { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; background: rgba(15,23,42,0.35); pointer-events: none; }
        .media-thumb .play-badge svg { width: 22px; height: 22px; color: #fff; filter: drop-shadow(0 1px 2px rgba(0,0,0,0.4)); }
        .live-badge { position: absolute; top: 5px; left: 5px; background: rgba(15,23,42,0.72); color: #fff; font-size: 9px; font-weight: 800; letter-spacing: 0.6px; padding: 2px 6px 2px 5px; border-radius: 10px; display: inline-flex; align-items: center; gap: 3px; pointer-events: none; z-index: 2; }
        .live-badge svg { width: 10px; height: 10px; color: var(--live); }
        .heic-fallback { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 6px; width: 100%; height: 100%; background: var(--accent-soft); color: var(--muted); }
        .heic-fallback .heic-chip { background: var(--border); color: var(--text); font-size: 11px; font-weight: 800; letter-spacing: 0.5px; padding: 3px 9px; border-radius: 6px; }
        .heic-fallback .heic-sub { font-size: 10px; color: var(--muted2); text-align: center; padding: 0 8px; line-height: 1.3; }
        .file-preview.heic-fallback-wrap { background: var(--accent-soft); }
        .batch-file-list { display: flex; flex-direction: column; gap: 6px; margin-top: 4px; }
        .batch-file-row { display: flex; align-items: center; gap: 10px; background: var(--accent-soft); border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; }
        .batch-file-thumb { position: relative; width: 42px; height: 42px; border-radius: 6px; overflow: hidden; background: #0f172a; flex-shrink: 0; display: flex; align-items: center; justify-content: center; cursor: pointer; }
        .batch-file-thumb img, .batch-file-thumb video { width: 100%; height: 100%; object-fit: cover; }
        .batch-file-icon { width: 42px; height: 42px; border-radius: 6px; background: var(--border); color: var(--muted); flex-shrink: 0; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px; }
        .batch-file-meta { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 1px; }
        .batch-file-name { font-weight: 600; font-size: 12px; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .batch-file-size { font-size: 11px; color: var(--muted); }
        .audio-player-wrap { margin-top: 6px; width: 100%; }
        .audio-player-wrap audio { width: 100%; height: 36px; border-radius: 6px; }
        .batch-audio-row audio { width: 100%; max-width: 220px; height: 32px; }
        .gallery-actions { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; justify-content: flex-end; align-items: center; }
        .gallery-meta { font-size: 11px; color: var(--muted); margin-top: 2px; }
        .zip-opt { display: inline-flex; align-items: center; gap: 6px; font-size: 11px; color: var(--muted); margin-right: auto; cursor: pointer; }
        .zip-opt input { width: 14px; height: 14px; padding: 0; -webkit-appearance: checkbox; appearance: checkbox; accent-color: var(--text); }

        .live-card { display: flex; flex-direction: column; gap: 8px; }
        .live-stage { position: relative; background: #0f172a; border-radius: 8px; overflow: hidden; display: flex; align-items: center; justify-content: center; min-height: 160px; max-height: 300px; cursor: pointer; }
        .live-stage img, .live-stage video { max-width: 100%; max-height: 300px; object-fit: contain; display: block; }
        .live-stage video { position: absolute; inset: 0; width: 100%; height: 100%; background: #0f172a; }
        .live-stage .live-badge { top: 8px; left: 8px; }
        .live-hint { position: absolute; bottom: 8px; left: 0; right: 0; text-align: center; font-size: 10px; color: rgba(255,255,255,0.78); pointer-events: none; text-shadow: 0 1px 2px rgba(0,0,0,0.6); }
        .seg { display: inline-flex; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; background: var(--card); }
        .seg button { background: transparent; color: var(--muted); border: none; border-radius: 0; min-height: 28px; height: 28px; padding: 0 12px; font-size: 11px; font-weight: 600; }
        .seg button.on { background: var(--accent); color: var(--bg); }
        .live-toolbar { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; justify-content: space-between; }
        .dl-menu { position: relative; display: inline-block; }
        .dl-pop { display: none; position: absolute; right: 0; bottom: calc(100% + 6px); min-width: 190px; background: var(--card); border: 1px solid var(--border); border-radius: 10px; box-shadow: 0 8px 28px rgba(0,0,0,0.22); padding: 4px; z-index: 40; flex-direction: column; }
        .dl-pop.open { display: flex; }
        .dl-pop button { background: transparent; color: var(--text); border: none; border-radius: 6px; justify-content: flex-start; min-height: 34px; padding: 6px 10px; font-size: 12px; width: 100%; text-align: left; }
        .dl-pop button:active, .dl-pop button:hover { background: var(--accent-soft); }
        .dl-pop small { display: block; font-size: 10px; color: var(--muted2); font-weight: 400; }

        .lightbox { display: none; position: fixed; inset: 0; z-index: 200; background: rgba(15,23,42,0.94); flex-direction: column; align-items: center; justify-content: center; padding: 12px; padding-top: max(12px, env(safe-area-inset-top)); }
        .lightbox.open { display: flex; }
        .lightbox-toolbar { position: absolute; top: 0; left: 0; right: 0; display: flex; justify-content: space-between; align-items: center; padding: 12px 14px; color: #e2e8f0; font-size: 13px; background: linear-gradient(to bottom, rgba(0,0,0,0.55), transparent); padding-top: max(12px, env(safe-area-inset-top)); z-index: 3; gap: 8px; }
        .lightbox-close, .lightbox-nav { background: rgba(255,255,255,0.12); border: 1px solid rgba(255,255,255,0.2); color: #fff; border-radius: 8px; padding: 8px 14px; font-size: 13px; cursor: pointer; min-height: 40px; }
        .lightbox-stage { position: relative; max-width: 96vw; max-height: 74vh; display: flex; align-items: center; justify-content: center; }
        .lightbox-stage img, .lightbox-stage video { max-width: 96vw; max-height: 74vh; object-fit: contain; border-radius: 6px; box-shadow: 0 8px 32px rgba(0,0,0,0.4); }
        .lightbox-nav-wrap { position: absolute; inset: 0; display: flex; align-items: center; justify-content: space-between; pointer-events: none; padding: 0 8px; z-index: 2; }
        .lightbox-nav-wrap button { pointer-events: auto; width: 44px; height: 44px; border-radius: 50%; display: flex; align-items: center; justify-content: center; padding: 0; }
        .lightbox-counter { font-variant-numeric: tabular-nums; }
        .lightbox-bottom { position: absolute; bottom: 0; left: 0; right: 0; display: flex; justify-content: center; align-items: center; gap: 8px; padding: 12px 14px; padding-bottom: max(14px, env(safe-area-inset-bottom)); background: linear-gradient(to top, rgba(0,0,0,0.55), transparent); z-index: 3; flex-wrap: wrap; }
        .lightbox-bottom .seg { border-color: rgba(255,255,255,0.25); background: rgba(255,255,255,0.08); }
        .lightbox-bottom .seg button { color: #cbd5e1; }
        .lightbox-bottom .seg button.on { background: #fff; color: #0f172a; }
        .lightbox-name { color: #cbd5e1; font-size: 11px; max-width: 60vw; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .lb-live-hint { position: absolute; bottom: 10px; left: 0; right: 0; text-align: center; font-size: 11px; color: rgba(255,255,255,0.8); pointer-events: none; text-shadow: 0 1px 2px rgba(0,0,0,0.6); }
        .lb-heic-fallback { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 10px; color: #cbd5e1; padding: 40px; text-align: center; }
        .lb-heic-fallback .heic-chip { background: rgba(255,255,255,0.14); color: #fff; font-size: 12px; font-weight: 800; letter-spacing: 0.5px; padding: 4px 12px; border-radius: 8px; }
        .lb-heic-fallback .heic-sub { font-size: 12px; color: #94a3b8; max-width: 280px; line-height: 1.5; }

        #log-container { font-family: ui-monospace, monospace; font-size: 11px; flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 4px; -webkit-overflow-scrolling: touch; }
        .log-entry { padding: 5px 7px; border-radius: 5px; display: flex; gap: 6px; align-items: flex-start; line-height: 1.35; }
        .log-time { color: var(--muted2); flex-shrink: 0; }
        .log-tag { padding: 1px 5px; border-radius: 3px; font-weight: 700; font-size: 9px; text-transform: uppercase; flex-shrink: 0; }
        .tag-info { background: #e0f2fe; color: #0369a1; }
        .tag-success { background: #dcfce7; color: #15803d; }
        .tag-warn { background: #fef3c7; color: #b45309; }
        .tag-error { background: #fee2e2; color: #b91c1c; }
        .tag-p2p { background: #f3e8ff; color: #6b21a8; }
        [data-theme="dark"] .tag-info { background: #0c4a6e; color: #7dd3fc; }
        [data-theme="dark"] .tag-success { background: #14532d; color: #86efac; }
        [data-theme="dark"] .tag-warn { background: #78350f; color: #fcd34d; }
        [data-theme="dark"] .tag-error { background: #7f1d1d; color: #fca5a5; }
        [data-theme="dark"] .tag-p2p { background: #3b0764; color: #e9d5ff; }
        .progress-bar { height: 5px; background: var(--border); border-radius: 3px; overflow: hidden; margin-top: 4px; }
        .progress-fill { height: 100%; background: var(--accent); width: 0%; transition: width 0.12s linear; }
        .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(15, 23, 42, 0.45); z-index: 100; justify-content: center; align-items: center; padding: 16px; }
        .modal { background: var(--card); border: 1px solid var(--border); padding: 18px; border-radius: 12px; width: 100%; max-width: 320px; text-align: center; box-shadow: 0 12px 40px rgba(0,0,0,0.2); }
        .transfer-active-badge { display: none; background: #fef3c7; color: #b45309; font-size: 11px; font-weight: 600; padding: 3px 9px; border-radius: 6px; }
        [data-theme="dark"] .transfer-active-badge { background: #78350f; color: #fde68a; }
        .conn-badge { font-size: 10px; font-weight: 700; padding: 2px 7px; border-radius: 5px; text-transform: uppercase; letter-spacing: 0.3px; }
        .empty-hint { color: var(--muted2); font-size: 12px; text-align: center; padding: 16px 8px; }

        .share-btn { background: var(--accent); color: var(--bg); border: 1px solid var(--accent); border-radius: 8px; height: 34px; min-height: 34px; padding: 0 12px; font-size: 12px; font-weight: 600; }
        .share-modal { max-width: 340px; padding: 22px 20px 18px; }
        .share-title { font-size: 16px; font-weight: 700; margin-bottom: 4px; }
        .share-sub { font-size: 12px; color: var(--muted); margin-bottom: 14px; line-height: 1.45; }
        .qr-frame { background: #ffffff; border: 1px solid var(--border); border-radius: 12px; padding: 12px; display: inline-flex; align-items: center; justify-content: center; margin-bottom: 12px; }
        .qr-frame svg, .qr-frame img { display: block; width: 216px; height: 216px; }
        .qr-fallback { width: 216px; height: 216px; display: flex; align-items: center; justify-content: center; color: #475569; font-size: 12px; text-align: center; padding: 12px; }
        .share-code { font-family: ui-monospace, Consolas, monospace; font-size: 26px; font-weight: 800; letter-spacing: 6px; color: var(--text); margin-bottom: 2px; }
        .share-code-label { font-size: 10px; text-transform: uppercase; letter-spacing: 0.6px; color: var(--muted2); margin-bottom: 12px; }
        .share-link { font-size: 11px; color: var(--muted); background: var(--accent-soft); border: 1px solid var(--border); border-radius: 8px; padding: 7px 9px; word-break: break-all; margin-bottom: 12px; text-align: left; font-family: ui-monospace, monospace; }
        .share-actions { display: flex; gap: 8px; justify-content: center; flex-wrap: wrap; }
        .share-close { margin-top: 10px; width: 100%; }
        .toast { position: fixed; left: 50%; bottom: max(20px, env(safe-area-inset-bottom)); transform: translateX(-50%) translateY(20px); background: var(--text); color: var(--bg); padding: 9px 16px; border-radius: 10px; font-size: 12px; font-weight: 600; opacity: 0; pointer-events: none; transition: opacity 0.2s, transform 0.2s; z-index: 300; }
        .toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }

        .stage-tray { border: 1px solid var(--border); border-radius: 10px; background: var(--accent-soft); padding: 8px; display: flex; flex-direction: column; gap: 8px; }
        .stage-list { display: grid; grid-template-columns: repeat(auto-fill, minmax(72px, 1fr)); gap: 6px; }
        .stage-item { position: relative; aspect-ratio: 1; border-radius: 8px; overflow: hidden; background: #0f172a; border: 1px solid var(--border); display: flex; align-items: center; justify-content: center; color: #cbd5e1; font-size: 10px; font-weight: 700; text-align: center; padding: 4px; }
        .stage-item img { width: 100%; height: 100%; object-fit: cover; position: absolute; inset: 0; }
        .stage-remove { position: absolute; top: 3px; right: 3px; width: 20px; height: 20px; min-height: 0; padding: 0; border-radius: 50%; background: rgba(15,23,42,0.75); border: none; color: #fff; font-size: 13px; line-height: 1; z-index: 2; }
        .stage-actions { display: flex; gap: 6px; align-items: center; justify-content: space-between; font-size: 11px; color: var(--muted); }
        .feed-right { display: inline-flex; align-items: center; gap: 8px; }
        .feed-remove { min-height: 0; width: 22px; height: 22px; padding: 0; border-radius: 6px; background: transparent; color: var(--muted); border: 1px solid var(--border); font-size: 14px; line-height: 1; }
        .drop-overlay { display: none; position: fixed; inset: 10px; z-index: 250; border: 3px dashed var(--text); border-radius: 16px; background: rgba(15,23,42,0.55); color: #fff; font-size: 18px; font-weight: 700; align-items: center; justify-content: center; pointer-events: none; }
        .drop-overlay.show { display: flex; }

        @media (max-width: 768px) {
            body { height: 100%; overflow: auto; }
            .mobile-nav { display: flex; }
            .app-grid { display: flex; flex-direction: column; height: auto; padding: 8px; padding-bottom: max(8px, env(safe-area-inset-bottom)); grid-template-columns: none; overflow: visible; gap: 8px; }
            .card { display: none; height: auto; min-height: calc(100dvh - 120px); }
            .card.mobile-active { display: flex; }
            header { padding-left: max(12px, env(safe-area-inset-left)); padding-right: max(12px, env(safe-area-inset-right)); }
            .share-label { display: none; }
            .share-btn { width: 34px; padding: 0; }
        }
        @media (prefers-reduced-motion: reduce) {
            * { transition: none !important; animation: none !important; }
        }
    </style>
</head>
<body>
    <header>
        <div class="brand">
            <img src="https://imgg.fr/r/LkSsr60e.png" alt="Logo">
            PairMe
            <span class="version-badge">v{{ app_version }}</span>
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
            <span class="transfer-active-badge" id="transfer-badge">Transferring</span>
            <button class="share-btn" onclick="openShareModal()" title="Share via QR code" aria-label="Share via QR code">
                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><path d="M14 14h3v3h-3zM20 14v.01M14 20v.01M17 20h4v-3"/></svg>
                <span class="share-label">QR</span>
            </button>
            <div class="room-tag" id="room-badge">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 8v4l3 3"/></svg>
                <span id="room-name">Lobby</span>
            </div>
            <button class="theme-btn" onclick="toggleTheme()" title="Toggle theme" aria-label="Toggle theme">
                <svg id="theme-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
            </button>
        </div>
    </header>
    <div class="mobile-nav">
        <button id="nav-devices" class="active" onclick="switchTab('devices')">Devices</button>
        <button id="nav-transfer" onclick="switchTab('transfer')">Transfer</button>
        <button id="nav-logs" onclick="switchTab('logs')">Logs</button>
    </div>
    <div class="app-grid">
        <div class="card mobile-active" id="card-devices">
            <div class="card-header">
                <span>Devices</span>
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg>
            </div>
            <div class="card-body">
                <div>
                    <div style="font-size:11px;color:var(--muted2);margin-bottom:2px;">THIS DEVICE</div>
                    <div id="my-id" style="font-family:ui-monospace,monospace;font-size:14px;font-weight:700;color:var(--text);">---</div>
                </div>
                <div class="row">
                    <input type="text" id="my-name" placeholder="Device Name" class="flex-1" maxlength="32">
                    <button class="flat icon-only" onclick="updateName()" title="Save Name" aria-label="Save name">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
                    </button>
                </div>
                <hr style="border:none;border-top:1px solid var(--border);">
                <div class="row">
                    <input type="text" id="room-code-input" placeholder="6-digit code" maxlength="6" inputmode="numeric" pattern="[0-9]*" class="flex-1">
                    <button class="flat" onclick="joinRoom()">Join</button>
                </div>
                <div class="row">
                    <button class="flat flex-1" onclick="createRoom()">Create Room</button>
                    <button class="flat icon-only" onclick="leaveRoom()" title="Leave Room" aria-label="Leave room">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
                    </button>
                </div>
                <label class="zip-opt" style="margin-right:0;"><input type="checkbox" id="notify-toggle" checked onchange="toggleNotify(this.checked)"> Sound and notifications</label>
                <div class="card-header" style="margin:8px -12px 0 -12px;border-top:1px solid var(--border);">Nearby</div>
                <div id="peer-list" style="display:flex;flex-direction:column;gap:6px;">
                    <div class="empty-hint">No devices detected</div>
                </div>
            </div>
        </div>
        <div class="card" id="card-transfer">
            <div class="card-header">
                <span>Transfer</span>
                <span id="target-peer-label" style="color:var(--text);text-transform:none;font-weight:600;">To: Everyone</span>
            </div>
            <div class="card-body">
                <div class="row">
                    <select id="peer-select" class="flex-1" onchange="onPeerSelectChange()">
                        <option value="">-- All Devices --</option>
                    </select>
                </div>
                <div class="row">
                    <textarea id="text-input" rows="2" placeholder="Message, link, or code..." class="flex-1"></textarea>
                    <button class="icon-only" onclick="sendText()" title="Send" aria-label="Send text">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
                    </button>
                </div>
                <div class="drop-zone" id="drop-zone" onclick="document.getElementById('file-input').click()">
                    <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
                    <span>Tap, drag, or press Ctrl+V to paste</span>
                    <span style="font-size:11px;color:var(--muted2);">Live Photo: select both the HEIC and MOV files</span>
                    <input type="file" id="file-input" multiple accept="*/*" style="display:none;" onchange="handleFileSelect(event)">
                </div>
                <div class="row">
                    <button class="flat flex-1" onclick="readClipboardButton()">Paste from clipboard</button>
                    <button class="flat flex-1" id="camera-btn" onclick="document.getElementById('camera-input').click()">Camera</button>
                    <input type="file" id="camera-input" accept="image/*,video/*" capture="environment" style="display:none;" onchange="handleFileSelect(event)">
                </div>
                <div id="stage-tray" class="stage-tray" style="display:none;"></div>
                <div id="progress-wrap" style="display:none;">
                    <div class="row" style="justify-content:space-between;font-size:11px;color:var(--muted);">
                        <span id="send-status">Sending</span>
                        <span id="send-pct">0%</span>
                    </div>
                    <div class="progress-bar"><div class="progress-fill" id="progress-fill"></div></div>
                </div>
                <div class="card-header" style="margin:0 -12px;"><span>Received</span><button class="action-btn" onclick="clearReceived()">Clear all</button></div>
                <ul class="feed-list" id="received-list"></ul>
            </div>
        </div>
        <div class="card" id="card-logs">
            <div class="card-header">
                <span>Logs</span>
                <button class="flat icon-only" onclick="clearLogs()" title="Clear Logs" style="width:28px;height:28px;padding:2px;min-height:28px;" aria-label="Clear logs">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                </button>
            </div>
            <div class="card-body" style="padding:8px;">
                <div id="log-container"></div>
            </div>
        </div>
    </div>
    <div class="modal-overlay" id="request-modal">
        <div class="modal">
            <div style="font-size:15px;font-weight:700;margin-bottom:8px;">Transfer Request</div>
            <div id="request-details" style="font-size:13px;color:var(--muted);margin-bottom:18px;"></div>
            <div class="row" style="justify-content:center;gap:10px;">
                <button class="flat" onclick="respondRequest(false)">Decline</button>
                <button onclick="respondRequest(true)">Accept</button>
            </div>
        </div>
    </div>

    <div class="modal-overlay" id="share-modal" onclick="if(event.target===this) closeShareModal()">
        <div class="modal share-modal" role="dialog" aria-modal="true" aria-labelledby="share-title">
            <div class="share-title" id="share-title">Scan to connect</div>
            <div class="share-sub">Open the camera on another device and scan. It joins this room and can send files right away.</div>
            <div class="qr-frame" id="qr-frame"></div>
            <div class="share-code" id="share-code">------</div>
            <div class="share-code-label">Room code</div>
            <div class="share-link" id="share-link"></div>
            <div class="share-actions">
                <button class="flat" onclick="copyShareLink(this)">Copy link</button>
                <button class="flat" id="native-share-btn" onclick="nativeShare()" style="display:none;">Share…</button>
            </div>
            <button class="share-close" onclick="closeShareModal()">Done</button>
        </div>
    </div>

    <div class="lightbox" id="lightbox" onclick="if(event.target===this) closeLightbox()">
        <div class="lightbox-toolbar">
            <span class="lightbox-counter" id="lightbox-counter">1 / 1</span>
            <div class="row" style="gap:8px;">
                <button class="lightbox-nav" id="lightbox-download" onclick="downloadLightboxItem()">Download</button>
                <button class="lightbox-close" onclick="closeLightbox()">Close</button>
            </div>
        </div>
        <div class="lightbox-nav-wrap">
            <button class="lightbox-nav" onclick="lightboxNav(-1)" title="Previous" aria-label="Previous">&#8249;</button>
            <button class="lightbox-nav" onclick="lightboxNav(1)" title="Next" aria-label="Next">&#8250;</button>
        </div>
        <div class="lightbox-stage" id="lightbox-stage"></div>
        <div class="lightbox-bottom" id="lightbox-bottom"></div>
    </div>
    <div class="toast" id="toast"></div>
    <div class="drop-overlay" id="drop-overlay">Drop files to send</div>
<script>
var socket = null;
var mySid = "";
var myPeerId = "";
var currentRoom = "Lobby";
var peerList = [];
var connections = {};
var pendingRequest = null;
var pendingFileQueue = {};
var relayFileQueue = {};
var relayBuffer = {};
var relayMeta = {};
var textStore = {};
var batchStore = {};
var mediaRegistry = {};
var lightboxItems = [];
var lightboxIndex = 0;
var lightboxMode = {};
var CHUNK_SIZE = 16384;
var P2P_MAX_CHUNK = 65536;
var P2P_HIGH_WATER = 4 * 1024 * 1024;
var P2P_LOW_WATER = 1024 * 1024;
var READ_BLOCK = 1024 * 1024;
var RELAY_CHUNK = 65536;
var RELAY_WINDOW_MIN = 4;
var RELAY_WINDOW_START = 8;
var RELAY_WINDOW_MAX = 48;
var RELAY_MAX_RETRIES = 8;
var RELAY_ACK_TIMEOUT = 8000;
var RELAY_BATCH_CONCURRENCY = 2;
var P2P_CONNECT_TIMEOUT = 2500;
var P2P_STALE_MS = 12000;
var BUFFER_STALE_MS = 120000;
var BATCH_RENDER_DELAY = 160;
var p2pDraining = {};
var p2pWaiters = {};
var lastProgressPaint = 0;
var memoryUid = null;
var TEXT_INLINE_LIMIT = 19000;
var stagedFiles = [];
var unreadCount = 0;
var BASE_TITLE = document.title;
var notifyEnabled = localStorage.getItem("pairme_notify") !== "0";
var audioCtx = null;
var dragDepth = 0;
var transferClock = { start: 0, total: 0 };
var STUN_SERVERS = {
    iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" },
        { urls: "stun:stun2.l.google.com:19302" },
        { urls: "stun:stun3.l.google.com:19302" },
        { urls: "stun:stun4.l.google.com:19302" },
        {
            urls: [
                "turn:openrelay.metered.ca:80",
                "turn:openrelay.metered.ca:443",
                "turn:openrelay.metered.ca:443?transport=tcp"
            ],
            username: "openrelayproject",
            credential: "openrelayproject"
        }
    ],
    iceCandidatePoolSize: 4
};
var WEBRTC_SUPPORTED = (typeof window.RTCPeerConnection === "function");
var activeTransfers = 0;
var peerConnState = {};
var pendingShareOpen = false;
var urlRoomCode = null;
var toastTimer = null;
var openDlPop = null;

function switchTab(tab) {
    var cards = ["devices", "transfer", "logs"];
    for (var i = 0; i < cards.length; i++) {
        var c = cards[i];
        document.getElementById("card-" + c).classList.remove("mobile-active");
        document.getElementById("nav-" + c).classList.remove("active");
    }
    document.getElementById("card-" + tab).classList.add("mobile-active");
    document.getElementById("nav-" + tab).classList.add("active");
}

function log(msg, type) {
    type = type || "info";
    var el = document.getElementById("log-container");
    var now = new Date();
    var time = now.getHours() + ":" + ("0" + now.getMinutes()).slice(-2) + ":" + ("0" + now.getSeconds()).slice(-2);
    var entry = document.createElement("div");
    entry.className = "log-entry";
    entry.innerHTML = '<span class="log-time">' + time + '</span><span class="log-tag tag-' + type + '">' + type + '</span><span style="word-break:break-all;">' + escapeHtml(msg) + '</span>';
    el.appendChild(entry);
    el.scrollTop = el.scrollHeight;
    while (el.children.length > 200) el.removeChild(el.firstChild);
}

function clearLogs() {
    document.getElementById("log-container").innerHTML = "";
}

function showToast(msg) {
    var t = document.getElementById("toast");
    t.textContent = msg;
    t.classList.add("show");
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(function() { t.classList.remove("show"); }, 2200);
}

function escapeHtml(text) {
    var div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
}

function formatBytes(bytes) {
    if (!bytes) return "0 B";
    var k = 1024;
    var sizes = ["B", "KB", "MB", "GB"];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

function toggleTheme() {
    var cur = document.documentElement.getAttribute("data-theme");
    var next = cur === "dark" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    localStorage.setItem("pairme_theme", next);
    updateThemeIcon(next);
}

function updateThemeIcon(theme) {
    var icon = document.getElementById("theme-icon");
    if (!icon) return;
    if (theme === "dark") {
        icon.innerHTML = '<circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>';
    } else {
        icon.innerHTML = '<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>';
    }
}

function initTheme() {
    var saved = localStorage.getItem("pairme_theme");
    var preferDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    var theme = saved || (preferDark ? "dark" : "light");
    document.documentElement.setAttribute("data-theme", theme);
    updateThemeIcon(theme);
}

function readRoomFromUrl() {
    try {
        var params = new URLSearchParams(window.location.search);
        var code = (params.get("room") || "").trim();
        if (/^\d{6}$/.test(code)) return code;
    } catch (e) {}
    return null;
}

function buildShareLink(code) {
    var base = window.location.origin + window.location.pathname;
    return base + "?room=" + encodeURIComponent(code);
}

function syncUrlWithRoom(code) {
    try {
        var url = new URL(window.location.href);
        if (code && code !== "Lobby") {
            url.searchParams.set("room", code);
        } else {
            url.searchParams.delete("room");
        }
        window.history.replaceState({}, "", url.toString());
    } catch (e) {}
}

function renderQr(link) {
    var frame = document.getElementById("qr-frame");
    frame.innerHTML = "";
    if (typeof qrcode !== "function") {
        frame.innerHTML = '<div class="qr-fallback">QR library could not load. Use the room code or link below.</div>';
        return;
    }
    try {
        var qr = qrcode(0, "M");
        qr.addData(link);
        qr.make();
        frame.innerHTML = qr.createSvgTag({ cellSize: 6, margin: 0, scalable: true });
        var svg = frame.querySelector("svg");
        if (svg) {
            svg.setAttribute("width", "216");
            svg.setAttribute("height", "216");
            svg.setAttribute("role", "img");
            svg.setAttribute("aria-label", "QR code to join room " + currentRoom);
        }
    } catch (e) {
        frame.innerHTML = '<div class="qr-fallback">Could not build QR code. Use the room code or link below.</div>';
    }
}

function populateShareModal() {
    var link = buildShareLink(currentRoom);
    document.getElementById("share-code").textContent = currentRoom;
    document.getElementById("share-link").textContent = link;
    renderQr(link);
    document.getElementById("native-share-btn").style.display = (navigator.share ? "inline-flex" : "none");
}

function openShareModal() {
    if (!socket || !socket.connected) {
        showToast("Still connecting, try again in a moment");
        return;
    }
    if (currentRoom === "Lobby") {
        pendingShareOpen = true;
        socket.emit("create_room_code");
        return;
    }
    populateShareModal();
    document.getElementById("share-modal").style.display = "flex";
}

function closeShareModal() {
    document.getElementById("share-modal").style.display = "none";
}

function copyShareLink(btn) {
    var link = buildShareLink(currentRoom);
    copyToClipboard(link, btn, "Copied!");
}

function nativeShare() {
    if (!navigator.share) return;
    navigator.share({
        title: "Join my PairMe room",
        text: "Join room " + currentRoom + " on PairMe to share files with me.",
        url: buildShareLink(currentRoom)
    }).catch(function() {});
}

function renderFormattedContent(text, textId) {
    if (!text) return "";
    textStore[textId] = text;
    var codeBlocks = [];
    var inlineCodes = [];
    var placeholderText = text.replace(/```([\s\S]*?)```/g, function(match, code) {
        codeBlocks.push(code);
        return "___CODE_BLOCK_" + (codeBlocks.length - 1) + "___";
    });
    placeholderText = placeholderText.replace(/`([^`]+)`/g, function(match, code) {
        inlineCodes.push(code);
        return "___INLINE_CODE_" + (inlineCodes.length - 1) + "___";
    });
    var escaped = escapeHtml(placeholderText);
    escaped = escaped.replace(/___INLINE_CODE_(\d+)___/g, function(match, index) {
        return '<code class="inline-code">' + escapeHtml(inlineCodes[index]) + '</code>';
    });
    escaped = escaped.replace(/___CODE_BLOCK_(\d+)___/g, function(match, index) {
        var cleanCode = escapeHtml(codeBlocks[index]);
        return '<div class="code-wrapper">' +
                    '<div class="code-header">' +
                        '<span>Code Block</span>' +
                        '<button class="copy-btn" onclick="copyCodeBlock(\'' + textId + '\', ' + index + ', this)">Copy Code</button>' +
                    '</div>' +
                    '<pre class="code-block"><code>' + cleanCode + '</code></pre>' +
               '</div>';
    });
    escaped = escaped.replace(/(https?:\/\/[^\s<]+)/g, function(url) {
        return '<a href="' + url + '" target="_blank" rel="noopener" class="text-link">' + url + '</a>';
    });
    return escaped;
}

function copyFullText(textId, btnElement) {
    var rawText = textStore[textId] || "";
    copyToClipboard(rawText, btnElement, "Copied!");
}

function copyCodeBlock(textId, codeIndex, btnElement) {
    var rawText = textStore[textId] || "";
    var codeBlocks = [];
    rawText.replace(/```([\s\S]*?)```/g, function(match, code) {
        codeBlocks.push(code);
    });
    var targetCode = codeBlocks[codeIndex] || "";
    copyToClipboard(targetCode, btnElement, "Copied!");
}

function copyToClipboard(str, btnElement, msg) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(str).then(function() {
            showCopySuccess(btnElement, msg);
        }).catch(function() {
            fallbackCopy(str, btnElement, msg);
        });
    } else {
        fallbackCopy(str, btnElement, msg);
    }
}

function fallbackCopy(str, btnElement, msg) {
    var textArea = document.createElement("textarea");
    textArea.value = str;
    textArea.style.position = "fixed";
    textArea.style.opacity = "0";
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    try {
        document.execCommand('copy');
        showCopySuccess(btnElement, msg);
    } catch (err) {}
    document.body.removeChild(textArea);
}

function showCopySuccess(btnElement, msg) {
    var originalText = btnElement.textContent;
    var originalBg = btnElement.style.background;
    var originalColor = btnElement.style.color;
    btnElement.textContent = msg;
    btnElement.style.background = "#16a34a";
    btnElement.style.color = "#ffffff";
    setTimeout(function() {
        btnElement.textContent = originalText;
        btnElement.style.background = originalBg;
        btnElement.style.color = originalColor;
    }, 1500);
}

function toggleExpand(blockId, btnElement) {
    var el = document.getElementById(blockId);
    if (!el) return;
    if (el.classList.contains("expanded")) {
        el.classList.remove("expanded");
        btnElement.textContent = "Show More";
    } else {
        el.classList.add("expanded");
        btnElement.textContent = "Show Less";
    }
}

function checkAutoExpand(blockId, overlayId) {
    setTimeout(function() {
        var el = document.getElementById(blockId);
        var overlay = document.getElementById(overlayId);
        if (el && overlay) {
            if (el.scrollHeight <= 230) {
                overlay.style.display = "none";
                el.style.maxHeight = "none";
            }
        }
    }, 50);
}

function generateTransferId() {
    return Date.now() + "_" + Math.floor(Math.random() * 100000);
}

function timeStamp() {
    var d = new Date();
    function p(n) { return ("0" + n).slice(-2); }
    return d.getFullYear() + p(d.getMonth() + 1) + p(d.getDate()) + "-" + p(d.getHours()) + p(d.getMinutes()) + p(d.getSeconds());
}

function formatEta(seconds) {
    if (!isFinite(seconds) || seconds < 0) return "";
    if (seconds < 60) return Math.ceil(seconds) + "s";
    return Math.floor(seconds / 60) + "m " + Math.ceil(seconds % 60) + "s";
}

function extFromType(type) {
    var map = { "image/png": "png", "image/jpeg": "jpg", "image/webp": "webp", "image/gif": "gif", "image/bmp": "bmp", "image/avif": "avif", "image/heic": "heic" };
    return map[type] || "bin";
}

function normalizePastedFile(file, index) {
    var generic = !file.name || /^(image|clip|blob)(\.[a-z0-9]+)?$/i.test(file.name);
    if (!generic) return file;
    var suffix = index ? "-" + (index + 1) : "";
    return new File([file], "pasted-" + timeStamp() + suffix + "." + extFromType(file.type), { type: file.type });
}

function stageFiles(files) {
    files.forEach(function(file) {
        var named = normalizePastedFile(file, stagedFiles.length);
        stagedFiles.push({ file: named, url: isImageType(named.type) ? URL.createObjectURL(named) : "" });
    });
    renderStageTray();
    if (window.innerWidth <= 768) switchTab("transfer");
    showToast(stagedFiles.length + " ready. Press Enter to send");
}

function unstageFile(index) {
    var entry = stagedFiles[index];
    if (!entry) return;
    if (entry.url) URL.revokeObjectURL(entry.url);
    stagedFiles.splice(index, 1);
    renderStageTray();
}

function clearStaged() {
    stagedFiles.forEach(function(entry) {
        if (entry.url) URL.revokeObjectURL(entry.url);
    });
    stagedFiles = [];
    renderStageTray();
}

function sendStaged() {
    if (!stagedFiles.length) return;
    if (!peerList.length) {
        showToast("No devices yet. Tap QR to invite one.");
        return;
    }
    var files = stagedFiles.map(function(entry) { return entry.file; });
    clearStaged();
    sendFiles(files);
}

function renderStageTray() {
    var tray = document.getElementById("stage-tray");
    if (!stagedFiles.length) {
        tray.style.display = "none";
        tray.innerHTML = "";
        return;
    }
    var total = stagedFiles.reduce(function(sum, entry) { return sum + entry.file.size; }, 0);
    var html = '<div class="stage-list">';
    stagedFiles.forEach(function(entry, i) {
        html += '<div class="stage-item" title="' + escapeHtml(entry.file.name) + '">' +
            (entry.url ? '<img src="' + entry.url + '" alt="">' : '<span>' + escapeHtml(fileExtLabel(entry.file.name, entry.file.type)) + '</span>') +
            '<button type="button" class="stage-remove" aria-label="Remove" onclick="unstageFile(' + i + ')">&times;</button>' +
        '</div>';
    });
    html += '</div><div class="stage-actions"><span>' + stagedFiles.length + ' ready · ' + formatBytes(total) + '</span>' +
        '<span class="row"><button type="button" class="action-btn" onclick="clearStaged()">Clear</button>' +
        '<button type="button" class="action-btn primary" onclick="sendStaged()">Send</button></span></div>';
    tray.innerHTML = html;
    tray.style.display = "flex";
}

function applyClipboardText(text) {
    var input = document.getElementById("text-input");
    input.value = input.value ? input.value + "\n" + text : text;
    input.focus();
}

function readClipboardButton() {
    if (!navigator.clipboard) {
        showToast("Clipboard not available");
        return;
    }
    if (!navigator.clipboard.read) {
        navigator.clipboard.readText().then(applyClipboardText).catch(function() {
            showToast("Clipboard permission denied");
        });
        return;
    }
    navigator.clipboard.read().then(function(items) {
        return Promise.all(items.map(function(item) {
            var imageType = item.types.filter(function(t) { return t.indexOf("image/") === 0; })[0];
            if (imageType) {
                return item.getType(imageType).then(function(blob) {
                    return { file: new File([blob], "image", { type: imageType }) };
                });
            }
            if (item.types.indexOf("text/plain") >= 0) {
                return item.getType("text/plain").then(function(blob) { return blob.text(); }).then(function(text) {
                    return { text: text };
                });
            }
            return null;
        }));
    }).then(function(results) {
        var files = [];
        var text = "";
        results.forEach(function(r) {
            if (!r) return;
            if (r.file) files.push(r.file);
            else if (r.text) text += r.text;
        });
        if (files.length) stageFiles(files);
        if (text) applyClipboardText(text);
        if (!files.length && !text) showToast("Clipboard is empty");
    }).catch(function() {
        showToast("Clipboard permission denied");
    });
}

function releaseItem(item) {
    [item.url, item.jpegUrl].forEach(function(u) {
        if (u) {
            try { URL.revokeObjectURL(u); } catch (e) {}
        }
    });
    if (item._mediaId) delete mediaRegistry[item._mediaId];
}

function removeFeedItem(li) {
    var items = li._getItems ? li._getItems() : [];
    items.forEach(releaseItem);
    if (li.dataset.textId) delete textStore[li.dataset.textId];
    var batchId = li.dataset.batchId;
    if (batchId && batchStore[batchId]) {
        var batch = batchStore[batchId];
        if (batch.renderTimer) clearTimeout(batch.renderTimer);
        (batch.unitIds || []).forEach(function(id) { delete unitRegistry[id]; });
        delete batchStore[batchId];
    }
    if (li.parentNode) li.parentNode.removeChild(li);
}

function clearReceived() {
    var list = document.getElementById("received-list");
    Array.prototype.slice.call(list.children).forEach(removeFeedItem);
}

function decorateFeedItem(li, getItems) {
    var header = li.querySelector(".feed-header");
    var time = li.querySelector(".feed-time");
    if (!header || !time) return;
    var wrap = document.createElement("span");
    wrap.className = "feed-right";
    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "feed-remove";
    btn.setAttribute("aria-label", "Remove");
    btn.innerHTML = "&times;";
    btn.onclick = function() { removeFeedItem(li); };
    header.replaceChild(wrap, time);
    wrap.appendChild(time);
    wrap.appendChild(btn);
    li._getItems = getItems;
}

function playPing() {
    try {
        var AC = window.AudioContext || window.webkitAudioContext;
        if (!AC) return;
        audioCtx = audioCtx || new AC();
        if (audioCtx.state === "suspended") audioCtx.resume();
        var now = audioCtx.currentTime;
        var osc = audioCtx.createOscillator();
        var gain = audioCtx.createGain();
        osc.type = "sine";
        osc.frequency.setValueAtTime(880, now);
        osc.frequency.exponentialRampToValueAtTime(1320, now + 0.12);
        gain.gain.setValueAtTime(0.0001, now);
        gain.gain.exponentialRampToValueAtTime(0.12, now + 0.02);
        gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.22);
        osc.connect(gain);
        gain.connect(audioCtx.destination);
        osc.start(now);
        osc.stop(now + 0.25);
    } catch (e) {}
}

function toggleNotify(on) {
    notifyEnabled = !!on;
    localStorage.setItem("pairme_notify", on ? "1" : "0");
    if (on && window.Notification && Notification.permission === "default") {
        try { Notification.requestPermission(); } catch (e) {}
    }
}

function notifyIncoming(sender, label) {
    if (notifyEnabled) playPing();
    if (!document.hidden) return;
    unreadCount++;
    document.title = "(" + unreadCount + ") " + BASE_TITLE;
    if (notifyEnabled && window.Notification && Notification.permission === "granted") {
        try { new Notification("PairMe", { body: sender + ": " + label, tag: "pairme" }); } catch (e) {}
    }
}

function randomHex(byteCount) {
    var bytes = new Uint8Array(byteCount);
    if (window.crypto && window.crypto.getRandomValues) {
        window.crypto.getRandomValues(bytes);
    } else {
        for (var i = 0; i < byteCount; i++) bytes[i] = Math.floor(Math.random() * 256);
    }
    var out = "";
    for (var j = 0; j < bytes.length; j++) out += ("0" + bytes[j].toString(16)).slice(-2);
    return out;
}

function getClientUid() {
    var uid = null;
    try { uid = sessionStorage.getItem("pairme_uid"); } catch (e) {}
    if (!uid || !/^[a-f0-9]{32}$/.test(uid)) {
        uid = memoryUid || randomHex(16);
        try { sessionStorage.setItem("pairme_uid", uid); } catch (e) {}
    }
    memoryUid = uid;
    return uid;
}

function initSocket() {
    socket = io({
        transports: ["websocket", "polling"],
        query: { fp: getClientUid() },
        reconnection: true,
        reconnectionAttempts: Infinity,
        reconnectionDelay: 1000,
        reconnectionDelayMax: 8000,
        randomizationFactor: 0.5,
        timeout: 20000
    });
    bindSocketEvents();
}

function dropPeerState(sid) {
    var pc = connections[sid];
    if (pc) {
        try { pc.close(); } catch (e) {}
    }
    resolveChannelWaiters(sid, false);
    delete connections[sid];
    delete peerConnState[sid];
    delete pendingFileQueue[sid];
    delete relayFileQueue[sid];
    delete p2pDraining[sid];
}

function closeAllConnections() {
    Object.keys(connections).forEach(dropPeerState);
}

function pruneDepartedPeers() {
    var alive = {};
    peerList.forEach(function(p) { alive[p.sid] = true; });
    Object.keys(connections).forEach(function(sid) {
        if (!alive[sid]) dropPeerState(sid);
    });
}

function bindSocketEvents() {
    socket.on("connect", function() {
        log("Connected to server", "success");
        var saved = localStorage.getItem("pairme_name");
        if (saved) {
            document.getElementById("my-name").value = saved;
            socket.emit("set_name", { name: saved });
        }
    });

    socket.on("init", function(data) {
        mySid = data.sid;
        myPeerId = data.peer_id;
        currentRoom = data.room || "Lobby";
        document.getElementById("my-id").textContent = myPeerId;
        document.getElementById("room-name").textContent = currentRoom;
        if (data.name) {
            document.getElementById("my-name").value = data.name;
        }
        log("ID: " + myPeerId, "info");

        if (urlRoomCode && urlRoomCode !== currentRoom) {
            log("Joining room from QR link: " + urlRoomCode, "info");
            socket.emit("join_room_code", { code: urlRoomCode });
        } else {
            syncUrlWithRoom(currentRoom);
        }
        urlRoomCode = null;
    });

    socket.on("peers", function(data) {
        peerList = (data || []).filter(function(p) { return p.sid !== mySid; });
        pruneDepartedPeers();
        renderPeers();
        peerList.forEach(function(p) {
            var state = peerConnState[p.sid];
            if (mySid < p.sid && !isDataChannelOpen(p.sid) && state !== "connecting" && state !== "p2p") {
                connectPeer(p.sid, false);
            }
        });
    });

    socket.on("signal", handleSignal);

    socket.on("transfer_request", function(data) {
        log("Incoming: " + data.file_name + " from " + data.from_name, "info");
        socket.emit("broadcast_response", {
            to: data.from,
            accepted: true,
            transfer_id: data.transfer_id
        });
    });

    socket.on("room_joined", function(data) {
        currentRoom = data.code;
        document.getElementById("room-name").textContent = data.code;
        syncUrlWithRoom(data.code);
        log("Joined room " + data.code, "success");
        closeAllConnections();
        if (pendingShareOpen) {
            pendingShareOpen = false;
            populateShareModal();
            document.getElementById("share-modal").style.display = "flex";
        } else if (document.getElementById("share-modal").style.display === "flex") {
            populateShareModal();
        }
    });

    socket.on("room_left", function() {
        currentRoom = "Lobby";
        document.getElementById("room-name").textContent = "Lobby";
        syncUrlWithRoom("Lobby");
        closeShareModal();
        log("Switched to Lobby", "info");
        closeAllConnections();
    });

    socket.on("room_error", function(data) {
        log("Room error: " + (data && data.msg ? data.msg : "unknown"), "error");
        showToast("Invalid room code");
    });

    socket.on("relay_text", function(data) {
        addReceived("text", data.text, data.from_name);
        log("Text from " + data.from_name, "info");
    });

    socket.on("relay_file_start", function(data) {
        data.lastActivity = Date.now();
        relayBuffer[data.transfer_id] = {};
        relayMeta[data.transfer_id] = data;
        log("Receiving " + data.file_name + (data.batch_total > 1 ? " (" + ((data.batch_index || 0) + 1) + "/" + data.batch_total + ")" : ""), "info");
    });

    socket.on("relay_file_chunk", function(data) {
        var store = relayBuffer[data.transfer_id];
        var meta = relayMeta[data.transfer_id];
        if (!store || !meta) return;
        var chunk = data.chunk;
        if (typeof chunk === "string") {
            try {
                var binary = atob(chunk);
                var bytes = new Uint8Array(binary.length);
                for (var i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
                chunk = bytes.buffer;
            } catch (e) {
                log("Chunk decode error", "error");
                return;
            }
        }
        store[data.seq] = chunk;
        meta.lastActivity = Date.now();
    });

    socket.on("relay_file_done", function(data) {
        var meta = relayMeta[data.transfer_id];
        var chunkMap = relayBuffer[data.transfer_id];
        delete relayMeta[data.transfer_id];
        delete relayBuffer[data.transfer_id];
        if (!meta || !chunkMap) return;
        var ordered = Object.keys(chunkMap).map(Number).sort(function(a, b) { return a - b; }).map(function(seq) { return chunkMap[seq]; });
        var blob = new Blob(ordered, { type: meta.file_type });
        if (meta.file_size && blob.size !== meta.file_size) {
            log("Size mismatch on " + meta.file_name + ", discarded", "error");
            return;
        }
        addReceived("file", {
            name: meta.file_name,
            size: meta.file_size,
            url: URL.createObjectURL(blob),
            type: meta.file_type,
            blob: blob,
            batch_id: meta.batch_id || "",
            batch_total: meta.batch_total || 1,
            batch_index: meta.batch_index || 0
        }, meta.from_name);
        log("Received " + meta.file_name + " (Relay)", "success");
    });

    socket.on("rate_limited", function(data) {
        log("Too many requests, slow down (" + data.event + ")", "warn");
    });

    socket.on("disconnect", function() {
        log("Disconnected from server", "warn");
    });
}

function isDataChannelOpen(sid) {
    var pc = connections[sid];
    return pc && pc.dataChannel && pc.dataChannel.readyState === "open";
}

function setPeerConnState(sid, state) {
    peerConnState[sid] = state;
    renderPeers();
}

function renderPeers() {
    var list = document.getElementById("peer-list");
    var select = document.getElementById("peer-select");
    var selected = select.value;
    list.innerHTML = "";
    select.innerHTML = '<option value="">-- All Devices --</option>';

    if (peerList.length === 0) {
        list.innerHTML = '<div class="empty-hint">No devices detected</div>';
        return;
    }

    peerList.forEach(function(p) {
        var state = peerConnState[p.sid] || (isDataChannelOpen(p.sid) ? "p2p" : "relay");
        var statusHtml = "";
        if (state === "p2p") statusHtml = '<span class="peer-status status-p2p">P2P</span>';
        else if (state === "connecting") statusHtml = '<span class="peer-status status-conn">…</span>';
        else statusHtml = '<span class="peer-status status-relay">Relay</span>';

        var item = document.createElement("div");
        item.className = "peer-item";
        item.onclick = function() { selectPeer(p.sid); };
        item.innerHTML = '<div class="peer-info"><span class="peer-name">' + escapeHtml(p.name) + '</span><span class="peer-id">' + p.id + '</span></div>' +
            '<div style="display:flex;align-items:center;gap:6px;">' + statusHtml +
            '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg></div>';
        list.appendChild(item);

        var opt = document.createElement("option");
        opt.value = p.sid;
        opt.textContent = p.name + " (" + p.id + ")";
        select.appendChild(opt);
    });
    if (selected) select.value = selected;
}

function selectPeer(sid) {
    document.getElementById("peer-select").value = sid;
    onPeerSelectChange();
    if (window.innerWidth <= 768) {
        switchTab("transfer");
    }
}

function onPeerSelectChange() {
    var sid = document.getElementById("peer-select").value;
    var label = document.getElementById("target-peer-label");
    if (sid) {
        var p = peerList.find(function(x) { return x.sid === sid; });
        label.textContent = "To: " + (p ? p.name : sid);
        connectPeer(sid, true);
    } else {
        label.textContent = "To: Everyone";
    }
}

function updateName() {
    var name = document.getElementById("my-name").value.trim();
    if (name) {
        socket.emit("set_name", { name: name });
        localStorage.setItem("pairme_name", name);
        log("Updated name: " + name, "success");
    }
}

function joinRoom() {
    var code = document.getElementById("room-code-input").value.trim();
    if (code.length === 6) socket.emit("join_room_code", { code: code });
}

function createRoom() { socket.emit("create_room_code"); }
function leaveRoom() { socket.emit("leave_room_code"); }

function getOrCreateConnection(targetSid, isInitiator) {
    if (!WEBRTC_SUPPORTED) return null;
    if (connections[targetSid] && connections[targetSid].connectionState !== "closed" && connections[targetSid].connectionState !== "failed") {
        return connections[targetSid];
    }

    if (connections[targetSid]) {
        try { connections[targetSid].close(); } catch(e){}
    }

    var pc = new RTCPeerConnection(STUN_SERVERS);
    pc.iceQueue = [];
    pc.targetSid = targetSid;
    pc.receiveBuffer = {};
    pc._startedAt = Date.now();
    pc._makingOffer = false;
    pc._ignoreOffer = false;
    pc._polite = (mySid < targetSid);
    setPeerConnState(targetSid, "connecting");

    pc.onicecandidate = function(e) {
        if (e.candidate) {
            socket.emit("signal", { to: targetSid, signal: { type: "ice", candidate: e.candidate } });
        }
    };

    pc.onconnectionstatechange = function() {
        var st = pc.connectionState;
        if (st === "connected") {
            if (isDataChannelOpen(targetSid)) setPeerConnState(targetSid, "p2p");
        } else if (st === "failed" || st === "disconnected") {
            log("P2P state " + st + " with " + targetSid.slice(0, 6), "warn");
            setPeerConnState(targetSid, "failed");
            if (!pc._restarted) {
                pc._restarted = true;
                try {
                    pc.restartIce();
                    if (isInitiator || pc._polite) {
                        makeOffer(pc, targetSid);
                    }
                } catch (err) {}
            }
        } else if (st === "closed") {
            setPeerConnState(targetSid, "relay");
        }
    };

    pc.oniceconnectionstatechange = function() {
        if (pc.iceConnectionState === "failed") {
            setPeerConnState(targetSid, "failed");
        }
    };

    pc.ondatachannel = function(e) {
        setupDataChannel(pc, e.channel, targetSid);
    };

    if (isInitiator) {
        var channel = pc.createDataChannel("pairme", { ordered: true, negotiated: false });
        setupDataChannel(pc, channel, targetSid);
    }

    connections[targetSid] = pc;
    return pc;
}

function setupDataChannel(pc, channel, targetSid) {
    pc.dataChannel = channel;
    channel.binaryType = "arraybuffer";
    try { channel.bufferedAmountLowThreshold = P2P_LOW_WATER; } catch (e) {}

    channel.onopen = function() {
        log("P2P open with " + targetSid.slice(0, 6), "p2p");
        setPeerConnState(targetSid, "p2p");
        resolveChannelWaiters(targetSid, true);
    };

    channel.onclose = function() {
        pc.receiveBuffer = {};
        pc.activeMeta = null;
        log("P2P closed " + targetSid.slice(0, 6), "warn");
        if (connections[targetSid] === pc) setPeerConnState(targetSid, "relay");
    };

    channel.onerror = function(err) {
        log("Channel error: " + (err.message || err), "error");
        if (connections[targetSid] === pc) setPeerConnState(targetSid, "failed");
    };

    channel.onmessage = function(e) {
        handleDataMessage(e.data, targetSid);
    };
}

function makeOffer(pc, targetSid) {
    if (pc._makingOffer) return;
    pc._makingOffer = true;
    pc.createOffer()
        .then(function(offer) { return pc.setLocalDescription(offer); })
        .then(function() {
            socket.emit("signal", { to: targetSid, signal: { type: "offer", sdp: pc.localDescription } });
        })
        .catch(function(err) { log("Offer err: " + err.message, "error"); })
        .finally(function() { pc._makingOffer = false; });
}

function handleSignal(data) {
    var fromSid = data.from;
    var signal = data.signal;
    var pc = getOrCreateConnection(fromSid, false);

    if (!pc) return;

    if (signal.type === "offer") {
        var offerCollision = (pc._makingOffer || pc.signalingState !== "stable");
        pc._ignoreOffer = !pc._polite && offerCollision;
        if (pc._ignoreOffer) {
            log("Ignoring colliding offer (impolite)", "info");
            return;
        }

        var doRollback = offerCollision && pc._polite;
        var p = Promise.resolve();
        if (doRollback) {
            p = pc.setLocalDescription({ type: "rollback" }).catch(function(){});
        }

        p.then(function() {
            return pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
        })
        .then(function() {
            while (pc.iceQueue.length) {
                pc.addIceCandidate(pc.iceQueue.shift()).catch(function(){});
            }
            return pc.createAnswer();
        })
        .then(function(ans) { return pc.setLocalDescription(ans); })
        .then(function() {
            socket.emit("signal", { to: fromSid, signal: { type: "answer", sdp: pc.localDescription } });
        })
        .catch(function(err) { log("Offer/answer err: " + err.message, "error"); });
    } else if (signal.type === "answer") {
        if (pc.signalingState !== "have-local-offer") {
            return;
        }
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .then(function() {
                while (pc.iceQueue.length) {
                    pc.addIceCandidate(pc.iceQueue.shift()).catch(function(){});
                }
            })
            .catch(function(err) {
                if (String(err.message || err).indexOf("stable") === -1) {
                    log("Answer err: " + err.message, "error");
                }
            });
    } else if (signal.type === "ice") {
        var candidate = new RTCIceCandidate(signal.candidate);
        if (pc.remoteDescription && pc.remoteDescription.type) {
            pc.addIceCandidate(candidate).catch(function(){});
        } else {
            pc.iceQueue.push(candidate);
        }
    }
}

function connectPeer(targetSid, force) {
    if (!WEBRTC_SUPPORTED) {
        if (force) log("WebRTC not supported, using relay only", "warn");
        return;
    }
    if (isDataChannelOpen(targetSid)) return;
    var existing = connections[targetSid];
    if (existing && peerConnState[targetSid] === "connecting" && Date.now() - existing._startedAt < P2P_STALE_MS) return;
    var pc = getOrCreateConnection(targetSid, true);
    pc._polite = (mySid < targetSid);
    makeOffer(pc, targetSid);
}

function sendText() {
    var input = document.getElementById("text-input");
    var text = input.value.trim();
    if (!text) return;
    if (text.length > TEXT_INLINE_LIMIT) {
        sendFiles([new File([text], "message-" + timeStamp() + ".txt", { type: "text/plain" })]);
        input.value = "";
        return;
    }
    var targetSid = document.getElementById("peer-select").value;

    if (targetSid) {
        sendTextTo(targetSid, text);
    } else {
        peerList.forEach(function(p) { sendTextTo(p.sid, text); });
    }
    input.value = "";
}

function sendTextTo(targetSid, text) {
    if (isDataChannelOpen(targetSid)) {
        try {
            connections[targetSid].dataChannel.send(JSON.stringify({ t: "txt", c: text }));
            log("Sent text (P2P)", "p2p");
            return;
        } catch (e) {}
    }
    var pInfo = peerList.find(function(x) { return x.sid === targetSid; });
    socket.emit("relay_text", { to: (pInfo ? pInfo.id : targetSid), text: text });
    log("Sent text (Relay)", "info");
}

function sendFiles(fileArr) {
    if (!fileArr.length) return;
    if (!peerList.length) {
        showToast("No devices yet. Tap QR to invite one.");
        log("No devices to send to", "warn");
        return;
    }
    var targetSid = document.getElementById("peer-select").value;
    var batchId = "b_" + generateTransferId();
    var total = fileArr.length;

    fileArr.forEach(function(file, idx) {
        if (targetSid) {
            sendFileTo(targetSid, file, batchId, total, idx);
        } else {
            peerList.forEach(function(p) { sendFileTo(p.sid, file, batchId, total, idx); });
        }
    });
    if (total > 1) log("Queued batch of " + total + " files", "info");
}

function handleFileSelect(e) {
    var files = e.target.files || (e.dataTransfer && e.dataTransfer.files);
    if (!files || !files.length) return;
    var fileArr = Array.prototype.slice.call(files);
    if (e.target && "value" in e.target) e.target.value = "";
    sendFiles(fileArr);
}

function readBlobBuffer(blob) {
    if (blob.arrayBuffer) return blob.arrayBuffer();
    return new Promise(function(resolve, reject) {
        var reader = new FileReader();
        reader.onload = function() { resolve(reader.result); };
        reader.onerror = function() { reject(reader.error); };
        reader.readAsArrayBuffer(blob);
    });
}

function setProgress(pct) {
    var now = performance.now();
    if (pct < 100 && now - lastProgressPaint < 80) return;
    lastProgressPaint = now;
    var detail = "";
    if (transferClock.total && pct > 0 && pct < 100) {
        var elapsed = (now - transferClock.start) / 1000;
        if (elapsed > 0.4) {
            var done = transferClock.total * pct / 100;
            var speed = done / elapsed;
            detail = " · " + formatBytes(speed) + "/s · " + formatEta((transferClock.total - done) / speed);
        }
    }
    document.getElementById("progress-fill").style.width = pct.toFixed(1) + "%";
    document.getElementById("send-pct").textContent = Math.round(pct) + "%" + detail;
}

function beginTransfer(label, totalBytes) {
    activeTransfers++;
    updateTransferBadge();
    document.getElementById("progress-wrap").style.display = "block";
    document.getElementById("send-status").textContent = label;
    transferClock = { start: performance.now(), total: totalBytes || 0 };
    lastProgressPaint = 0;
    setProgress(0);
}

function endTransfer() {
    activeTransfers = Math.max(0, activeTransfers - 1);
    updateTransferBadge();
    setTimeout(function() {
        if (activeTransfers === 0) document.getElementById("progress-wrap").style.display = "none";
    }, 600);
}

function batchLabel(item) {
    return item.batch_total > 1 ? " (" + (item.batch_index + 1) + "/" + item.batch_total + ")" : "";
}

function waitForChannel(targetSid, timeoutMs) {
    return new Promise(function(resolve) {
        if (isDataChannelOpen(targetSid)) {
            resolve(true);
            return;
        }
        var waiter = { resolve: resolve, timer: null };
        waiter.timer = setTimeout(function() {
            var list = p2pWaiters[targetSid] || [];
            var idx = list.indexOf(waiter);
            if (idx >= 0) list.splice(idx, 1);
            resolve(false);
        }, timeoutMs);
        if (!p2pWaiters[targetSid]) p2pWaiters[targetSid] = [];
        p2pWaiters[targetSid].push(waiter);
    });
}

function resolveChannelWaiters(targetSid, ok) {
    var list = p2pWaiters[targetSid];
    if (!list) return;
    delete p2pWaiters[targetSid];
    list.forEach(function(w) {
        clearTimeout(w.timer);
        w.resolve(ok);
    });
}

function getP2PChunkSize(pc) {
    var limit = pc.sctp && pc.sctp.maxMessageSize ? pc.sctp.maxMessageSize : CHUNK_SIZE;
    return Math.min(P2P_MAX_CHUNK, limit);
}

function sendFileTo(targetSid, file, batchId, batchTotal, batchIndex) {
    var pInfo = peerList.find(function(x) { return x.sid === targetSid; });
    var item = {
        file: file,
        transfer_id: generateTransferId(),
        batch_id: batchId || ("b_" + generateTransferId()),
        batch_total: batchTotal || 1,
        batch_index: (typeof batchIndex === "number") ? batchIndex : 0,
        target_peer_id: pInfo ? pInfo.id : targetSid
    };
    if (isDataChannelOpen(targetSid)) {
        enqueueP2P(targetSid, item);
        return;
    }
    connectPeer(targetSid, true);
    waitForChannel(targetSid, P2P_CONNECT_TIMEOUT).then(function(open) {
        if (open) {
            enqueueP2P(targetSid, item);
        } else {
            log("P2P not ready, using Relay for " + file.name, "warn");
            enqueueRelay(targetSid, item);
        }
    });
}

function enqueueP2P(targetSid, item) {
    if (!pendingFileQueue[targetSid]) pendingFileQueue[targetSid] = [];
    pendingFileQueue[targetSid].push(item);
    drainP2PQueue(targetSid);
}

function enqueueRelay(targetSid, item) {
    var queue = relayFileQueue[targetSid];
    if (!queue) {
        queue = relayFileQueue[targetSid] = [];
        queue._active = 0;
    }
    queue.push(item);
    processRelayQueue(targetSid);
}

function drainP2PQueue(targetSid) {
    if (p2pDraining[targetSid]) return;
    var queue = pendingFileQueue[targetSid];
    if (!queue) return;
    p2pDraining[targetSid] = true;

    function next() {
        var item = queue[0];
        if (!item) {
            p2pDraining[targetSid] = false;
            return;
        }
        if (!isDataChannelOpen(targetSid)) {
            queue.shift();
            enqueueRelay(targetSid, item);
            next();
            return;
        }
        Promise.resolve().then(function() {
            return sendFileOverChannel(connections[targetSid], item);
        }).then(function() {
            queue.shift();
        }, function(err) {
            log("P2P send failed, switching to Relay: " + (err && err.message ? err.message : err), "warn");
            queue.shift();
            enqueueRelay(targetSid, item);
        }).then(next);
    }
    next();
}

function sendFileOverChannel(pc, item) {
    var channel = pc.dataChannel;
    var file = item.file;
    channel.send(JSON.stringify({
        t: "fs",
        n: file.name,
        s: file.size,
        m: file.type,
        id: item.transfer_id,
        bid: item.batch_id,
        bt: item.batch_total,
        bi: item.batch_index
    }));
    beginTransfer("P2P " + file.name + batchLabel(item), file.size);
    return pumpFileToChannel(channel, file, getP2PChunkSize(pc), setProgress).then(function() {
        channel.send(JSON.stringify({ t: "fe", id: item.transfer_id }));
        endTransfer();
        log("Sent " + file.name + " (P2P)", "success");
    }, function(err) {
        endTransfer();
        throw err;
    });
}

function pumpFileToChannel(channel, file, chunkSize, onProgress) {
    return new Promise(function(resolve, reject) {
        var offset = 0;
        var block = null;
        var blockPos = 0;
        var loading = false;
        var settled = false;

        function settle(err) {
            if (settled) return;
            settled = true;
            channel.onbufferedamountlow = null;
            channel.removeEventListener("close", onClose);
            if (err) reject(err);
            else resolve();
        }

        function onClose() {
            settle(new Error("Channel closed"));
        }

        channel.addEventListener("close", onClose);

        function loadBlock() {
            loading = true;
            readBlobBuffer(file.slice(offset, Math.min(offset + READ_BLOCK, file.size))).then(function(buffer) {
                block = buffer;
                blockPos = 0;
                loading = false;
                pump();
            }).catch(settle);
        }

        function pump() {
            if (settled || loading) return;
            try {
                while (offset < file.size) {
                    if (channel.readyState !== "open") {
                        settle(new Error("Channel closed"));
                        return;
                    }
                    if (!block || blockPos >= block.byteLength) {
                        block = null;
                        loadBlock();
                        return;
                    }
                    if (channel.bufferedAmount > P2P_HIGH_WATER) {
                        channel.onbufferedamountlow = function() {
                            channel.onbufferedamountlow = null;
                            pump();
                        };
                        return;
                    }
                    var end = Math.min(blockPos + chunkSize, block.byteLength);
                    channel.send(new Uint8Array(block, blockPos, end - blockPos));
                    offset += end - blockPos;
                    blockPos = end;
                    onProgress(offset / file.size * 100);
                }
                settle();
            } catch (err) {
                settle(err);
            }
        }

        pump();
    });
}

function processRelayQueue(targetSid) {
    var queue = relayFileQueue[targetSid];
    if (!queue) return;
    while (queue._active < RELAY_BATCH_CONCURRENCY && queue.length) {
        var item = queue.shift();
        queue._active++;
        relaySendFile(targetSid, item.target_peer_id, item.file, item.transfer_id, item.batch_id, item.batch_total, item.batch_index, function() {
            queue._active--;
            processRelayQueue(targetSid);
        });
    }
}

function relaySendFile(targetSid, targetPeerId, file, transferId, batchId, batchTotal, batchIndex, onComplete) {
    socket.emit("relay_file_start", {
        to: targetPeerId,
        file_name: file.name,
        file_size: file.size,
        file_type: file.type,
        transfer_id: transferId,
        batch_id: batchId || "",
        batch_total: batchTotal || 1,
        batch_index: batchIndex || 0
    });
    beginTransfer("Relay " + file.name + batchLabel({ batch_total: batchTotal || 1, batch_index: batchIndex || 0 }), file.size);

    var offset = 0;
    var seq = 0;
    var inFlight = 0;
    var acked = 0;
    var windowSize = RELAY_WINDOW_START;
    var block = null;
    var blockPos = 0;
    var loading = false;
    var closed = false;

    function finish(ok) {
        if (closed) return;
        closed = true;
        endTransfer();
        if (ok) {
            socket.emit("relay_file_done", { to: targetPeerId, transfer_id: transferId });
            log("Sent " + file.name + " (Relay)", "success");
        } else {
            log("Giving up on " + file.name + " after repeated failures", "error");
        }
        if (onComplete) onComplete();
    }

    function sendChunk(seqNo, payload, attempt) {
        if (closed) return;
        socket.timeout(RELAY_ACK_TIMEOUT).emit("relay_file_chunk", {
            to: targetPeerId,
            transfer_id: transferId,
            seq: seqNo,
            chunk: payload
        }, function(err, ack) {
            if (closed) return;
            if (!err && ack === true) {
                inFlight--;
                acked += payload.byteLength;
                windowSize = Math.min(RELAY_WINDOW_MAX, windowSize + 1);
                setProgress(file.size ? acked / file.size * 100 : 100);
                pump();
                return;
            }
            if (attempt >= RELAY_MAX_RETRIES) {
                finish(false);
                return;
            }
            windowSize = Math.max(RELAY_WINDOW_MIN, windowSize >> 1);
            setTimeout(function() { sendChunk(seqNo, payload, attempt + 1); }, 100 * (attempt + 1));
        });
    }

    function loadBlock() {
        loading = true;
        readBlobBuffer(file.slice(offset, Math.min(offset + READ_BLOCK, file.size))).then(function(buffer) {
            block = buffer;
            blockPos = 0;
            loading = false;
            pump();
        }).catch(function() {
            loading = false;
            finish(false);
        });
    }

    function pump() {
        if (closed) return;
        while (inFlight < windowSize && offset < file.size) {
            if (!block || blockPos >= block.byteLength) {
                if (!loading) loadBlock();
                return;
            }
            var end = Math.min(blockPos + RELAY_CHUNK, block.byteLength);
            var payload = block.slice(blockPos, end);
            blockPos = end;
            offset += payload.byteLength;
            inFlight++;
            sendChunk(seq++, payload, 0);
        }
        if (offset >= file.size && inFlight === 0) finish(true);
    }

    pump();
}

function handleDataMessage(data, fromSid) {
    var pc = connections[fromSid];
    if (!pc) return;
    if (typeof data !== "string") {
        var active = pc.activeMeta;
        if (active && pc.receiveBuffer[active.id]) pc.receiveBuffer[active.id].push(data);
        return;
    }
    var msg;
    try {
        msg = JSON.parse(data);
    } catch (e) {
        log("Bad data message", "error");
        return;
    }
    var senderName = (peerList.find(function(p) { return p.sid === fromSid; }) || {}).name || fromSid.slice(0, 6);
    if (msg.t === "txt") {
        addReceived("text", msg.c, senderName);
    } else if (msg.t === "fs") {
        pc.receiveBuffer = {};
        pc.activeMeta = msg;
        pc.receiveBuffer[msg.id] = [];
    } else if (msg.t === "fe") {
        var buffers = pc.receiveBuffer[msg.id];
        var meta = pc.activeMeta;
        if (!buffers || !meta || meta.id !== msg.id) return;
        var blob = new Blob(buffers, { type: meta.m });
        delete pc.receiveBuffer[msg.id];
        pc.activeMeta = null;
        if (meta.s && blob.size !== meta.s) {
            log("Size mismatch on " + meta.n + ", discarded", "error");
            return;
        }
        addReceived("file", {
            name: meta.n,
            size: meta.s,
            url: URL.createObjectURL(blob),
            type: meta.m,
            blob: blob,
            batch_id: meta.bid || "",
            batch_total: meta.bt || 1,
            batch_index: meta.bi || 0
        }, senderName);
        log("Received " + meta.n + " (P2P)", "success");
    }
}

function updateTransferBadge() {
    var badge = document.getElementById("transfer-badge");
    if (activeTransfers > 0) {
        badge.style.display = "inline-block";
        badge.textContent = "Transferring " + activeTransfers;
    } else {
        badge.style.display = "none";
    }
}

function guessMimeFromName(name) {
    if (!name) return "";
    var ext = (name.split(".").pop() || "").toLowerCase();
    var map = {
        heic: "image/heic", heif: "image/heif",
        jpg: "image/jpeg", jpeg: "image/jpeg", jpe: "image/jpeg",
        png: "image/png", gif: "image/gif", webp: "image/webp",
        avif: "image/avif", bmp: "image/bmp", tif: "image/tiff", tiff: "image/tiff",
        mp4: "video/mp4", webm: "video/webm", mov: "video/quicktime", m4v: "video/x-m4v",
        mkv: "video/x-matroska", avi: "video/x-msvideo",
        mp3: "audio/mpeg", m4a: "audio/mp4", aac: "audio/aac",
        wav: "audio/wav", ogg: "audio/ogg", oga: "audio/ogg",
        flac: "audio/flac", opus: "audio/opus", caf: "audio/x-caf",
        pdf: "application/pdf", zip: "application/zip", txt: "text/plain"
    };
    return map[ext] || "";
}

function normalizeItemMime(item) {
    if (!item.type || item.type === "application/octet-stream" || item.type === "") {
        var guessed = guessMimeFromName(item.name);
        if (guessed) item.type = guessed;
    }
    return item;
}

function isHeicType(type, name) {
    var t = (type || "").toLowerCase();
    if (t.indexOf("heic") >= 0 || t.indexOf("heif") >= 0) return true;
    var n = (name || "").toLowerCase();
    return n.endsWith(".heic") || n.endsWith(".heif");
}

function isImageType(type) { return !!(type && type.indexOf("image/") === 0); }
function isVideoType(type) { return !!(type && type.indexOf("video/") === 0); }
function isAudioType(type) { return !!(type && type.indexOf("audio/") === 0); }
function isMediaType(type) { return isImageType(type) || isVideoType(type); }

function fileExtLabel(name, type) {
    if (name && name.indexOf(".") > -1) {
        var ext = name.split(".").pop().toUpperCase();
        if (ext.length <= 5) return ext;
    }
    if (type) {
        if (type.indexOf("pdf") >= 0) return "PDF";
        if (type.indexOf("zip") >= 0 || type.indexOf("compressed") >= 0) return "ZIP";
        if (type.indexOf("audio") === 0) return "AUD";
        if (type.indexOf("text") === 0) return "TXT";
        if (type.indexOf("heic") >= 0 || type.indexOf("heif") >= 0) return "HEIC";
    }
    return "FILE";
}

function baseName(name) {
    var n = name || "file";
    var dot = n.lastIndexOf(".");
    return dot > 0 ? n.slice(0, dot) : n;
}

function swapExt(name, newExt) {
    return baseName(name) + "." + newExt;
}

function isStillImage(item) {
    return isImageType(item.type) || isHeicType(item.type, item.name);
}

function isMovItem(item) {
    if (isVideoType(item.type)) {
        var n = (item.name || "").toLowerCase();
        return n.endsWith(".mov") || item.type === "video/quicktime";
    }
    return false;
}

function registerMedia(item) {
    if (item._mediaId && mediaRegistry[item._mediaId]) return item._mediaId;
    var id = "m_" + generateTransferId() + "_" + Math.floor(Math.random() * 1000);
    item._mediaId = id;
    mediaRegistry[id] = item;
    return id;
}

var libheifInstance = null;
function getLibheif() {
    if (libheifInstance) return libheifInstance;
    if (typeof libheif === "undefined") return null;
    try {
        libheifInstance = libheif();
    } catch (e) {
        libheifInstance = null;
    }
    return libheifInstance;
}

function convertHeicViaLibheifJs(blob) {
    return new Promise(function(resolve, reject) {
        var lh = getLibheif();
        if (!lh) { reject(new Error("libheif-js not available")); return; }
        blob.arrayBuffer().then(function(buf) {
            var decoder = new lh.HeifDecoder();
            var data;
            try {
                data = decoder.decode(new Uint8Array(buf));
            } catch (e) {
                reject(e);
                return;
            }
            if (!data || !data.length) { reject(new Error("No image data decoded")); return; }
            var image = data[0];
            var w = image.get_width();
            var h = image.get_height();
            var canvas = document.createElement("canvas");
            canvas.width = w;
            canvas.height = h;
            var ctx = canvas.getContext("2d");
            var imgData = ctx.createImageData(w, h);
            image.display(imgData, function(displayData) {
                if (!displayData) { reject(new Error("libheif display() failed")); return; }
                ctx.putImageData(displayData, 0, 0);
                canvas.toBlob(function(jpegBlob) {
                    if (jpegBlob) resolve(jpegBlob);
                    else reject(new Error("canvas.toBlob returned null"));
                }, "image/jpeg", 0.92);
            });
        }).catch(reject);
    });
}

var heicChain = Promise.resolve();
function runHeicSerial(task) {
    var run = heicChain.then(task, task);
    heicChain = run.catch(function() {});
    return run;
}

function ensureJpegPreview(item) {
    if (item._jpegPromise) return item._jpegPromise;
    if (!isHeicType(item.type, item.name)) {
        item._jpegPromise = Promise.resolve(item.blob || null);
        return item._jpegPromise;
    }

    var start = item.blob ? Promise.resolve(item.blob) : fetch(item.url).then(function(r) { return r.blob(); });

    function tryHeic2any(blob) {
        if (typeof heic2any === "undefined") return Promise.reject(new Error("heic2any not loaded"));
        return heic2any({ blob: blob, toType: "image/jpeg", quality: 0.92 }).then(function(result) {
            return Array.isArray(result) ? result[0] : result;
        });
    }

    function tryLibheifJs(blob) {
        return convertHeicViaLibheifJs(blob);
    }

    item._jpegPromise = start.then(function(blob) {
        return runHeicSerial(function() {
            return tryHeic2any(blob).catch(function(err1) {
                log("HEIC convert (heic2any) failed, trying fallback decoder: " + (err1 && err1.message ? err1.message : err1), "warn");
                return tryLibheifJs(blob).catch(function(err2) {
                    log("HEIC convert (fallback) failed: " + (err2 && err2.message ? err2.message : err2), "warn");
                    item.previewFailed = true;
                    throw err2;
                });
            });
        });
    }).then(function(jpegBlob) {
        item.jpegBlob = jpegBlob;
        item.jpegUrl = URL.createObjectURL(jpegBlob);
        item.previewUrl = item.jpegUrl;
        item.convertedFromHeic = true;
        item.previewFailed = false;
        return jpegBlob;
    });

    item._jpegPromise.catch(function() {});
    return item._jpegPromise;
}

function prepareItemPreview(item) {
    return new Promise(function(resolve) {
        normalizeItemMime(item);
        item.previewUrl = item.url;
        if (!isHeicType(item.type, item.name)) {
            resolve(item);
            return;
        }
        ensureJpegPreview(item).then(function() {
            resolve(item);
        }).catch(function() {
            item.previewFailed = true;
            resolve(item);
        });
    });
}

function previewSrc(item) {
    return item.previewUrl || item.url;
}

function heicFallbackThumbHtml() {
    return '<div class="heic-fallback"><span class="heic-chip">HEIC</span></div>';
}

function heicFallbackCardHtml() {
    return '<div class="heic-fallback">' +
        '<span class="heic-chip">HEIC</span>' +
        '<span class="heic-sub">Preview not supported in this browser.<br>Original file is intact, download to view.</span>' +
    '</div>';
}

function buildUnits(items) {
    var stills = {};
    var movs = {};
    var used = {};

    items.forEach(function(it) {
        var key = baseName(it.name).toLowerCase();
        if (isStillImage(it)) {
            if (!stills[key]) stills[key] = it;
        } else if (isMovItem(it)) {
            if (!movs[key]) movs[key] = it;
        }
    });

    var units = [];
    items.forEach(function(it) {
        var key = baseName(it.name).toLowerCase();
        if (used[it.name + "|" + it.size]) return;
        if (isStillImage(it) && movs[key] && stills[key] === it) {
            var mov = movs[key];
            used[it.name + "|" + it.size] = true;
            used[mov.name + "|" + mov.size] = true;
            units.push({ kind: "live", still: it, mov: mov, key: key });
        } else if (isMovItem(it) && stills[key] && movs[key] === it) {
            var st = stills[key];
            if (!used[st.name + "|" + st.size]) {
                used[st.name + "|" + st.size] = true;
                used[it.name + "|" + it.size] = true;
                units.push({ kind: "live", still: st, mov: it, key: key });
            }
        }
    });
    items.forEach(function(it) {
        if (used[it.name + "|" + it.size]) return;
        used[it.name + "|" + it.size] = true;
        units.push({ kind: "single", item: it });
    });

    var idxOf = function(u) {
        if (u.kind === "live") return Math.min(u.still.batch_index || 0, u.mov.batch_index || 0);
        return u.item.batch_index || 0;
    };
    units.sort(function(a, b) { return idxOf(a) - idxOf(b); });
    return units;
}

function triggerDownload(url, name) {
    var a = document.createElement("a");
    a.href = url;
    a.download = name || "file";
    a.style.display = "none";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
}

function downloadOriginal(item) {
    triggerDownload(item.url, item.name);
}

function downloadAsJpg(item) {
    if (!isHeicType(item.type, item.name)) {
        triggerDownload(item.url, item.name);
        return;
    }
    showToast("Converting to JPG...");
    ensureJpegPreview(item).then(function() {
        triggerDownload(item.jpegUrl, swapExt(item.name, "jpg"));
    }).catch(function() {
        showToast("Could not convert. Downloading original HEIC instead.");
        triggerDownload(item.url, item.name);
    });
}

function downloadLiveMov(unit) {
    triggerDownload(unit.mov.url, unit.mov.name);
}

function downloadLivePair(unit) {
    triggerDownload(unit.still.url, unit.still.name);
    setTimeout(function() { triggerDownload(unit.mov.url, unit.mov.name); }, 400);
}

function closeAllDlPops() {
    var pops = document.querySelectorAll(".dl-pop.open");
    for (var i = 0; i < pops.length; i++) pops[i].classList.remove("open");
}

function toggleDlPop(popId, ev) {
    if (ev) ev.stopPropagation();
    var el = document.getElementById(popId);
    if (!el) return;
    var wasOpen = el.classList.contains("open");
    closeAllDlPops();
    if (!wasOpen) el.classList.add("open");
}

document.addEventListener("click", function() { closeAllDlPops(); });

function runDl(unitId, action) {
    var u = unitRegistry[unitId];
    if (!u) return;
    closeAllDlPops();
    if (u.kind === "live") {
        if (action === "heic") downloadOriginal(u.still);
        else if (action === "jpg") downloadAsJpg(u.still);
        else if (action === "mov") downloadLiveMov(u);
        else if (action === "pair") downloadLivePair(u);
    } else {
        if (action === "orig") downloadOriginal(u.item);
        else if (action === "jpg") downloadAsJpg(u.item);
    }
}

var unitRegistry = {};

function registerUnit(u) {
    var id = "u_" + generateTransferId() + "_" + Math.floor(Math.random() * 1000);
    unitRegistry[id] = u;
    return id;
}

function dlMenuHtml(unitId, u) {
    var popId = "pop_" + unitId;
    var items = "";
    if (u.kind === "live") {
        var stillIsHeic = isHeicType(u.still.type, u.still.name);
        items += '<button onclick="runDl(\'' + unitId + '\',\'heic\')">' + (stillIsHeic ? "HEIC (original)" : "Photo (original)") + '<small>' + escapeHtml(u.still.name) + ' · ' + formatBytes(u.still.size) + '</small></button>';
        if (stillIsHeic) items += '<button onclick="runDl(\'' + unitId + '\',\'jpg\')">JPG (converted)<small>Works everywhere</small></button>';
        items += '<button onclick="runDl(\'' + unitId + '\',\'mov\')">MOV (Live video)<small>' + escapeHtml(u.mov.name) + ' · ' + formatBytes(u.mov.size) + '</small></button>';
        items += '<button onclick="runDl(\'' + unitId + '\',\'pair\')">Both files<small>Original photo + MOV</small></button>';
    } else if (isHeicType(u.item.type, u.item.name)) {
        items += '<button onclick="runDl(\'' + unitId + '\',\'orig\')">HEIC (original)<small>' + formatBytes(u.item.size) + '</small></button>';
        items += '<button onclick="runDl(\'' + unitId + '\',\'jpg\')">JPG (converted)<small>Works everywhere</small></button>';
    } else {
        return '<a href="' + u.item.url + '" download="' + escapeHtml(u.item.name) + '" class="action-btn">Download</a>';
    }
    return '<div class="dl-menu">' +
        '<button class="action-btn primary" onclick="toggleDlPop(\'' + popId + '\', event)">Download &#9662;</button>' +
        '<div class="dl-pop" id="' + popId + '" onclick="event.stopPropagation()">' + items + '</div>' +
    '</div>';
}

var LIVE_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="2.2" fill="currentColor"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="9.5" stroke-dasharray="2 2.6"/></svg>';

function liveBadgeHtml() {
    return '<span class="live-badge">' + LIVE_ICON + 'LIVE</span>';
}

function attachLiveStage(stageEl, unit, modeGetter) {
    var video = null;
    var pressTimer = null;

    function ensureVideo() {
        if (video) return video;
        video = document.createElement("video");
        video.src = unit.mov.url;
        video.playsInline = true;
        video.setAttribute("playsinline", "");
        video.muted = false;
        video.loop = false;
        video.preload = "auto";
        video.style.display = "none";
        video.addEventListener("ended", function() { hideVideo(); });
        stageEl.appendChild(video);
        return video;
    }
    function showVideoAndPlay() {
        var v = ensureVideo();
        v.style.display = "block";
        try { v.currentTime = 0; } catch (e) {}
        var p = v.play();
        if (p && p.catch) p.catch(function() {
            v.muted = true;
            v.play().catch(function() {});
        });
    }
    function hideVideo() {
        if (!video) return;
        try { video.pause(); } catch (e) {}
        video.style.display = "none";
    }

    var pressed = false;
    function down(e) {
        if (modeGetter() === "live") return;
        pressed = true;
        pressTimer = setTimeout(function() { if (pressed) showVideoAndPlay(); }, 220);
    }
    function up() {
        pressed = false;
        if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; }
        if (modeGetter() !== "live") hideVideo();
    }
    stageEl.addEventListener("mousedown", down);
    stageEl.addEventListener("touchstart", down, { passive: true });
    stageEl.addEventListener("mouseup", up);
    stageEl.addEventListener("mouseleave", up);
    stageEl.addEventListener("touchend", up);
    stageEl.addEventListener("touchcancel", up);

    return {
        setMode: function(mode) {
            if (mode === "live") {
                showVideoAndPlay();
                video.controls = true;
                video.loop = true;
            } else {
                if (video) { video.controls = false; video.loop = false; }
                hideVideo();
            }
        },
        destroy: function() {
            if (video) { try { video.pause(); } catch (e) {} if (video.parentNode) video.parentNode.removeChild(video); video = null; }
        }
    };
}

function buildLiveCard(unit, container) {
    var unitId = registerUnit(unit);
    var mode = "photo";
    var stillMediaId = registerMedia(unit.still);
    var movMediaId = registerMedia(unit.mov);
    unit.stillMediaId = stillMediaId;
    unit.movMediaId = movMediaId;

    var wrap = document.createElement("div");
    wrap.className = "live-card";

    var stage = document.createElement("div");
    stage.className = "live-stage";
    if (unit.still.previewFailed) {
        stage.innerHTML = heicFallbackCardHtml() + liveBadgeHtml();
    } else {
        stage.innerHTML = '<img src="' + previewSrc(unit.still) + '" alt="' + escapeHtml(unit.still.name) + '">' +
            liveBadgeHtml() +
            '<div class="live-hint">Press and hold to play</div>';
    }
    wrap.appendChild(stage);

    var ctl = attachLiveStage(stage, unit, function() { return mode; });

    var toolbar = document.createElement("div");
    toolbar.className = "live-toolbar";
    var seg = document.createElement("div");
    seg.className = "seg";
    var bPhoto = document.createElement("button");
    bPhoto.textContent = "Photo";
    bPhoto.className = "on";
    var bLive = document.createElement("button");
    bLive.textContent = "Live";
    seg.appendChild(bPhoto);
    seg.appendChild(bLive);

    function setMode(m) {
        mode = m;
        bPhoto.className = (m === "photo") ? "on" : "";
        bLive.className = (m === "live") ? "on" : "";
        var hint = stage.querySelector(".live-hint");
        if (hint) hint.style.display = (m === "photo") ? "block" : "none";
        ctl.setMode(m);
    }
    bPhoto.onclick = function() { setMode("photo"); };
    bLive.onclick = function() { setMode("live"); };

    var right = document.createElement("div");
    right.style.cssText = "display:flex;gap:6px;align-items:center;";
    var expand = document.createElement("button");
    expand.className = "action-btn";
    expand.textContent = "Open";
    expand.onclick = function() { openLightbox([stillMediaId], 0); };
    right.appendChild(expand);
    var dlWrap = document.createElement("span");
    dlWrap.innerHTML = dlMenuHtml(unitId, unit);
    right.appendChild(dlWrap);

    toolbar.appendChild(seg);
    toolbar.appendChild(right);
    wrap.appendChild(toolbar);

    var meta = document.createElement("div");
    meta.className = "gallery-meta";
    meta.textContent = baseName(unit.still.name) + " · " + formatBytes(unit.still.size + unit.mov.size);
    wrap.appendChild(meta);

    container.appendChild(wrap);
}

function ensureBatchCard(batchId, sender, total) {
    if (batchStore[batchId] && batchStore[batchId].cardEl) return batchStore[batchId];
    var list = document.getElementById("received-list");
    var li = document.createElement("li");
    li.className = "feed-item";
    li.dataset.batchId = batchId;
    var now = new Date();
    var time = now.getHours() + ":" + ("0" + now.getMinutes()).slice(-2);
    li.innerHTML =
        '<div class="feed-header">' +
            '<div class="feed-author">' +
                '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>' +
                '<span>' + escapeHtml(sender) + '</span>' +
            '</div>' +
            '<span class="feed-time">' + time + '</span>' +
        '</div>' +
        '<div class="gallery-meta" id="batch-meta-' + batchId + '">Receiving 0 / ' + total + '...</div>' +
        '<div id="batch-body-' + batchId + '"></div>' +
        '<div class="gallery-actions" id="batch-actions-' + batchId + '" style="display:none;">' +
            '<label class="zip-opt" id="batch-zipopt-' + batchId + '" style="display:none;"><input type="checkbox" id="batch-keepheic-' + batchId + '" checked> Keep HEIC original</label>' +
            '<button class="action-btn" onclick="downloadBatchIndividual(\'' + batchId + '\')">Download all</button>' +
            '<button class="action-btn primary" onclick="downloadBatchZip(\'' + batchId + '\')">Download ZIP</button>' +
        '</div>';
    list.insertBefore(li, list.firstChild);
    decorateFeedItem(li, function() { return batchStore[batchId] ? batchStore[batchId].items : []; });
    batchStore[batchId] = {
        sender: sender,
        items: [],
        total: total,
        cardEl: li,
        mediaIds: [],
        unitIds: [],
        renderTimer: null,
        mode: null,
        liveCtls: []
    };
    return batchStore[batchId];
}

function renderBatchBody(batchId) {
    var batch = batchStore[batchId];
    if (!batch) return;
    var body = document.getElementById("batch-body-" + batchId);
    if (!body) return;
    (batch.unitIds || []).forEach(function(id) { delete unitRegistry[id]; });
    batch.unitIds = [];
    body.innerHTML = "";
    batch.mediaIds = [];

    var units = buildUnits(batch.items);
    batch.units = units;
    var hasHeic = batch.items.some(function(it) { return isHeicType(it.type, it.name); });
    var zipOpt = document.getElementById("batch-zipopt-" + batchId);
    if (zipOpt) zipOpt.style.display = hasHeic ? "inline-flex" : "none";

    var allVisual = units.every(function(u) {
        return u.kind === "live" || isMediaType(u.item.type) || isHeicType(u.item.type, u.item.name);
    });

    if (allVisual) {
        var grid = document.createElement("div");
        grid.className = "media-gallery";
        var ids = [];
        units.forEach(function(u) {
            var it = (u.kind === "live") ? u.still : u.item;
            var mediaId = registerMedia(it);
            if (u.kind === "live") u.stillMediaId = mediaId;
            ids.push(mediaId);
        });
        batch.mediaIds = ids;
        units.forEach(function(u, idx) {
            var it = (u.kind === "live") ? u.still : u.item;
            var thumb = document.createElement("div");
            thumb.className = "media-thumb";
            (function(i) {
                thumb.onclick = function() { openLightbox(ids, i, units); };
            })(idx);
            if (isStillImage(it)) {
                if (it.previewFailed) {
                    thumb.innerHTML = heicFallbackThumbHtml();
                } else {
                    thumb.innerHTML = '<img src="' + previewSrc(it) + '" alt="' + escapeHtml(it.name) + '" loading="lazy">';
                }
            } else {
                thumb.innerHTML = '<video src="' + it.url + '" muted preload="metadata"></video>' +
                    '<div class="play-badge"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg></div>';
            }
            if (u.kind === "live") thumb.insertAdjacentHTML("beforeend", liveBadgeHtml());
            grid.appendChild(thumb);
        });
        body.appendChild(grid);
    } else {
        var listEl = document.createElement("div");
        listEl.className = "batch-file-list";
        var listIds = [];
        var listUnits = [];
        units.forEach(function(u) {
            var it = (u.kind === "live") ? u.still : u.item;
            if (isMediaType(it.type) || isHeicType(it.type, it.name)) {
                listIds.push(registerMedia(it));
                listUnits.push(u);
            }
        });
        batch.mediaIds = listIds;

        units.forEach(function(u) {
            var it = (u.kind === "live") ? u.still : u.item;
            var row = document.createElement("div");
            row.className = "batch-file-row";
            var unitId = registerUnit(u);
            batch.unitIds.push(unitId);

            if (isMediaType(it.type) || isHeicType(it.type, it.name)) {
                var pos = listUnits.indexOf(u);
                var thumb = document.createElement("div");
                thumb.className = "batch-file-thumb";
                (function(p) {
                    thumb.onclick = function() { openLightbox(listIds, p, listUnits); };
                })(pos);
                if (isStillImage(it)) {
                    if (it.previewFailed) {
                        thumb.innerHTML = heicFallbackThumbHtml();
                    } else {
                        thumb.innerHTML = '<img src="' + previewSrc(it) + '" alt="">';
                    }
                } else {
                    thumb.innerHTML = '<video src="' + it.url + '" muted preload="metadata"></video>';
                }
                row.appendChild(thumb);
            } else if (isAudioType(it.type)) {
                var icon = document.createElement("div");
                icon.className = "batch-file-icon";
                icon.textContent = "AUD";
                row.appendChild(icon);
            } else {
                var icon2 = document.createElement("div");
                icon2.className = "batch-file-icon";
                icon2.textContent = fileExtLabel(it.name, it.type);
                row.appendChild(icon2);
            }

            var meta = document.createElement("div");
            meta.className = "batch-file-meta";
            var nameLine = escapeHtml(it.name);
            var sizeLine = formatBytes(it.size);
            if (u.kind === "live") {
                nameLine += ' <span style="color:var(--live);font-weight:700;font-size:10px;">LIVE</span>';
                sizeLine = formatBytes(u.still.size + u.mov.size) + " (photo + video)";
            } else if (it.convertedFromHeic) {
                nameLine += ' <span style="color:var(--muted2);font-weight:400;">(HEIC)</span>';
            } else if (it.previewFailed) {
                nameLine += ' <span style="color:var(--muted2);font-weight:400;">(HEIC, no preview)</span>';
            }
            meta.innerHTML =
                '<span class="batch-file-name" title="' + escapeHtml(it.name) + '">' + nameLine + '</span>' +
                '<span class="batch-file-size">' + sizeLine + '</span>';
            row.appendChild(meta);

            if (isAudioType(it.type)) {
                var audioWrap = document.createElement("div");
                audioWrap.className = "batch-audio-row";
                audioWrap.innerHTML = '<audio controls preload="metadata" src="' + it.url + '"></audio>';
                row.appendChild(audioWrap);
            }

            var dlHolder = document.createElement("span");
            dlHolder.innerHTML = dlMenuHtml(unitId, u);
            row.appendChild(dlHolder);
            listEl.appendChild(row);
        });
        body.appendChild(listEl);
    }
}

function scheduleBatchRender(batchId) {
    var batch = batchStore[batchId];
    if (!batch || batch.renderTimer) return;
    batch.renderTimer = setTimeout(function() {
        batch.renderTimer = null;
        renderBatchBody(batchId);
    }, BATCH_RENDER_DELAY);
}

function addToBatch(batchId, item, sender) {
    var total = item.batch_total || 1;
    var batch = ensureBatchCard(batchId, sender, total);

    prepareItemPreview(item).then(function(ready) {
        batch.items.push(ready);
        var metaEl = document.getElementById("batch-meta-" + batchId);
        var done = batch.items.length;

        if (done >= total) {
            var units = buildUnits(batch.items);
            var liveCount = units.filter(function(u) { return u.kind === "live"; }).length;
            var label = done + " file" + (done > 1 ? "s" : "") + " · " +
                formatBytes(batch.items.reduce(function(s, x) { return s + (x.size || 0); }, 0));
            if (liveCount) label += " · " + liveCount + " Live Photo" + (liveCount > 1 ? "s" : "");
            metaEl.textContent = label;
            if (batch.renderTimer) {
                clearTimeout(batch.renderTimer);
                batch.renderTimer = null;
            }
            renderBatchBody(batchId);
            notifyIncoming(sender, done + " file" + (done > 1 ? "s" : ""));
            document.getElementById("batch-actions-" + batchId).style.display = "flex";
        } else {
            metaEl.textContent = "Receiving " + done + " / " + total + "...";
            scheduleBatchRender(batchId);
        }
    });
}

function addReceived(type, data, sender) {
    var list = document.getElementById("received-list");
    var now = new Date();
    var time = now.getHours() + ":" + ("0" + now.getMinutes()).slice(-2);

    if (type === "text") {
        var li = document.createElement("li");
        li.className = "feed-item";
        var textId = "txt_" + generateTransferId();
        var blockId = "block_" + textId;
        var overlayId = "overlay_" + textId;
        var formatted = renderFormattedContent(data, textId);
        li.innerHTML =
            '<div class="feed-header">' +
                '<div class="feed-author">' +
                    '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>' +
                    '<span>' + escapeHtml(sender) + '</span>' +
                '</div>' +
                '<span class="feed-time">' + time + '</span>' +
            '</div>' +
            '<div class="expandable-block" id="' + blockId + '">' +
                '<div class="text-content">' + formatted + '</div>' +
                '<div class="expandable-overlay" id="' + overlayId + '">' +
                    '<button class="expand-toggle-btn" onclick="toggleExpand(\'' + blockId + '\', this)">Show More</button>' +
                '</div>' +
            '</div>' +
            '<div class="feed-actions" style="justify-content:flex-end;">' +
                '<button class="action-btn" onclick="copyFullText(\'' + textId + '\', this)">Copy All</button>' +
            '</div>';
        list.insertBefore(li, list.firstChild);
        li.dataset.textId = textId;
        decorateFeedItem(li, function() { return []; });
        notifyIncoming(sender, String(data).slice(0, 80));
        checkAutoExpand(blockId, overlayId);
        return;
    }

    var batchId = data.batch_id;
    var batchTotal = data.batch_total || 1;

    if (batchId && batchTotal > 1) {
        addToBatch(batchId, data, sender);
        return;
    }

    prepareItemPreview(data).then(function(item) {
        var li = document.createElement("li");
        li.className = "feed-item";

        var header =
            '<div class="feed-header">' +
                '<div class="feed-author">' +
                    '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>' +
                    '<span>' + escapeHtml(sender) + '</span>' +
                '</div>' +
                '<span class="feed-time">' + time + '</span>' +
            '</div>';

        var unit = { kind: "single", item: item };
        var unitId = registerUnit(unit);
        var mediaId = registerMedia(item);
        var titleExtra = "";
        if (item.convertedFromHeic) titleExtra = ' <span style="color:var(--muted2);font-weight:400;font-size:11px;">(HEIC)</span>';
        else if (item.previewFailed) titleExtra = ' <span style="color:var(--muted2);font-weight:400;font-size:11px;">(HEIC, no preview)</span>';

        var previewHtml = "";
        if (isStillImage(item)) {
            if (item.previewFailed) {
                previewHtml = '<div class="file-preview heic-fallback-wrap">' + heicFallbackCardHtml() + '</div>';
            } else {
                previewHtml = '<div class="file-preview" style="cursor:pointer" onclick="openLightbox([\'' + mediaId + '\'], 0)"><img src="' + previewSrc(item) + '" class="preview-img" alt="preview" /></div>';
            }
        } else if (isVideoType(item.type)) {
            previewHtml = '<div class="file-preview" style="cursor:pointer" onclick="openLightbox([\'' + mediaId + '\'], 0)"><video src="' + item.url + '" class="preview-img" muted playsinline></video></div>';
        } else if (isAudioType(item.type)) {
            previewHtml = '<div class="audio-player-wrap"><audio controls preload="metadata" src="' + item.url + '"></audio></div>';
        }

        li.innerHTML = header +
            '<div class="file-card">' +
                '<div class="file-meta">' +
                    '<span class="file-title" title="' + escapeHtml(item.name) + '">' + escapeHtml(item.name) + titleExtra + '</span>' +
                    '<span class="file-size">' + formatBytes(item.size) + '</span>' +
                '</div>' +
                '<span class="dl-slot">' + dlMenuHtml(unitId, unit) + '</span>' +
            '</div>' + previewHtml;
        list.insertBefore(li, list.firstChild);
        decorateFeedItem(li, function() { return [item]; });
        notifyIncoming(sender, item.name);
    });
}

var lightboxUnits = null;

function openLightbox(mediaIds, startIndex, units) {
    lightboxItems = mediaIds.slice();
    lightboxUnits = units || null;
    lightboxIndex = Math.max(0, Math.min(startIndex || 0, lightboxItems.length - 1));
    lightboxMode = {};
    renderLightbox();
    document.getElementById("lightbox").classList.add("open");
    document.body.style.overflow = "hidden";
}

function closeLightbox() {
    stopLightboxLive();
    document.getElementById("lightbox").classList.remove("open");
    document.getElementById("lightbox-stage").innerHTML = "";
    document.getElementById("lightbox-bottom").innerHTML = "";
    document.body.style.overflow = "";
    lightboxItems = [];
    lightboxUnits = null;
}

function lightboxNav(delta) {
    if (!lightboxItems.length) return;
    stopLightboxLive();
    lightboxIndex = (lightboxIndex + delta + lightboxItems.length) % lightboxItems.length;
    renderLightbox();
}

var lbLiveCtl = null;

function stopLightboxLive() {
    if (lbLiveCtl) { lbLiveCtl.destroy(); lbLiveCtl = null; }
}

function currentLightboxUnit() {
    if (lightboxUnits && lightboxUnits[lightboxIndex]) return lightboxUnits[lightboxIndex];
    var id = lightboxItems[lightboxIndex];
    var item = mediaRegistry[id];
    return item ? { kind: "single", item: item } : null;
}

function renderLightbox() {
    stopLightboxLive();
    var unit = currentLightboxUnit();
    if (!unit) return;
    var item = (unit.kind === "live") ? unit.still : unit.item;
    document.getElementById("lightbox-counter").textContent = (lightboxIndex + 1) + " / " + lightboxItems.length;
    var stage = document.getElementById("lightbox-stage");
    var bottom = document.getElementById("lightbox-bottom");
    stage.innerHTML = "";
    bottom.innerHTML = "";

    if (unit.kind === "live") {
        var mode = lightboxMode[lightboxIndex] || "photo";

        if (unit.still.previewFailed) {
            stage.innerHTML = '<div class="lb-heic-fallback">' +
                '<span class="heic-chip">HEIC</span>' +
                '<span class="heic-sub">Photo preview not supported in this browser. Use Download to save the original HEIC + MOV pair.</span>' +
            '</div>';
        } else {
            var img = document.createElement("img");
            img.src = previewSrc(unit.still);
            img.alt = unit.still.name || "";
            stage.appendChild(img);
            stage.insertAdjacentHTML("beforeend", liveBadgeHtml());
            var hint = document.createElement("div");
            hint.className = "lb-live-hint";
            hint.textContent = "Press and hold the photo to play Live";
            stage.appendChild(hint);

            lbLiveCtl = attachLiveStage(stage, unit, function() { return lightboxMode[lightboxIndex] || "photo"; });
        }

        var seg = document.createElement("div");
        seg.className = "seg";
        var bPhoto = document.createElement("button");
        bPhoto.textContent = "Photo";
        var bLive = document.createElement("button");
        bLive.textContent = "Live";
        seg.appendChild(bPhoto);
        seg.appendChild(bLive);
        function apply(m) {
            lightboxMode[lightboxIndex] = m;
            bPhoto.className = (m === "photo") ? "on" : "";
            bLive.className = (m === "live") ? "on" : "";
            var hintEl = stage.querySelector(".lb-live-hint");
            if (hintEl) hintEl.style.display = (m === "photo") ? "block" : "none";
            if (lbLiveCtl) lbLiveCtl.setMode(m);
        }
        bPhoto.onclick = function() { apply("photo"); };
        bLive.onclick = function() { apply("live"); };
        bottom.appendChild(seg);
        apply(mode);

        var nm = document.createElement("span");
        nm.className = "lightbox-name";
        nm.textContent = unit.still.name + " + " + unit.mov.name;
        bottom.appendChild(nm);
        return;
    }

    if (isVideoType(item.type)) {
        var v = document.createElement("video");
        v.src = item.url;
        v.controls = true;
        v.autoplay = true;
        v.playsInline = true;
        stage.appendChild(v);
    } else if (isAudioType(item.type)) {
        var a = document.createElement("audio");
        a.src = item.url;
        a.controls = true;
        a.autoplay = true;
        a.style.width = "min(90vw, 420px)";
        stage.appendChild(a);
    } else if (item.previewFailed) {
        stage.innerHTML = '<div class="lb-heic-fallback">' +
            '<span class="heic-chip">HEIC</span>' +
            '<span class="heic-sub">Preview not supported in this browser. The original file is intact, use Download to save it.</span>' +
        '</div>';
    } else {
        var img2 = document.createElement("img");
        img2.src = previewSrc(item);
        img2.alt = item.name || "";
        stage.appendChild(img2);
    }

    if (isHeicType(item.type, item.name) && !item.previewFailed) {
        var seg2 = document.createElement("div");
        seg2.className = "seg";
        var showJpg = document.createElement("button");
        showJpg.textContent = "Preview (JPG)";
        showJpg.className = "on";
        var showRaw = document.createElement("button");
        showRaw.textContent = "Original HEIC";
        seg2.appendChild(showJpg);
        seg2.appendChild(showRaw);
        showRaw.onclick = function() {
            showToast("HEIC original cannot render in this browser. Use Download.");
        };
        bottom.appendChild(seg2);
    }
    var nm2 = document.createElement("span");
    nm2.className = "lightbox-name";
    nm2.textContent = item.name;
    bottom.appendChild(nm2);
}

function downloadLightboxItem() {
    var unit = currentLightboxUnit();
    if (!unit) return;
    if (unit.kind === "live") {
        downloadLivePair(unit);
    } else {
        downloadOriginal(unit.item);
    }
}

function batchFilesForZip(batch, keepHeic) {
    var tasks = [];
    batch.items.forEach(function(item) {
        if (isHeicType(item.type, item.name) && !keepHeic) {
            tasks.push(ensureJpegPreview(item).then(function(jpg) {
                return { name: swapExt(item.name, "jpg"), blob: jpg };
            }).catch(function() {
                return { name: item.name, blob: item.blob || null, url: item.url };
            }));
        } else {
            tasks.push(Promise.resolve({ name: item.name, blob: item.blob || null, url: item.url }));
        }
    });
    return Promise.all(tasks);
}

function downloadBatchIndividual(batchId) {
    var batch = batchStore[batchId];
    if (!batch) return;
    var keepEl = document.getElementById("batch-keepheic-" + batchId);
    var keep = keepEl ? keepEl.checked : true;
    batchFilesForZip(batch, keep).then(function(files) {
        files.forEach(function(f, i) {
            setTimeout(function() {
                if (f.blob) {
                    var u = URL.createObjectURL(f.blob);
                    triggerDownload(u, f.name);
                    setTimeout(function() { URL.revokeObjectURL(u); }, 8000);
                } else {
                    triggerDownload(f.url, f.name);
                }
            }, i * 350);
        });
        log("Downloading " + files.length + " files individually", "info");
    });
}

function downloadBatchZip(batchId) {
    var batch = batchStore[batchId];
    if (!batch || typeof JSZip === "undefined") {
        log("JSZip not available, falling back to individual downloads", "warn");
        downloadBatchIndividual(batchId);
        return;
    }
    var keepEl = document.getElementById("batch-keepheic-" + batchId);
    var keep = keepEl ? keepEl.checked : true;
    var btn = document.querySelector('#batch-actions-' + batchId + ' .primary');
    if (btn) { btn.textContent = "Zipping..."; btn.disabled = true; }
    var zip = new JSZip();
    var folder = zip.folder("pairme_" + batchId.slice(-6));

    batchFilesForZip(batch, keep).then(function(files) {
        var adds = files.map(function(f) {
            if (f.blob) { folder.file(f.name || "file", f.blob); return Promise.resolve(); }
            return fetch(f.url).then(function(r) { return r.blob(); }).then(function(b) { folder.file(f.name || "file", b); });
        });
        return Promise.all(adds);
    }).then(function() {
        return zip.generateAsync({ type: "blob", compression: "DEFLATE", compressionOptions: { level: 6 } });
    }).then(function(content) {
        var url = URL.createObjectURL(content);
        triggerDownload(url, "pairme_" + batchId.slice(-6) + ".zip");
        setTimeout(function() { URL.revokeObjectURL(url); }, 5000);
        log("ZIP ready (" + batch.items.length + " files)", "success");
    }).catch(function(err) {
        log("ZIP failed: " + (err.message || err), "error");
        downloadBatchIndividual(batchId);
    }).finally(function() {
        if (btn) { btn.textContent = "Download ZIP"; btn.disabled = false; }
    });
}

document.addEventListener("keydown", function(e) {
    if (e.key === "Escape") {
        var sm = document.getElementById("share-modal");
        if (sm && sm.style.display === "flex") { closeShareModal(); return; }
    }
    var lb = document.getElementById("lightbox");
    if (!lb || !lb.classList.contains("open")) return;
    if (e.key === "Escape") closeLightbox();
    if (e.key === "ArrowLeft") lightboxNav(-1);
    if (e.key === "ArrowRight") lightboxNav(1);
});

function respondRequest(accepted) {
    document.getElementById("request-modal").style.display = "none";
    if (pendingRequest) {
        socket.emit("broadcast_response", { to: pendingRequest.from, accepted: accepted, transfer_id: pendingRequest.transfer_id });
        pendingRequest = null;
    }
}

function dragHasFiles(e) {
    return !!(e.dataTransfer && Array.prototype.indexOf.call(e.dataTransfer.types || [], "Files") >= 0);
}

window.addEventListener("dragenter", function(e) {
    if (!dragHasFiles(e)) return;
    e.preventDefault();
    dragDepth++;
    document.getElementById("drop-overlay").classList.add("show");
});

window.addEventListener("dragover", function(e) {
    if (dragHasFiles(e)) e.preventDefault();
});

window.addEventListener("dragleave", function(e) {
    if (!dragHasFiles(e)) return;
    dragDepth = Math.max(0, dragDepth - 1);
    if (!dragDepth) document.getElementById("drop-overlay").classList.remove("show");
});

window.addEventListener("drop", function(e) {
    if (!dragHasFiles(e)) return;
    e.preventDefault();
    dragDepth = 0;
    document.getElementById("drop-overlay").classList.remove("show");
    sendFiles(Array.prototype.slice.call(e.dataTransfer.files));
});

document.addEventListener("paste", function(e) {
    if (document.getElementById("lightbox").classList.contains("open")) return;
    var data = e.clipboardData;
    if (!data) return;
    var files = Array.prototype.slice.call(data.files || []);
    if (!files.length && data.items) {
        Array.prototype.forEach.call(data.items, function(item) {
            if (item.kind === "file") {
                var f = item.getAsFile();
                if (f) files.push(f);
            }
        });
    }
    if (files.length) {
        e.preventDefault();
        stageFiles(files);
        return;
    }
    var t = e.target;
    var editable = t && (t.tagName === "INPUT" || t.tagName === "TEXTAREA" || t.isContentEditable);
    if (editable) return;
    var text = data.getData("text/plain");
    if (text) {
        e.preventDefault();
        applyClipboardText(text);
    }
});

document.addEventListener("keydown", function(e) {
    if (!stagedFiles.length) return;
    if (document.getElementById("lightbox").classList.contains("open")) return;
    var tag = e.target && e.target.tagName;
    if (e.key === "Escape") {
        clearStaged();
    } else if (e.key === "Enter" && !e.shiftKey && tag !== "INPUT" && tag !== "TEXTAREA" && tag !== "SELECT" && tag !== "BUTTON") {
        e.preventDefault();
        sendStaged();
    }
});

window.addEventListener("beforeunload", function(e) {
    if (activeTransfers > 0) {
        e.preventDefault();
        e.returnValue = "File transfer in progress. Leave anyway?";
    }
});

document.addEventListener("visibilitychange", function() {
    if (document.hidden) {
        log("Tab hidden - transfer continues in background", "info");
    } else {
        unreadCount = 0;
        document.title = BASE_TITLE;
    }
});

window.onload = function() {
    initTheme();
    urlRoomCode = readRoomFromUrl();
    if (urlRoomCode) {
        switchTab("transfer");
    }
    setInterval(function() {
        var now = Date.now();
        Object.keys(relayMeta).forEach(function(id) {
            if (now - (relayMeta[id].lastActivity || 0) > BUFFER_STALE_MS) {
                delete relayMeta[id];
                delete relayBuffer[id];
                log("Dropped stale partial transfer", "warn");
            }
        });
    }, 30000);
    document.getElementById("notify-toggle").checked = notifyEnabled;
    if (!(navigator.maxTouchPoints > 0)) {
        document.getElementById("camera-btn").style.display = "none";
    }
    document.getElementById("text-input").addEventListener("keydown", function(e) {
        if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
            e.preventDefault();
            if (this.value.trim()) sendText();
            else if (stagedFiles.length) sendStaged();
        }
    });
    document.getElementById("room-code-input").addEventListener("keydown", function(e) {
        if (e.key === "Enter") joinRoom();
    });
    initSocket();
};
</script>
</body>
</html>
"""

INDEX_HTML = HTML_TEMPLATE.replace("{{ app_version }}", APP_VERSION)
INDEX_ETAG = hashlib.sha256(INDEX_HTML.encode()).hexdigest()[:20]

start_background_services()

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
