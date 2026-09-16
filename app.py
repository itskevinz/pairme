from gevent import monkey
monkey.patch_all()

import os
import re
import time
import uuid
import random
import logging
import hashlib
from collections import defaultdict
from functools import wraps

from flask import Flask, render_template_string, request
from flask_socketio import SocketIO, emit, join_room, leave_room

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pairme")

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY") or hashlib.sha256(os.urandom(32)).hexdigest()

ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*")
cors_origins = ALLOWED_ORIGINS.split(",") if ALLOWED_ORIGINS != "*" else "*"

socketio = SocketIO(
    app,
    cors_allowed_origins=cors_origins,
    async_mode="gevent",
    ping_timeout=60,
    ping_interval=25,
    max_http_buffer_size=50 * 1024 * 1024,
    engineio_logger=False,
    logger=False,
)

peers = {}
rooms_index = defaultdict(set)
fp_to_peer_id = {}
peer_id_to_sid = {}
peer_last_room = {}
peer_last_name = {}
rate_buckets = defaultdict(list)

STALE_TTL = 90
CLEANUP_INTERVAL = 30

NAME_RE = re.compile(r"^[\w \-.]{1,32}$", re.UNICODE)
ROOM_CODE_RE = re.compile(r"^\d{6}$")
MAX_TEXT_LEN = 20000
MAX_CHUNK_B64_LEN = 400_000
RATE_LIMIT_WINDOW = 5.0
RATE_LIMIT_MAX_EVENTS = 80
FILECHUNK_RATE_LIMIT = 1200
FILECHUNK_WINDOW = 5.0


def generate_code():
    return str(random.randint(100000, 999999))


def resolve_target_sid(to):
    if to in peers:
        return to
    return peer_id_to_sid.get(to)


def rate_limited(sid, weight=1, bucket="default", limit=RATE_LIMIT_MAX_EVENTS, window=RATE_LIMIT_WINDOW):
    key = (sid, bucket)
    now = time.time()
    bucket_list = rate_buckets[key]
    cutoff = now - window
    while bucket_list and bucket_list[0] < cutoff:
        bucket_list.pop(0)
    if len(bucket_list) + weight > limit:
        return True
    for _ in range(weight):
        bucket_list.append(now)
    return False


def guarded(weight=1, bucket="default", limit=None, window=None):
    lim = limit or RATE_LIMIT_MAX_EVENTS
    win = window or RATE_LIMIT_WINDOW

    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            sid = request.sid
            if sid in peers:
                peers[sid]["last_seen"] = time.time()
            if rate_limited(sid, weight, bucket, lim, win):
                log.warning("rate limit hit sid=%s event=%s bucket=%s", sid, fn.__name__, bucket)
                emit("rate_limited", {"event": fn.__name__})
                return False
            return fn(*args, **kwargs)
        return wrapper
    return deco


def leave_current_room(sid):
    info = peers.get(sid)
    if not info:
        return
    room = info.get("room")
    if room and room != "Lobby":
        leave_room(room)
        rooms_index[room].discard(sid)
        if not rooms_index[room]:
            del rooms_index[room]


def drop_rate_buckets(sid):
    for key in [k for k in rate_buckets.keys() if k[0] == sid]:
        rate_buckets.pop(key, None)


def broadcast_peers(room):
    if room == "Lobby":
        member_sids = {sid for sid, info in peers.items() if info.get("room") == "Lobby"}
        rooms_index["Lobby"] = member_sids
    else:
        member_sids = rooms_index.get(room, set())

    infos = [(sid, peers[sid]) for sid in member_sids if sid in peers]
    by_sid = {sid: {"sid": sid, "id": info["id"], "name": info["name"]} for sid, info in infos}
    for sid, _ in infos:
        others = [v for k, v in by_sid.items() if k != sid]
        socketio.emit("peers", others, room=sid)


def cleanup_stale_peers():
    while True:
        socketio.sleep(CLEANUP_INTERVAL)
        now = time.time()
        stale = [sid for sid, info in peers.items() if now - info.get("last_seen", now) > STALE_TTL]
        for sid in stale:
            log.info("dropping stale peer sid=%s", sid)
            info = peers.get(sid, {})
            room = info.get("room")
            peer_id = info.get("id")
            leave_current_room(sid)
            peers.pop(sid, None)
            drop_rate_buckets(sid)
            if peer_id and peer_id_to_sid.get(peer_id) == sid:
                del peer_id_to_sid[peer_id]
            if room:
                broadcast_peers(room)


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/health")
def health():
    return {"status": "ok", "peers": len(peers)}, 200


@socketio.on("connect")
def handle_connect():
    sid = request.sid
    fp = re.sub(r"[^a-f0-9]", "", (request.args.get("fp") or ""))[:128]

    if fp and fp in fp_to_peer_id:
        peer_id = fp_to_peer_id[fp]
    else:
        peer_id = str(uuid.uuid4())[:8]
        if fp:
            fp_to_peer_id[fp] = peer_id

    room = peer_last_room.get(peer_id, "Lobby")
    name = peer_last_name.get(peer_id, "Device " + peer_id[-4:].upper())

    peers[sid] = {
        "id": peer_id,
        "name": name,
        "joined": time.time(),
        "last_seen": time.time(),
        "room": room,
        "fp": fp,
    }
    peer_id_to_sid[peer_id] = sid
    join_room(room)
    rooms_index[room].add(sid)
    emit("init", {"peer_id": peer_id, "sid": sid, "room": room, "name": name})
    broadcast_peers(room)


@socketio.on("disconnect")
def handle_disconnect():
    sid = request.sid
    info = peers.get(sid, {})
    room = info.get("room")
    peer_id = info.get("id")
    leave_current_room(sid)
    peers.pop(sid, None)
    drop_rate_buckets(sid)
    if peer_id and peer_id_to_sid.get(peer_id) == sid:
        del peer_id_to_sid[peer_id]
    if room:
        broadcast_peers(room)


@socketio.on("set_name")
@guarded()
def handle_set_name(data):
    sid = request.sid
    if sid not in peers or not isinstance(data, dict):
        return
    raw = str(data.get("name", "")).strip()
    if raw and NAME_RE.match(raw):
        peers[sid]["name"] = raw
        peer_last_name[peers[sid]["id"]] = raw
        broadcast_peers(peers[sid]["room"])


@socketio.on("join_room_code")
@guarded()
def handle_join_room_code(data):
    sid = request.sid
    if sid not in peers or not isinstance(data, dict):
        return
    code = str(data.get("code", "")).strip()
    if not ROOM_CODE_RE.match(code):
        emit("room_error", {"msg": "Invalid code"})
        return
    old_room = peers[sid].get("room")
    leave_current_room(sid)
    join_room(code)
    peers[sid]["room"] = code
    peer_last_room[peers[sid]["id"]] = code
    rooms_index[code].add(sid)
    emit("room_joined", {"code": code})
    broadcast_peers(code)
    if old_room:
        broadcast_peers(old_room)


@socketio.on("create_room_code")
@guarded()
def handle_create_room_code():
    sid = request.sid
    if sid not in peers:
        return
    code = generate_code()
    while code in rooms_index:
        code = generate_code()
    old_room = peers[sid].get("room")
    leave_current_room(sid)
    join_room(code)
    peers[sid]["room"] = code
    peer_last_room[peers[sid]["id"]] = code
    rooms_index[code] = {sid}
    emit("room_joined", {"code": code})
    broadcast_peers(code)
    if old_room:
        broadcast_peers(old_room)


@socketio.on("leave_room_code")
@guarded()
def handle_leave_room_code():
    sid = request.sid
    if sid not in peers:
        return
    old_room = peers[sid].get("room")
    leave_current_room(sid)
    join_room("Lobby")
    peers[sid]["room"] = "Lobby"
    peer_last_room[peers[sid]["id"]] = "Lobby"
    rooms_index["Lobby"].add(sid)
    emit("room_left", {})
    broadcast_peers("Lobby")
    if old_room:
        broadcast_peers(old_room)


def _same_room(sid_a, sid_b):
    a, b = peers.get(sid_a), peers.get(sid_b)
    return bool(a and b and a.get("room") == b.get("room"))


@socketio.on("signal")
@guarded(weight=2)
def handle_signal(data):
    if not isinstance(data, dict):
        return
    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))
    if target_sid in peers and _same_room(sid, target_sid):
        emit("signal", {
            "from": sid,
            "from_peer": peers[sid]["id"],
            "from_name": peers[sid]["name"],
            "signal": data.get("signal")
        }, room=target_sid)


@socketio.on("broadcast_request")
@guarded()
def handle_broadcast_request(data):
    if not isinstance(data, dict):
        return
    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))
    if target_sid in peers and _same_room(sid, target_sid):
        emit("transfer_request", {
            "from": sid,
            "from_peer": peers[sid]["id"],
            "from_name": peers[sid]["name"],
            "file_name": str(data.get("file_name", "file"))[:255],
            "file_size": int(data.get("file_size", 0) or 0),
            "file_type": str(data.get("file_type", ""))[:100],
            "transfer_id": str(data.get("transfer_id", ""))[:64],
            "batch_id": str(data.get("batch_id", ""))[:64],
            "batch_total": int(data.get("batch_total", 1) or 1),
            "batch_index": int(data.get("batch_index", 0) or 0),
        }, room=target_sid)


@socketio.on("broadcast_response")
@guarded()
def handle_broadcast_response(data):
    if not isinstance(data, dict):
        return
    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))
    if target_sid in peers and _same_room(sid, target_sid):
        emit("transfer_response", {
            "from": sid,
            "accepted": bool(data.get("accepted", False)),
            "transfer_id": str(data.get("transfer_id", ""))[:64]
        }, room=target_sid)


@socketio.on("relay_text")
@guarded(weight=2)
def handle_relay_text(data):
    if not isinstance(data, dict):
        return
    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))
    text = str(data.get("text", ""))[:MAX_TEXT_LEN]
    if target_sid in peers and _same_room(sid, target_sid):
        emit("relay_text", {
            "from": sid,
            "from_name": peers[sid]["name"],
            "text": text
        }, room=target_sid)


@socketio.on("relay_file_start")
@guarded()
def handle_relay_file_start(data):
    if not isinstance(data, dict):
        return
    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))
    if target_sid in peers and _same_room(sid, target_sid):
        emit("relay_file_start", {
            "from": sid,
            "from_name": peers[sid]["name"],
            "file_name": str(data.get("file_name", "file"))[:255],
            "file_size": int(data.get("file_size", 0) or 0),
            "file_type": str(data.get("file_type", ""))[:100],
            "transfer_id": str(data.get("transfer_id", ""))[:64],
            "batch_id": str(data.get("batch_id", ""))[:64],
            "batch_total": int(data.get("batch_total", 1) or 1),
            "batch_index": int(data.get("batch_index", 0) or 0),
        }, room=target_sid)


@socketio.on("relay_file_chunk")
@guarded(weight=1, bucket="filechunk", limit=FILECHUNK_RATE_LIMIT, window=FILECHUNK_WINDOW)
def handle_relay_file_chunk(data):
    if not isinstance(data, dict):
        return False
    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))
    chunk = data.get("chunk", "")
    if not isinstance(chunk, str) or len(chunk) > MAX_CHUNK_B64_LEN:
        return False
    if target_sid in peers and _same_room(sid, target_sid):
        emit("relay_file_chunk", {
            "from": sid,
            "transfer_id": str(data.get("transfer_id", ""))[:64],
            "chunk": chunk,
            "seq": int(data.get("seq", 0) or 0)
        }, room=target_sid)
        return True
    return False


@socketio.on("relay_file_done")
@guarded()
def handle_relay_file_done(data):
    if not isinstance(data, dict):
        return
    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))
    if target_sid in peers and _same_room(sid, target_sid):
        emit("relay_file_done", {
            "from": sid,
            "transfer_id": str(data.get("transfer_id", ""))[:64]
        }, room=target_sid)


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
            --shadow: 0 1px 3px rgba(0,0,0,0.35);
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; -webkit-tap-highlight-color: transparent; }
        html, body { height: 100%; }
        body { background: var(--bg); color: var(--text); display: flex; flex-direction: column; overflow: hidden; -webkit-text-size-adjust: 100%; transition: background 0.2s, color 0.2s; }
        header { background: var(--card); padding: 10px 16px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; flex-shrink: 0; padding-top: max(10px, env(safe-area-inset-top)); }
        .brand { font-size: 16px; font-weight: 700; color: var(--text); letter-spacing: -0.3px; display: flex; align-items: center; gap: 8px; }
        .brand img { width: 22px; height: 22px; border-radius: 5px; object-fit: cover; }
        .room-tag { background: var(--accent-soft); color: var(--muted); padding: 4px 10px; border-radius: 8px; font-size: 12px; font-weight: 600; border: 1px solid var(--border); display: flex; align-items: center; gap: 5px; }
        .theme-btn { background: transparent; border: 1px solid var(--border); color: var(--muted); border-radius: 8px; width: 34px; height: 34px; display: flex; align-items: center; justify-content: center; cursor: pointer; }
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
        button.flat { background: var(--card); color: var(--text); border: 1px solid var(--border); }
        button.icon-only { padding: 8px; width: 38px; height: 38px; flex-shrink: 0; }
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
        .copy-btn { background: transparent; border: 1px solid #475569; color: #cbd5e1; border-radius: 5px; padding: 3px 8px; font-size: 10px; cursor: pointer; }
        .copy-btn:active { background: #334155; }
        .code-block { color: #f8fafc; padding: 10px; font-family: ui-monospace, Consolas, Monaco, monospace; font-size: 12px; line-height: 1.45; overflow-x: auto; max-height: 280px; white-space: pre; word-break: normal; -webkit-overflow-scrolling: touch; }
        .inline-code { background: var(--accent-soft); color: var(--text); border: 1px solid var(--border); padding: 1px 5px; border-radius: 4px; font-family: ui-monospace, monospace; font-size: 12px; word-break: break-all; }
        .expandable-block { position: relative; max-height: 220px; overflow: hidden; transition: max-height 0.2s ease; }
        .expandable-block.expanded { max-height: none !important; }
        .expandable-overlay { position: absolute; bottom: 0; left: 0; right: 0; height: 60px; background: linear-gradient(to bottom, transparent, var(--card)); pointer-events: none; display: flex; align-items: flex-end; justify-content: center; padding-bottom: 4px; }
        .expandable-block.expanded .expandable-overlay { display: none; }
        .expand-toggle-btn { background: var(--card); border: 1px solid var(--border); color: var(--text); font-size: 11px; font-weight: 600; padding: 4px 12px; border-radius: 14px; cursor: pointer; pointer-events: auto; box-shadow: var(--shadow); }
        .file-card { display: flex; align-items: center; justify-content: space-between; gap: 10px; background: var(--accent-soft); border: 1px solid var(--border); padding: 10px; border-radius: 8px; }
        .file-meta { display: flex; flex-direction: column; min-width: 0; flex: 1; }
        .file-title { font-weight: 600; font-size: 12px; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .file-size { font-size: 11px; color: var(--muted); }
        .file-preview { margin-top: 4px; text-align: center; background: #0f172a; border-radius: 8px; overflow: hidden; max-height: 240px; display: flex; align-items: center; justify-content: center; }
        .preview-img { max-width: 100%; max-height: 240px; object-fit: contain; display: block; }
        .action-btn { background: var(--card); border: 1px solid var(--border); color: var(--text); padding: 5px 10px; font-size: 11px; border-radius: 6px; font-weight: 500; height: 28px; min-height: 28px; }
        .action-btn:active { background: var(--accent-soft); }
        .action-btn.primary { background: var(--accent); color: var(--bg); border-color: var(--accent); }
        .media-gallery { display: grid; grid-template-columns: repeat(auto-fill, minmax(88px, 1fr)); gap: 6px; margin-top: 4px; }
        .media-thumb { position: relative; aspect-ratio: 1; border-radius: 8px; overflow: hidden; background: #0f172a; cursor: pointer; border: 1px solid var(--border); }
        .media-thumb img, .media-thumb video { width: 100%; height: 100%; object-fit: cover; display: block; }
        .media-thumb .play-badge { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; background: rgba(15,23,42,0.35); pointer-events: none; }
        .media-thumb .play-badge svg { width: 22px; height: 22px; color: #fff; filter: drop-shadow(0 1px 2px rgba(0,0,0,0.4)); }
        .batch-file-list { display: flex; flex-direction: column; gap: 6px; margin-top: 4px; }
        .batch-file-row { display: flex; align-items: center; gap: 10px; background: var(--accent-soft); border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; }
        .batch-file-thumb { width: 42px; height: 42px; border-radius: 6px; overflow: hidden; background: #0f172a; flex-shrink: 0; display: flex; align-items: center; justify-content: center; cursor: pointer; }
        .batch-file-thumb img, .batch-file-thumb video { width: 100%; height: 100%; object-fit: cover; }
        .batch-file-icon { width: 42px; height: 42px; border-radius: 6px; background: var(--border); color: var(--muted); flex-shrink: 0; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px; }
        .batch-file-meta { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 1px; }
        .batch-file-name { font-weight: 600; font-size: 12px; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .batch-file-size { font-size: 11px; color: var(--muted); }
        .audio-player-wrap { margin-top: 6px; width: 100%; }
        .audio-player-wrap audio { width: 100%; height: 36px; border-radius: 6px; }
        .batch-audio-row audio { width: 100%; max-width: 220px; height: 32px; }
        .gallery-actions { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; justify-content: flex-end; }
        .gallery-meta { font-size: 11px; color: var(--muted); margin-top: 2px; }
        .lightbox { display: none; position: fixed; inset: 0; z-index: 200; background: rgba(15,23,42,0.94); flex-direction: column; align-items: center; justify-content: center; padding: 12px; padding-top: max(12px, env(safe-area-inset-top)); }
        .lightbox.open { display: flex; }
        .lightbox-toolbar { position: absolute; top: 0; left: 0; right: 0; display: flex; justify-content: space-between; align-items: center; padding: 12px 14px; color: #e2e8f0; font-size: 13px; background: linear-gradient(to bottom, rgba(0,0,0,0.55), transparent); padding-top: max(12px, env(safe-area-inset-top)); }
        .lightbox-close, .lightbox-nav { background: rgba(255,255,255,0.12); border: 1px solid rgba(255,255,255,0.2); color: #fff; border-radius: 8px; padding: 8px 14px; font-size: 13px; cursor: pointer; min-height: 40px; }
        .lightbox-stage { max-width: 96vw; max-height: 78vh; display: flex; align-items: center; justify-content: center; }
        .lightbox-stage img, .lightbox-stage video { max-width: 96vw; max-height: 78vh; object-fit: contain; border-radius: 6px; box-shadow: 0 8px 32px rgba(0,0,0,0.4); }
        .lightbox-nav-wrap { position: absolute; inset: 0; display: flex; align-items: center; justify-content: space-between; pointer-events: none; padding: 0 8px; }
        .lightbox-nav-wrap button { pointer-events: auto; width: 44px; height: 44px; border-radius: 50%; display: flex; align-items: center; justify-content: center; padding: 0; }
        .lightbox-counter { font-variant-numeric: tabular-nums; }
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
        @media (max-width: 768px) {
            body { height: 100%; overflow: auto; }
            .mobile-nav { display: flex; }
            .app-grid { display: flex; flex-direction: column; height: auto; padding: 8px; padding-bottom: max(8px, env(safe-area-inset-bottom)); grid-template-columns: none; overflow: visible; gap: 8px; }
            .card { display: none; height: auto; min-height: calc(100dvh - 120px); }
            .card.mobile-active { display: flex; }
            header { padding-left: max(12px, env(safe-area-inset-left)); padding-right: max(12px, env(safe-area-inset-right)); }
        }
    </style>
</head>
<body>
    <header>
        <div class="brand">
            <img src="https://imgg.fr/r/LkSsr60e.png" alt="Logo">
            PairMe
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
            <span class="transfer-active-badge" id="transfer-badge">Transferring</span>
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
                    <span>Tap or drag files / photos here</span>
                    <span style="font-size:11px;color:var(--muted2);">Multi-select → gallery + ZIP download</span>
                    <input type="file" id="file-input" multiple accept="*/*" style="display:none;" onchange="handleFileSelect(event)">
                </div>
                <div id="progress-wrap" style="display:none;">
                    <div class="row" style="justify-content:space-between;font-size:11px;color:var(--muted);">
                        <span id="send-status">Sending</span>
                        <span id="send-pct">0%</span>
                    </div>
                    <div class="progress-bar"><div class="progress-fill" id="progress-fill"></div></div>
                </div>
                <div class="card-header" style="margin:0 -12px;">Received</div>
                <ul class="feed-list" id="received-list"></ul>
            </div>
        </div>
        <div class="card" id="card-logs">
            <div class="card-header">
                <span>Logs</span>
                <button class="flat icon-only" onclick="clearLogs()" title="Clear Logs" style="width:28px;height:28px;padding:2px;" aria-label="Clear logs">
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
    <div class="lightbox" id="lightbox" onclick="if(event.target===this) closeLightbox()">
        <div class="lightbox-toolbar">
            <span class="lightbox-counter" id="lightbox-counter">1 / 1</span>
            <div class="row" style="gap:8px;">
                <button class="lightbox-nav" id="lightbox-download" onclick="downloadLightboxItem()">Download</button>
                <button class="lightbox-close" onclick="closeLightbox()">Close</button>
            </div>
        </div>
        <div class="lightbox-nav-wrap">
            <button class="lightbox-nav" onclick="lightboxNav(-1)" title="Previous">&#8249;</button>
            <button class="lightbox-nav" onclick="lightboxNav(1)" title="Next">&#8250;</button>
        </div>
        <div class="lightbox-stage" id="lightbox-stage"></div>
    </div>
<script>
/* ========== PairMe upgraded client ========== */
var socket = null;
var mySid = "";
var myPeerId = "";
var peerList = [];
var connections = {};          // sid -> RTCPeerConnection
var pendingRequest = null;
var pendingFileQueue = {};     // sid -> [{file, transfer_id, ...}]
var relayFileQueue = {};
var relayBuffer = {};
var relayMeta = {};
var textStore = {};
var batchStore = {};
var mediaRegistry = {};
var lightboxItems = [];
var lightboxIndex = 0;
var CHUNK_SIZE = 16384;        // 16KB - most stable across browsers + iOS Safari
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
var DEVICE_FP = null;
var activeTransfers = 0;
var RELAY_BATCH_CONCURRENCY = 3;
var P2P_CONNECT_TIMEOUT = 10000; // ms before falling back to relay for a file
var peerConnState = {};        // sid -> "connecting" | "p2p" | "relay" | "failed"

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

function escapeHtml(text) {
    var div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
}

function formatBytes(bytes) {
    if (bytes === 0) return "0 B";
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
    btnElement.textContent = msg;
    btnElement.style.background = "#16a34a";
    btnElement.style.color = "#ffffff";
    setTimeout(function() {
        btnElement.textContent = originalText;
        btnElement.style.background = "transparent";
        btnElement.style.color = "";
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

function fnv1aHash(str) {
    var h = 0x811c9dc5;
    for (var i = 0; i < str.length; i++) {
        h ^= str.charCodeAt(i);
        h = (h * 0x01000193) >>> 0;
    }
    return h.toString(16);
}

function getCanvasFingerprint() {
    try {
        var canvas = document.createElement("canvas");
        canvas.width = 220; canvas.height = 40;
        var ctx = canvas.getContext("2d");
        ctx.textBaseline = "top";
        ctx.font = "14px 'Arial'";
        ctx.fillStyle = "#f60";
        ctx.fillRect(0, 0, 100, 20);
        ctx.fillStyle = "#069";
        ctx.fillText("pairme_fp_%!@#", 2, 2);
        ctx.fillStyle = "rgba(102,204,0,0.7)";
        ctx.fillText("device_id", 4, 14);
        return canvas.toDataURL();
    } catch (e) { return "no-canvas"; }
}

function getWebglFingerprint() {
    try {
        var canvas = document.createElement("canvas");
        var gl = canvas.getContext("webgl") || canvas.getContext("experimental-webgl");
        if (!gl) return "no-webgl";
        var ext = gl.getExtension("WEBGL_debug_renderer_info");
        var vendor = ext ? gl.getParameter(ext.UNMASKED_VENDOR_WEBGL) : gl.getParameter(gl.VENDOR);
        var renderer = ext ? gl.getParameter(ext.UNMASKED_RENDERER_WEBGL) : gl.getParameter(gl.RENDERER);
        return vendor + "~" + renderer;
    } catch (e) { return "no-webgl"; }
}

function getFontFingerprint() {
    try {
        var testFonts = ["Arial", "Courier New", "Georgia", "Times New Roman", "Verdana", "Comic Sans MS", "Impact", "PingFang SC", "Helvetica Neue"];
        var baseFonts = ["monospace", "sans-serif", "serif"];
        var testString = "mmmmmmmmmmlli";
        var testSize = "72px";
        var span = document.createElement("span");
        span.style.position = "absolute";
        span.style.left = "-9999px";
        span.style.fontSize = testSize;
        span.textContent = testString;
        document.body.appendChild(span);
        var baseSizes = {};
        baseFonts.forEach(function(bf) {
            span.style.fontFamily = bf;
            baseSizes[bf] = span.offsetWidth + "x" + span.offsetHeight;
        });
        var detected = [];
        testFonts.forEach(function(font) {
            var found = false;
            for (var i = 0; i < baseFonts.length; i++) {
                span.style.fontFamily = "'" + font + "', " + baseFonts[i];
                var size = span.offsetWidth + "x" + span.offsetHeight;
                if (size !== baseSizes[baseFonts[i]]) { found = true; break; }
            }
            if (found) detected.push(font);
        });
        document.body.removeChild(span);
        return detected.join(",");
    } catch (e) { return "no-fonts"; }
}

function getAudioFingerprint() {
    return new Promise(function(resolve) {
        try {
            var AudioCtx = window.OfflineAudioContext || window.webkitOfflineAudioContext;
            if (!AudioCtx) return resolve("no-audio");
            var ctx = new AudioCtx(1, 5000, 44100);
            var osc = ctx.createOscillator();
            osc.type = "triangle";
            osc.frequency.setValueAtTime(10000, ctx.currentTime);
            var compressor = ctx.createDynamicsCompressor();
            osc.connect(compressor);
            compressor.connect(ctx.destination);
            osc.start(0);
            ctx.startRendering();
            ctx.oncomplete = function(e) {
                var output = e.renderedBuffer.getChannelData(0);
                var sum = 0;
                for (var i = 4500; i < 5000; i++) sum += Math.abs(output[i]);
                resolve(sum.toFixed(6));
            };
            setTimeout(function() { resolve("audio-timeout"); }, 800);
        } catch (e) { resolve("no-audio"); }
    });
}

function getHardwareFingerprint() {
    var parts = [
        navigator.hardwareConcurrency || 0,
        navigator.deviceMemory || 0,
        screen.width, screen.height, screen.colorDepth,
        window.devicePixelRatio || 1,
        navigator.platform || "",
        navigator.maxTouchPoints || 0,
        Intl.DateTimeFormat().resolvedOptions().timeZone || ""
    ];
    return parts.join("~");
}

function computeDeviceFingerprint() {
    return getAudioFingerprint().then(function(audioFp) {
        var raw = [
            getCanvasFingerprint(),
            getWebglFingerprint(),
            getFontFingerprint(),
            getHardwareFingerprint(),
            audioFp
        ].join("||");
        var hash = fnv1aHash(raw) + fnv1aHash(raw.split("").reverse().join(""));
        return hash;
    });
}

function getOrComputeFingerprint() {
    return computeDeviceFingerprint().then(function(hash) {
        DEVICE_FP = hash;
        return hash;
    }).catch(function() {
        var fallback = localStorage.getItem("pairme_fp_fallback");
        if (!fallback) {
            fallback = fnv1aHash(String(Date.now()) + String(Math.random()));
            localStorage.setItem("pairme_fp_fallback", fallback);
        }
        DEVICE_FP = fallback;
        return fallback;
    });
}

function initSocket() {
    getOrComputeFingerprint().then(function(fp) {
        socket = io({
            transports: ["websocket", "polling"],
            query: { fp: fp },
            reconnection: true,
            reconnectionAttempts: Infinity,
            reconnectionDelay: 1000,
            reconnectionDelayMax: 8000,
            randomizationFactor: 0.5,
            timeout: 20000
        });
        bindSocketEvents();
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
        document.getElementById("my-id").textContent = myPeerId;
        if (data.room && data.room !== "Lobby") {
            document.getElementById("room-name").textContent = data.room;
        }
        if (data.name) {
            document.getElementById("my-name").value = data.name;
        }
        log("ID: " + myPeerId, "info");
    });

    socket.on("peers", function(data) {
        peerList = data;
        renderPeers();
        // Proactively try to establish P2P with new peers
        peerList.forEach(function(p) {
            if (!connections[p.sid] || !isDataChannelOpen(p.sid)) {
                connectPeer(p.sid, true);
            }
        });
    });

    socket.on("signal", handleSignal);

    socket.on("transfer_request", function(data) {
        pendingRequest = data;
        document.getElementById("request-details").textContent = data.from_name + " → " + data.file_name + " (" + formatBytes(data.file_size) + ")";
        document.getElementById("request-modal").style.display = "flex";
    });

    socket.on("transfer_response", function(data) {
        if (data.accepted) {
            log("Accepted by peer", "success");
            startDataTransfer(data.from, data.transfer_id);
        } else {
            log("Declined by peer", "warn");
            // remove from queue
            var q = pendingFileQueue[data.from];
            if (q && q.length && q[0].transfer_id === data.transfer_id) {
                q.shift();
            }
        }
    });

    socket.on("room_joined", function(data) {
        document.getElementById("room-name").textContent = data.code;
        log("Joined room " + data.code, "success");
        // close old connections when changing room
        Object.keys(connections).forEach(function(sid) {
            try { connections[sid].close(); } catch(e){}
            delete connections[sid];
            delete peerConnState[sid];
        });
    });

    socket.on("room_left", function() {
        document.getElementById("room-name").textContent = "Lobby";
        log("Switched to Lobby", "info");
        Object.keys(connections).forEach(function(sid) {
            try { connections[sid].close(); } catch(e){}
            delete connections[sid];
            delete peerConnState[sid];
        });
    });

    socket.on("relay_text", function(data) {
        addReceived("text", data.text, data.from_name);
        log("Text from " + data.from_name, "info");
    });

    socket.on("relay_file_start", function(data) {
        relayBuffer[data.transfer_id] = {};
        relayMeta[data.transfer_id] = data;
        log("Receiving " + data.file_name + (data.batch_total > 1 ? " (" + ((data.batch_index || 0) + 1) + "/" + data.batch_total + ")" : ""), "info");
    });

    socket.on("relay_file_chunk", function(data) {
        if (relayBuffer[data.transfer_id]) {
            try {
                var binary = atob(data.chunk);
                var bytes = new Uint8Array(binary.length);
                for (var i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
                relayBuffer[data.transfer_id][data.seq] = bytes.buffer;
            } catch (e) {
                log("Chunk decode error", "error");
            }
        }
    });

    socket.on("relay_file_done", function(data) {
        var meta = relayMeta[data.transfer_id];
        var chunkMap = relayBuffer[data.transfer_id];
        if (meta && chunkMap) {
            var seqs = Object.keys(chunkMap).map(Number).sort(function(a, b) { return a - b; });
            var ordered = seqs.map(function(s) { return chunkMap[s]; });
            var blob = new Blob(ordered, { type: meta.file_type });
            var url = URL.createObjectURL(blob);
            addReceived("file", {
                name: meta.file_name,
                size: meta.file_size,
                url: url,
                type: meta.file_type,
                blob: blob,
                batch_id: meta.batch_id || "",
                batch_total: meta.batch_total || 1,
                batch_index: meta.batch_index || 0
            }, meta.from_name);
            log("Received " + meta.file_name + " (Relay)", "success");
            delete relayBuffer[data.transfer_id];
            delete relayMeta[data.transfer_id];
        }
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
    renderPeers(); // refresh status badges
}

function renderPeers() {
    var list = document.getElementById("peer-list");
    var select = document.getElementById("peer-select");
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

/* ---------- WebRTC core (fixed) ---------- */

function getOrCreateConnection(targetSid, isInitiator) {
    if (!WEBRTC_SUPPORTED) return null;
    if (connections[targetSid] && connections[targetSid].connectionState !== "closed" && connections[targetSid].connectionState !== "failed") {
        return connections[targetSid];
    }

    // clean previous
    if (connections[targetSid]) {
        try { connections[targetSid].close(); } catch(e){}
    }

    var pc = new RTCPeerConnection(STUN_SERVERS);
    pc.iceQueue = [];
    pc.targetSid = targetSid;
    pc.receiveBuffer = {};
    pc._makingOffer = false;
    pc._ignoreOffer = false;
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
            // attempt ICE restart once
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

    // critical for Safari / backpressure
    try {
        channel.bufferedAmountLowThreshold = CHUNK_SIZE * 4;
    } catch (e) {}

    channel.onopen = function() {
        log("P2P open with " + targetSid.slice(0, 6), "p2p");
        setPeerConnState(targetSid, "p2p");
        // if there are pending files waiting for this channel, start them
        if (pendingFileQueue[targetSid] && pendingFileQueue[targetSid].length) {
            var first = pendingFileQueue[targetSid][0];
            socket.emit("broadcast_request", {
                to: first.target_peer_id,
                file_name: first.file.name,
                file_size: first.file.size,
                file_type: first.file.type,
                transfer_id: first.transfer_id,
                batch_id: first.batch_id,
                batch_total: first.batch_total,
                batch_index: first.batch_index
            });
        }
    };

    channel.onclose = function() {
        log("P2P closed " + targetSid.slice(0, 6), "warn");
        setPeerConnState(targetSid, "relay");
    };

    channel.onerror = function(err) {
        log("Channel error: " + (err.message || err), "error");
        setPeerConnState(targetSid, "failed");
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
        if (pc._ignoreOffer) return;

        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
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
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .then(function() {
                while (pc.iceQueue.length) {
                    pc.addIceCandidate(pc.iceQueue.shift()).catch(function(){});
                }
            })
            .catch(function(err) { log("Answer err: " + err.message, "error"); });
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
    if (isDataChannelOpen(targetSid) && !force) return;

    var pc = getOrCreateConnection(targetSid, true);
    // polite peer = lower id for simple perfect negotiation
    pc._polite = (mySid < targetSid);
    makeOffer(pc, targetSid);
}

/* ---------- Send text / files ---------- */

function sendText() {
    var text = document.getElementById("text-input").value.trim();
    if (!text) return;
    var targetSid = document.getElementById("peer-select").value;

    if (targetSid) {
        sendTextTo(targetSid, text);
    } else {
        peerList.forEach(function(p) { sendTextTo(p.sid, text); });
    }
    document.getElementById("text-input").value = "";
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

function handleFileSelect(e) {
    var files = e.target.files || (e.dataTransfer && e.dataTransfer.files);
    if (!files || !files.length) return;
    var targetSid = document.getElementById("peer-select").value;
    var batchId = "b_" + generateTransferId();
    var fileArr = Array.prototype.slice.call(files);
    var total = fileArr.length;

    fileArr.forEach(function(file, idx) {
        if (targetSid) {
            sendFileTo(targetSid, file, batchId, total, idx);
        } else {
            peerList.forEach(function(p) { sendFileTo(p.sid, file, batchId, total, idx); });
        }
    });
    document.getElementById("file-input").value = "";
    if (total > 1) log("Queued batch of " + total + " files", "info");
}

function sendFileTo(targetSid, file, batchId, batchTotal, batchIndex) {
    batchId = batchId || ("b_" + generateTransferId());
    batchTotal = batchTotal || 1;
    batchIndex = (typeof batchIndex === "number") ? batchIndex : 0;
    var transferId = generateTransferId();
    var pInfo = peerList.find(function(x) { return x.sid === targetSid; });
    var targetPeerId = pInfo ? pInfo.id : targetSid;
    var meta = {
        file: file,
        transfer_id: transferId,
        batch_id: batchId,
        batch_total: batchTotal,
        batch_index: batchIndex,
        target_peer_id: targetPeerId
    };

    // Prefer P2P if already open
    if (isDataChannelOpen(targetSid)) {
        if (!pendingFileQueue[targetSid]) pendingFileQueue[targetSid] = [];
        pendingFileQueue[targetSid].push(meta);
        if (pendingFileQueue[targetSid].length === 1) {
            socket.emit("broadcast_request", {
                to: targetPeerId,
                file_name: file.name,
                file_size: file.size,
                file_type: file.type,
                transfer_id: transferId,
                batch_id: batchId,
                batch_total: batchTotal,
                batch_index: batchIndex
            });
        }
        return;
    }

    // Try to establish P2P first, with timeout fallback to relay
    connectPeer(targetSid, true);
    if (!pendingFileQueue[targetSid]) pendingFileQueue[targetSid] = [];
    pendingFileQueue[targetSid].push(meta);

    var waited = 0;
    var check = setInterval(function() {
        waited += 400;
        if (isDataChannelOpen(targetSid)) {
            clearInterval(check);
            // channel open handler will start the queue
            return;
        }
        if (waited >= P2P_CONNECT_TIMEOUT) {
            clearInterval(check);
            // move to relay
            var q = pendingFileQueue[targetSid];
            if (q) {
                var idx = q.findIndex(function(m) { return m.transfer_id === transferId; });
                if (idx >= 0) {
                    var item = q.splice(idx, 1)[0];
                    if (!relayFileQueue[targetSid]) relayFileQueue[targetSid] = [];
                    relayFileQueue[targetSid].push(item);
                    if (relayFileQueue[targetSid].length === 1 || (relayFileQueue[targetSid]._active || 0) < RELAY_BATCH_CONCURRENCY) {
                        processRelayQueue(targetSid);
                    }
                }
            }
            setPeerConnState(targetSid, "relay");
            log("P2P timeout → using Relay for " + file.name, "warn");
        }
    }, 400);
}

function processRelayQueue(targetSid) {
    var queue = relayFileQueue[targetSid];
    if (!queue) return;
    if (queue._active === undefined) queue._active = 0;
    while (queue._active < RELAY_BATCH_CONCURRENCY && queue.length) {
        var item = queue.shift();
        queue._active++;
        relaySendFile(targetSid, item.target_peer_id, item.file, item.transfer_id, item.batch_id, item.batch_total, item.batch_index, function() {
            queue._active--;
            processRelayQueue(targetSid);
        });
    }
}

function startDataTransfer(targetSid, transferId) {
    var queue = pendingFileQueue[targetSid];
    if (!queue || !queue.length) return;
    var item = queue[0];
    if (item.transfer_id !== transferId) return; // safety

    var file = item.file;
    var pc = connections[targetSid];
    if (!pc || !pc.dataChannel || pc.dataChannel.readyState !== "open") {
        // fallback to relay
        queue.shift();
        if (!relayFileQueue[targetSid]) relayFileQueue[targetSid] = [];
        relayFileQueue[targetSid].push(item);
        processRelayQueue(targetSid);
        return;
    }
    var channel = pc.dataChannel;

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

    var reader = new FileReader();
    reader.onload = function(e) {
        var buffer = e.target.result;
        var offset = 0;
        document.getElementById("progress-wrap").style.display = "block";
        document.getElementById("send-status").textContent = "P2P " + file.name + (item.batch_total > 1 ? " (" + (item.batch_index + 1) + "/" + item.batch_total + ")" : "");
        activeTransfers++;
        updateTransferBadge();

        function sendNext() {
            if (offset >= buffer.byteLength) {
                channel.send(JSON.stringify({ t: "fe", id: item.transfer_id }));
                log("Sent " + file.name + " (P2P)", "success");
                setTimeout(function() { document.getElementById("progress-wrap").style.display = "none"; }, 600);
                activeTransfers--;
                updateTransferBadge();
                queue.shift();
                if (queue.length) {
                    var next = queue[0];
                    socket.emit("broadcast_request", {
                        to: item.target_peer_id,
                        file_name: next.file.name,
                        file_size: next.file.size,
                        file_type: next.file.type,
                        transfer_id: next.transfer_id,
                        batch_id: next.batch_id,
                        batch_total: next.batch_total,
                        batch_index: next.batch_index
                    });
                }
                return;
            }

            // backpressure
            if (channel.bufferedAmount > CHUNK_SIZE * 12) {
                // wait for low event or poll
                var onLow = function() {
                    channel.removeEventListener("bufferedamountlow", onLow);
                    sendNext();
                };
                channel.addEventListener("bufferedamountlow", onLow);
                // also poll for Safari which sometimes misses the event
                setTimeout(function() {
                    if (channel.bufferedAmount <= CHUNK_SIZE * 8) {
                        channel.removeEventListener("bufferedamountlow", onLow);
                        sendNext();
                    }
                }, 40);
                return;
            }

            var chunk = buffer.slice(offset, offset + CHUNK_SIZE);
            try {
                channel.send(chunk);
            } catch (err) {
                log("Send chunk failed, falling back to relay", "error");
                // move remaining to relay roughly (simplified: just current file to relay)
                activeTransfers--;
                updateTransferBadge();
                queue.shift();
                if (!relayFileQueue[targetSid]) relayFileQueue[targetSid] = [];
                relayFileQueue[targetSid].unshift(item);
                processRelayQueue(targetSid);
                return;
            }
            offset += chunk.byteLength;
            var pct = Math.min(100, Math.round((offset / buffer.byteLength) * 100));
            document.getElementById("progress-fill").style.width = pct + "%";
            document.getElementById("send-pct").textContent = pct + "%";
            // continue
            if (typeof requestAnimationFrame === "function") {
                requestAnimationFrame(sendNext);
            } else {
                setTimeout(sendNext, 0);
            }
        }
        sendNext();
    };
    reader.readAsArrayBuffer(file);
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
    var reader = new FileReader();
    reader.onerror = function() {
        log("Failed to read " + file.name, "error");
        activeTransfers--;
        updateTransferBadge();
        if (onComplete) onComplete();
    };
    reader.onload = function(e) {
        var bytes = new Uint8Array(e.target.result);
        var total = bytes.length;
        var offset = 0;
        var inFlight = 0;
        var MAX_IN_FLIGHT = 12;
        var MAX_RETRIES = 8;
        var aborted = false;
        var sentBytes = 0;
        var seqCounter = 0;
        document.getElementById("progress-wrap").style.display = "block";
        document.getElementById("send-status").textContent = "Relay " + file.name;
        activeTransfers++;
        updateTransferBadge();

        function finishIfDone() {
            if (!aborted && offset >= total && inFlight === 0) {
                socket.emit("relay_file_done", { to: targetPeerId, transfer_id: transferId });
                log("Sent " + file.name + " (Relay)", "success");
                setTimeout(function() { document.getElementById("progress-wrap").style.display = "none"; }, 600);
                activeTransfers--;
                updateTransferBadge();
                if (onComplete) onComplete();
            }
        }

        function sendOneChunk(seq, b64, chunkLen) {
            var attempts = 0;
            function attempt() {
                if (aborted) return;
                socket.emit("relay_file_chunk", { to: targetPeerId, transfer_id: transferId, chunk: b64, seq: seq }, function(ack) {
                    if (ack) {
                        inFlight--;
                        sentBytes += chunkLen;
                        var pct = Math.min(100, Math.round((sentBytes / total) * 100));
                        document.getElementById("progress-fill").style.width = pct + "%";
                        document.getElementById("send-pct").textContent = pct + "%";
                        pump();
                        finishIfDone();
                    } else if (attempts < MAX_RETRIES) {
                        attempts++;
                        setTimeout(attempt, 120 * attempts);
                    } else {
                        inFlight--;
                        aborted = true;
                        log("Giving up on " + file.name + " after repeated failures", "error");
                        document.getElementById("progress-wrap").style.display = "none";
                        activeTransfers--;
                        updateTransferBadge();
                        if (onComplete) onComplete();
                    }
                });
            }
            attempt();
        }

        function pump() {
            while (!aborted && inFlight < MAX_IN_FLIGHT && offset < total) {
                var end = Math.min(offset + CHUNK_SIZE, total);
                var chunk = bytes.subarray(offset, end);
                var binary = "";
                for (var i = 0; i < chunk.length; i++) binary += String.fromCharCode(chunk[i]);
                var b64 = btoa(binary);
                var chunkLen = end - offset;
                offset = end;
                inFlight++;
                sendOneChunk(seqCounter++, b64, chunkLen);
            }
        }
        pump();
    };
    reader.readAsArrayBuffer(file);
}

function handleDataMessage(data, fromSid) {
    if (typeof data === "string") {
        try {
            var msg = JSON.parse(data);
            if (msg.t === "txt") {
                var name = (peerList.find(function(p){return p.sid===fromSid;}) || {}).name || fromSid.slice(0,6);
                addReceived("text", msg.c, name);
            } else if (msg.t === "fs") {
                var pc = connections[fromSid];
                if (pc) {
                    pc.activeMeta = msg;
                    pc.receiveBuffer[msg.id] = [];
                }
            } else if (msg.t === "fe") {
                var pc2 = connections[fromSid];
                if (pc2 && pc2.receiveBuffer[msg.id] && pc2.activeMeta) {
                    var buffers = pc2.receiveBuffer[msg.id];
                    var blob = new Blob(buffers, { type: pc2.activeMeta.m });
                    var url = URL.createObjectURL(blob);
                    var senderName = (peerList.find(function(p){return p.sid===fromSid;}) || {}).name || fromSid.slice(0,6);
                    addReceived("file", {
                        name: pc2.activeMeta.n,
                        size: pc2.activeMeta.s,
                        url: url,
                        type: pc2.activeMeta.m,
                        blob: blob,
                        batch_id: pc2.activeMeta.bid || "",
                        batch_total: pc2.activeMeta.bt || 1,
                        batch_index: pc2.activeMeta.bi || 0
                    }, senderName);
                    log("Received " + pc2.activeMeta.n + " (P2P)", "success");
                    delete pc2.receiveBuffer[msg.id];
                }
            }
        } catch (e) {
            log("Bad data message", "error");
        }
    } else {
        var pc3 = connections[fromSid];
        if (pc3 && pc3.activeMeta && pc3.receiveBuffer[pc3.activeMeta.id]) {
            pc3.receiveBuffer[pc3.activeMeta.id].push(data);
        }
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

/* ---------- Media helpers (kept + polished) ---------- */

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

function registerMedia(item) {
    var id = "m_" + generateTransferId();
    mediaRegistry[id] = item;
    return id;
}

function prepareItemPreview(item) {
    return new Promise(function(resolve) {
        normalizeItemMime(item);
        item.previewUrl = item.url;
        item.previewReady = true;

        if (!isHeicType(item.type, item.name)) {
            resolve(item);
            return;
        }
        if (typeof heic2any === "undefined") {
            resolve(item);
            return;
        }

        var sourceBlob = item.blob;
        var start = sourceBlob
            ? Promise.resolve(sourceBlob)
            : fetch(item.url).then(function(r) { return r.blob(); });

        start.then(function(blob) {
            return heic2any({ blob: blob, toType: "image/jpeg", quality: 0.92 });
        }).then(function(result) {
            var jpegBlob = Array.isArray(result) ? result[0] : result;
            item.previewUrl = URL.createObjectURL(jpegBlob);
            item.previewType = "image/jpeg";
            item.convertedFromHeic = true;
            resolve(item);
        }).catch(function(err) {
            log("HEIC preview convert failed: " + (err && err.message ? err.message : err), "warn");
            resolve(item);
        });
    });
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
            '<button class="action-btn" onclick="downloadBatchIndividual(\'' + batchId + '\')">Download all</button>' +
            '<button class="action-btn primary" onclick="downloadBatchZip(\'' + batchId + '\')">Download ZIP</button>' +
        '</div>';
    list.insertBefore(li, list.firstChild);
    batchStore[batchId] = {
        sender: sender,
        items: [],
        total: total,
        cardEl: li,
        mediaIds: [],
        mode: null
    };
    return batchStore[batchId];
}

function decideBatchMode(batch) {
    if (!batch.items.length) return "list";
    var allVisual = batch.items.every(function(it) { return isMediaType(it.type); });
    return allVisual ? "gallery" : "list";
}

function previewSrc(item) {
    return item.previewUrl || item.url;
}

function renderBatchBody(batchId) {
    var batch = batchStore[batchId];
    if (!batch) return;
    var body = document.getElementById("batch-body-" + batchId);
    if (!body) return;

    var mode = decideBatchMode(batch);
    batch.mode = mode;
    body.innerHTML = "";

    if (mode === "gallery") {
        var grid = document.createElement("div");
        grid.className = "media-gallery";
        batch.mediaIds = [];
        batch.items.forEach(function(item) {
            var mediaId = registerMedia(item);
            batch.mediaIds.push(mediaId);
            var thumb = document.createElement("div");
            thumb.className = "media-thumb";
            (function(mid, ids) {
                thumb.onclick = function() { openLightbox(ids, ids.indexOf(mid)); };
            })(mediaId, batch.mediaIds);
            if (isImageType(item.type) || item.convertedFromHeic) {
                thumb.innerHTML = '<img src="' + previewSrc(item) + '" alt="' + escapeHtml(item.name) + '" loading="lazy">';
            } else if (isVideoType(item.type)) {
                thumb.innerHTML =
                    '<video src="' + item.url + '" muted preload="metadata"></video>' +
                    '<div class="play-badge"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg></div>';
            } else {
                thumb.innerHTML = '<div class="batch-file-icon" style="width:100%;height:100%;border-radius:0;">' + fileExtLabel(item.name, item.type) + '</div>';
            }
            grid.appendChild(thumb);
        });
        body.appendChild(grid);
    } else {
        var listEl = document.createElement("div");
        listEl.className = "batch-file-list";
        batch.mediaIds = [];
        batch.items.forEach(function(item) {
            var row = document.createElement("div");
            row.className = "batch-file-row";

            if (isMediaType(item.type) || item.convertedFromHeic) {
                var mediaId = registerMedia(item);
                batch.mediaIds.push(mediaId);
                var thumb = document.createElement("div");
                thumb.className = "batch-file-thumb";
                (function(mid) {
                    thumb.onclick = function() {
                        openLightbox(batch.mediaIds.slice(), batch.mediaIds.indexOf(mid));
                    };
                })(mediaId);
                if (isImageType(item.type) || item.convertedFromHeic) {
                    thumb.innerHTML = '<img src="' + previewSrc(item) + '" alt="">';
                } else {
                    thumb.innerHTML = '<video src="' + item.url + '" muted preload="metadata"></video>';
                }
                row.appendChild(thumb);
            } else if (isAudioType(item.type)) {
                var icon = document.createElement("div");
                icon.className = "batch-file-icon";
                icon.textContent = "AUD";
                row.appendChild(icon);
            } else {
                var icon2 = document.createElement("div");
                icon2.className = "batch-file-icon";
                icon2.textContent = fileExtLabel(item.name, item.type);
                row.appendChild(icon2);
            }

            var meta = document.createElement("div");
            meta.className = "batch-file-meta";
            var nameLine = escapeHtml(item.name);
            if (item.convertedFromHeic) nameLine += ' <span style="color:var(--muted2);font-weight:400;">(HEIC)</span>';
            meta.innerHTML =
                '<span class="batch-file-name" title="' + escapeHtml(item.name) + '">' + nameLine + '</span>' +
                '<span class="batch-file-size">' + formatBytes(item.size) + '</span>';
            row.appendChild(meta);

            if (isAudioType(item.type)) {
                var audioWrap = document.createElement("div");
                audioWrap.className = "batch-audio-row";
                audioWrap.innerHTML = '<audio controls preload="metadata" src="' + item.url + '"></audio>';
                row.appendChild(audioWrap);
            }

            var dl = document.createElement("a");
            dl.href = item.url;
            dl.download = item.name || "file";
            dl.className = "action-btn";
            dl.style.textDecoration = "none";
            dl.textContent = "Download";
            row.appendChild(dl);
            listEl.appendChild(row);
        });
        body.appendChild(listEl);
    }
}

function addToBatch(batchId, item, sender) {
    var total = item.batch_total || 1;
    var batch = ensureBatchCard(batchId, sender, total);

    prepareItemPreview(item).then(function(ready) {
        batch.items.push(ready);
        var metaEl = document.getElementById("batch-meta-" + batchId);
        var done = batch.items.length;

        if (done >= total) {
            metaEl.textContent = done + " file" + (done > 1 ? "s" : "") + " · " +
                formatBytes(batch.items.reduce(function(s, x) { return s + (x.size || 0); }, 0));
            renderBatchBody(batchId);
            document.getElementById("batch-actions-" + batchId).style.display = "flex";
        } else {
            metaEl.textContent = "Receiving " + done + " / " + total + "...";
            renderBatchBody(batchId);
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
        var mediaId = registerMedia(item);
        var titleExtra = item.convertedFromHeic ? ' <span style="color:var(--muted2);font-weight:400;font-size:11px;">(HEIC → preview)</span>' : '';
        var previewHtml = "";
        if (isImageType(item.type) || item.convertedFromHeic) {
            previewHtml = '<div class="file-preview" style="cursor:pointer" onclick="openLightbox([\'' + mediaId + '\'], 0)"><img src="' + previewSrc(item) + '" class="preview-img" alt="preview" /></div>';
        } else if (isVideoType(item.type)) {
            previewHtml = '<div class="file-preview" style="cursor:pointer" onclick="openLightbox([\'' + mediaId + '\'], 0)"><video src="' + item.url + '" class="preview-img" muted playsinline></video></div>';
        } else if (isAudioType(item.type)) {
            previewHtml = '<div class="audio-player-wrap"><audio controls preload="metadata" src="' + item.url + '"></audio></div>';
        }

        li.innerHTML =
            '<div class="feed-header">' +
                '<div class="feed-author">' +
                    '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>' +
                    '<span>' + escapeHtml(sender) + '</span>' +
                '</div>' +
                '<span class="feed-time">' + time + '</span>' +
            '</div>' +
            '<div class="file-card">' +
                '<div class="file-meta">' +
                    '<span class="file-title" title="' + escapeHtml(item.name) + '">' + escapeHtml(item.name) + titleExtra + '</span>' +
                    '<span class="file-size">' + formatBytes(item.size) + '</span>' +
                '</div>' +
                '<a href="' + item.url + '" download="' + escapeHtml(item.name) + '" class="action-btn" style="text-decoration:none;display:inline-flex;align-items:center;gap:4px;">' +
                    '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>' +
                    'Download' +
                '</a>' +
            '</div>' + previewHtml;
        list.insertBefore(li, list.firstChild);
    });
}

function openLightbox(mediaIds, startIndex) {
    lightboxItems = mediaIds.slice();
    lightboxIndex = Math.max(0, Math.min(startIndex || 0, lightboxItems.length - 1));
    renderLightbox();
    document.getElementById("lightbox").classList.add("open");
    document.body.style.overflow = "hidden";
}

function closeLightbox() {
    document.getElementById("lightbox").classList.remove("open");
    document.getElementById("lightbox-stage").innerHTML = "";
    document.body.style.overflow = "";
    lightboxItems = [];
}

function lightboxNav(delta) {
    if (!lightboxItems.length) return;
    lightboxIndex = (lightboxIndex + delta + lightboxItems.length) % lightboxItems.length;
    renderLightbox();
}

function renderLightbox() {
    var id = lightboxItems[lightboxIndex];
    var item = mediaRegistry[id];
    if (!item) return;
    document.getElementById("lightbox-counter").textContent = (lightboxIndex + 1) + " / " + lightboxItems.length;
    var stage = document.getElementById("lightbox-stage");
    stage.innerHTML = "";
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
    } else {
        var img = document.createElement("img");
        img.src = previewSrc(item);
        img.alt = item.name || "";
        stage.appendChild(img);
    }
}

function downloadLightboxItem() {
    var id = lightboxItems[lightboxIndex];
    var item = mediaRegistry[id];
    if (!item) return;
    triggerDownload(item.url, item.name);
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

function downloadBatchIndividual(batchId) {
    var batch = batchStore[batchId];
    if (!batch) return;
    batch.items.forEach(function(item, i) {
        setTimeout(function() { triggerDownload(item.url, item.name); }, i * 350);
    });
    log("Downloading " + batch.items.length + " files individually", "info");
}

function downloadBatchZip(batchId) {
    var batch = batchStore[batchId];
    if (!batch || typeof JSZip === "undefined") {
        log("JSZip not available, falling back to individual downloads", "warn");
        downloadBatchIndividual(batchId);
        return;
    }
    var btn = document.querySelector('#batch-actions-' + batchId + ' .primary');
    if (btn) { btn.textContent = "Zipping..."; btn.disabled = true; }
    var zip = new JSZip();
    var folder = zip.folder("pairme_" + batchId.slice(-6));
    var promises = batch.items.map(function(item) {
        if (item.blob) return Promise.resolve(item.blob).then(function(b) { folder.file(item.name || "file", b); });
        return fetch(item.url).then(function(r) { return r.blob(); }).then(function(b) { folder.file(item.name || "file", b); });
    });
    Promise.all(promises).then(function() {
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

var dropZone = document.getElementById("drop-zone");
dropZone.addEventListener("dragover", function(e) {
    e.preventDefault();
    dropZone.classList.add("dragover");
});
dropZone.addEventListener("dragleave", function() {
    dropZone.classList.remove("dragover");
});
dropZone.addEventListener("drop", function(e) {
    e.preventDefault();
    dropZone.classList.remove("dragover");
    handleFileSelect({ target: { files: e.dataTransfer.files } });
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
    }
});

window.onload = function() {
    initTheme();
    initSocket();
};
</script>
</body>
</html>
"""

if __name__ == "__main__":
    socketio.start_background_task(cleanup_stale_peers)
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
