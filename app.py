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
    max_http_buffer_size=20 * 1024 * 1024,
)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
peers = {}                        # sid -> info dict
rooms_index = defaultdict(set)    # room -> set(sid), O(1) membership
fp_to_peer_id = {}                # fingerprint hash -> stable peer_id across reconnects
rate_buckets = defaultdict(list)  # sid -> [event timestamps] sliding window
STALE_TTL = 90                    # seconds idle before a dead peer is force-dropped
CLEANUP_INTERVAL = 30

NAME_RE = re.compile(r"^[\w \-.]{1,32}$", re.UNICODE)
ROOM_CODE_RE = re.compile(r"^\d{6}$")
MAX_TEXT_LEN = 20000
MAX_CHUNK_B64_LEN = 400_000       # bounds per-message relay memory (~300KB raw)
RATE_LIMIT_WINDOW = 5.0
RATE_LIMIT_MAX_EVENTS = 60
FILECHUNK_RATE_LIMIT = 400   # generous abuse ceiling; real pacing comes from client ack window
FILECHUNK_WINDOW = 5.0


def generate_code():
    return str(random.randint(100000, 999999))


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
    """Rate-limits a socket handler (per sid, per bucket) and refreshes the
    peer's TTL clock. Different buckets get independent budgets so a file
    transfer's chunk stream can't starve, or be starved by, other events."""
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
    """O(room size), not O(total connected peers)."""
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
    """Background loop: force-drops peers whose socket died without a clean
    disconnect (phone lock, network drop) so rooms/lists don't rot."""
    while True:
        socketio.sleep(CLEANUP_INTERVAL)
        now = time.time()
        stale = [sid for sid, info in peers.items() if now - info.get("last_seen", now) > STALE_TTL]
        for sid in stale:
            log.info("dropping stale peer sid=%s", sid)
            room = peers.get(sid, {}).get("room")
            leave_current_room(sid)
            peers.pop(sid, None)
            drop_rate_buckets(sid)
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

    peers[sid] = {
        "id": peer_id,
        "name": "Device " + peer_id[-4:].upper(),
        "joined": time.time(),
        "last_seen": time.time(),
        "room": "Lobby",
        "fp": fp,
    }
    join_room("Lobby")
    rooms_index["Lobby"].add(sid)
    emit("init", {"peer_id": peer_id, "sid": sid})
    broadcast_peers("Lobby")


@socketio.on("disconnect")
def handle_disconnect():
    sid = request.sid
    room = peers.get(sid, {}).get("room")
    leave_current_room(sid)
    peers.pop(sid, None)
    drop_rate_buckets(sid)
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
    target_sid = data.get("to")
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
    target_sid = data.get("to")
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
    target_sid = data.get("to")
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
    target_sid = data.get("to")
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
    target_sid = data.get("to")
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
    target_sid = data.get("to")
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
    target_sid = data.get("to")
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
    <meta name="keywords" content="file share, pairme, p2p transfer, cross platform transfer, local share, web sharing">
    <meta name="theme-color" content="#ffffff">
    
    <meta property="og:type" content="website">
    <meta property="og:title" content="PairMe - Fast Cross-Device Sharing">
    <meta property="og:description" content="Share text, links, code, and files instantly across mobile and desktop devices.">
    <meta property="og:image" content="https://imgg.fr/r/LkSsr60e.png">
    
    <link rel="icon" type="image/png" href="https://imgg.fr/r/LkSsr60e.png">
    <link rel="apple-touch-icon" href="https://imgg.fr/r/LkSsr60e.png">

    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; -webkit-tap-highlight-color: transparent; }
        body { background: #f8fafc; color: #0f172a; height: 100vh; display: flex; flex-direction: column; overflow: hidden; -webkit-text-size-adjust: 100%; }
        
        header { background: #ffffff; padding: 10px 16px; border-bottom: 1px solid #e2e8f0; display: flex; justify-content: space-between; align-items: center; flex-shrink: 0; }
        .brand { font-size: 16px; font-weight: 700; color: #0f172a; letter-spacing: -0.3px; display: flex; align-items: center; gap: 8px; }
        .brand img { width: 22px; height: 22px; border-radius: 4px; object-fit: cover; }
        .room-tag { background: #f1f5f9; color: #475569; padding: 4px 8px; border-radius: 6px; font-size: 12px; font-weight: 600; border: 1px solid #e2e8f0; display: flex; align-items: center; gap: 4px; }

        .mobile-nav { display: none; background: #ffffff; border-bottom: 1px solid #e2e8f0; flex-shrink: 0; }
        .mobile-nav button { flex: 1; background: transparent; border: none; border-bottom: 2px solid transparent; padding: 10px 0; color: #64748b; font-size: 13px; font-weight: 600; border-radius: 0; }
        .mobile-nav button.active { color: #0f172a; border-bottom-color: #0f172a; background: transparent; }

        .app-grid { display: grid; grid-template-columns: 280px 1fr 300px; gap: 12px; padding: 12px; height: calc(100vh - 53px); flex: 1; overflow: hidden; }
        .card { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; display: flex; flex-direction: column; overflow: hidden; height: 100%; }
        .card-header { padding: 10px 12px; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; color: #64748b; border-bottom: 1px solid #f1f5f9; display: flex; justify-content: space-between; align-items: center; background: #ffffff; flex-shrink: 0; }
        .card-body { padding: 12px; flex: 1; overflow-y: auto; -webkit-overflow-scrolling: touch; display: flex; flex-direction: column; gap: 10px; }

        input, select, textarea { font-size: 13px; border-radius: 6px; border: 1px solid #cbd5e1; background: #ffffff; color: #0f172a; padding: 8px 10px; outline: none; -webkit-appearance: none; appearance: none; }
        input:focus, select:focus, textarea:focus { border-color: #0f172a; }
        
        button { background: #0f172a; color: #ffffff; border: 1px solid #0f172a; border-radius: 6px; font-size: 13px; font-weight: 500; padding: 8px 12px; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; gap: 6px; -webkit-appearance: none; }
        button:active { opacity: 0.8; }
        button.flat { background: #ffffff; color: #0f172a; border: 1px solid #cbd5e1; }
        button.icon-only { padding: 8px; width: 34px; height: 34px; flex-shrink: 0; }

        .row { display: flex; gap: 8px; align-items: center; }
        .flex-1 { flex: 1; min-width: 0; }

        .peer-item { background: #ffffff; border: 1px solid #e2e8f0; padding: 8px 10px; border-radius: 6px; display: flex; justify-content: space-between; align-items: center; cursor: pointer; }
        .peer-item:active, .peer-item.active { border-color: #0f172a; background: #f8fafc; }
        .peer-info { display: flex; flex-direction: column; }
        .peer-name { font-weight: 600; font-size: 13px; color: #0f172a; }
        .peer-id { font-size: 11px; color: #94a3b8; font-family: monospace; }

        .drop-zone { border: 2px dashed #cbd5e1; border-radius: 8px; padding: 16px; text-align: center; color: #64748b; cursor: pointer; background: #f8fafc; display: flex; flex-direction: column; align-items: center; gap: 6px; font-size: 12px; }

        .feed-list { list-style: none; display: flex; flex-direction: column; gap: 10px; }
        .feed-item { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 10px 12px; font-size: 13px; box-shadow: 0 1px 2px rgba(0,0,0,0.03); display: flex; flex-direction: column; gap: 8px; }
        .feed-header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #f1f5f9; padding-bottom: 6px; }
        .feed-author { font-weight: 600; font-size: 12px; color: #334155; display: flex; align-items: center; gap: 6px; }
        .feed-time { font-size: 11px; color: #94a3b8; }
        .feed-actions { display: flex; gap: 6px; align-items: center; }

        .text-content { font-size: 13px; line-height: 1.5; color: #1e293b; word-break: break-word; white-space: pre-wrap; }
        .text-link { color: #2563eb; text-decoration: underline; word-break: break-all; }
        
        .code-wrapper { margin: 6px 0; border-radius: 6px; overflow: hidden; background: #0f172a; border: 1px solid #1e293b; }
        .code-header { display: flex; justify-content: space-between; align-items: center; background: #1e293b; padding: 4px 10px; font-size: 11px; color: #94a3b8; font-family: monospace; }
        .copy-btn { background: transparent; border: 1px solid #475569; color: #cbd5e1; border-radius: 4px; padding: 2px 8px; font-size: 10px; cursor: pointer; }
        .copy-btn:active { background: #334155; }
        .code-block { color: #f8fafc; padding: 10px; font-family: Consolas, Monaco, "Andale Mono", "Ubuntu Mono", monospace; font-size: 12px; line-height: 1.45; overflow-x: auto; max-height: 280px; white-space: pre; word-break: normal; word-wrap: normal; -webkit-overflow-scrolling: touch; }
        .inline-code { background: #f1f5f9; color: #0f172a; border: 1px solid #e2e8f0; padding: 1px 5px; border-radius: 4px; font-family: monospace; font-size: 12px; word-break: break-all; }

        .expandable-block { position: relative; max-height: 220px; overflow: hidden; transition: max-height 0.2s ease; }
        .expandable-block.expanded { max-height: none !important; }
        .expandable-overlay { position: absolute; bottom: 0; left: 0; right: 0; height: 60px; background: linear-gradient(to bottom, rgba(255,255,255,0), rgba(255,255,255,1)); pointer-events: none; display: flex; align-items: flex-end; justify-content: center; padding-bottom: 4px; }
        .expandable-block.expanded .expandable-overlay { display: none; }
        .expand-toggle-btn { background: #ffffff; border: 1px solid #cbd5e1; color: #0f172a; font-size: 11px; font-weight: 600; padding: 3px 10px; border-radius: 12px; cursor: pointer; pointer-events: auto; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }

        .file-card { display: flex; align-items: center; justify-content: space-between; gap: 10px; background: #f8fafc; border: 1px solid #e2e8f0; padding: 8px 10px; border-radius: 6px; }
        .file-meta { display: flex; flex-direction: column; min-width: 0; flex: 1; }
        .file-title { font-weight: 600; font-size: 12px; color: #0f172a; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .file-size { font-size: 11px; color: #64748b; }

        .file-preview { margin-top: 4px; text-align: center; background: #0f172a; border-radius: 6px; overflow: hidden; max-height: 240px; display: flex; align-items: center; justify-content: center; }
        .preview-img { max-width: 100%; max-height: 240px; object-fit: contain; display: block; }

        .action-btn { background: #ffffff; border: 1px solid #cbd5e1; color: #334155; padding: 4px 8px; font-size: 11px; border-radius: 4px; font-weight: 500; height: 26px; }
        .action-btn:active { background: #f1f5f9; }
        .action-btn.primary { background: #0f172a; color: #fff; border-color: #0f172a; }
        .action-btn.primary:active { opacity: 0.85; }

        /* Media gallery (batch of images/videos) */
        .media-gallery { display: grid; grid-template-columns: repeat(auto-fill, minmax(88px, 1fr)); gap: 6px; margin-top: 4px; }
        .media-thumb { position: relative; aspect-ratio: 1; border-radius: 6px; overflow: hidden; background: #0f172a; cursor: pointer; border: 1px solid #e2e8f0; }
        .media-thumb img, .media-thumb video { width: 100%; height: 100%; object-fit: cover; display: block; }
        .media-thumb .play-badge { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; background: rgba(15,23,42,0.35); pointer-events: none; }
        .media-thumb .play-badge svg { width: 22px; height: 22px; color: #fff; filter: drop-shadow(0 1px 2px rgba(0,0,0,0.4)); }
        .media-thumb .file-badge { position: absolute; bottom: 4px; left: 4px; right: 4px; font-size: 9px; color: #fff; background: rgba(15,23,42,0.7); padding: 2px 4px; border-radius: 3px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .gallery-actions { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; justify-content: flex-end; }
        .gallery-meta { font-size: 11px; color: #64748b; margin-top: 2px; }

        /* Lightbox */
        .lightbox { display: none; position: fixed; inset: 0; z-index: 200; background: rgba(15,23,42,0.92); flex-direction: column; align-items: center; justify-content: center; padding: 12px; }
        .lightbox.open { display: flex; }
        .lightbox-toolbar { position: absolute; top: 0; left: 0; right: 0; display: flex; justify-content: space-between; align-items: center; padding: 10px 14px; color: #e2e8f0; font-size: 13px; background: linear-gradient(to bottom, rgba(0,0,0,0.5), transparent); }
        .lightbox-close, .lightbox-nav { background: rgba(255,255,255,0.12); border: 1px solid rgba(255,255,255,0.2); color: #fff; border-radius: 6px; padding: 6px 12px; font-size: 13px; cursor: pointer; }
        .lightbox-close:active, .lightbox-nav:active { background: rgba(255,255,255,0.25); }
        .lightbox-stage { max-width: 96vw; max-height: 78vh; display: flex; align-items: center; justify-content: center; }
        .lightbox-stage img, .lightbox-stage video { max-width: 96vw; max-height: 78vh; object-fit: contain; border-radius: 4px; box-shadow: 0 8px 32px rgba(0,0,0,0.4); }
        .lightbox-nav-wrap { position: absolute; inset: 0; display: flex; align-items: center; justify-content: space-between; pointer-events: none; padding: 0 8px; }
        .lightbox-nav-wrap button { pointer-events: auto; width: 40px; height: 40px; border-radius: 50%; display: flex; align-items: center; justify-content: center; padding: 0; }
        .lightbox-counter { font-variant-numeric: tabular-nums; }

        #log-container { font-family: monospace; font-size: 11px; flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 4px; -webkit-overflow-scrolling: touch; }
        .log-entry { padding: 4px 6px; border-radius: 4px; display: flex; gap: 6px; align-items: flex-start; line-height: 1.3; }
        .log-time { color: #94a3b8; flex-shrink: 0; }
        .log-tag { padding: 1px 4px; border-radius: 3px; font-weight: 700; font-size: 9px; text-transform: uppercase; flex-shrink: 0; }
        
        .tag-info { background: #e0f2fe; color: #0369a1; }
        .tag-success { background: #dcfce7; color: #15803d; }
        .tag-warn { background: #fef3c7; color: #b45309; }
        .tag-error { background: #fee2e2; color: #b91c1c; }
        .tag-p2p { background: #f3e8ff; color: #6b21a8; }

        .progress-bar { height: 4px; background: #e2e8f0; border-radius: 2px; overflow: hidden; margin-top: 4px; }
        .progress-fill { height: 100%; background: #0f172a; width: 0%; }

        .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(15, 23, 42, 0.4); z-index: 100; justify-content: center; align-items: center; padding: 16px; }
        .modal { background: #ffffff; border: 1px solid #e2e8f0; padding: 16px; border-radius: 8px; width: 100%; max-width: 300px; text-align: center; }

        @media (max-width: 768px) {
            body { height: 100%; overflow: auto; }
            .mobile-nav { display: flex; }
            .app-grid { display: flex; flex-direction: column; height: auto; padding: 8px; grid-template-columns: none; overflow: visible; }
            .card { display: none; height: auto; min-height: calc(100vh - 110px); }
            .card.mobile-active { display: flex; }
        }
    </style>
</head>
<body>

    <header>
        <div class="brand">
            <img src="https://imgg.fr/r/LkSsr60e.png" alt="Logo">
            PairMe
        </div>
        <div class="room-tag" id="room-badge">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 8v4l3 3"/></svg>
            <span id="room-name">Lobby</span>
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
                    <div style="font-size:11px;color:#94a3b8;margin-bottom:2px;">THIS DEVICE</div>
                    <div id="my-id" style="font-family:monospace;font-size:13px;font-weight:700;color:#0f172a;">---</div>
                </div>
                <div class="row">
                    <input type="text" id="my-name" placeholder="Device Name" class="flex-1">
                    <button class="flat icon-only" onclick="updateName()" title="Save Name">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
                    </button>
                </div>
                <hr style="border:none;border-top:1px solid #f1f5f9;">
                <div class="row">
                    <input type="text" id="room-code-input" placeholder="6-digit code" maxlength="6" class="flex-1">
                    <button class="flat" onclick="joinRoom()">Join</button>
                </div>
                <div class="row">
                    <button class="flat flex-1" onclick="createRoom()">Create</button>
                    <button class="flat icon-only" onclick="leaveRoom()" title="Leave Room">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
                    </button>
                </div>
                <div class="card-header" style="margin:8px -12px 0 -12px;border-top:1px solid #f1f5f9;">Nearby</div>
                <div id="peer-list" style="display:flex;flex-direction:column;gap:6px;">
                    <div style="color:#94a3b8;font-size:12px;">No devices detected</div>
                </div>
            </div>
        </div>

        <div class="card" id="card-transfer">
            <div class="card-header">
                <span>Transfer</span>
                <span id="target-peer-label" style="color:#0f172a;text-transform:none;font-weight:600;">To: Everyone</span>
            </div>
            <div class="card-body">
                <div class="row">
                    <select id="peer-select" class="flex-1" onchange="onPeerSelectChange()">
                        <option value="">-- All Devices --</option>
                    </select>
                </div>
                <div class="row">
                    <textarea id="text-input" rows="2" placeholder="Message, link, or code..." class="flex-1"></textarea>
                    <button class="icon-only" onclick="sendText()" title="Send">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
                    </button>
                </div>

                <div class="drop-zone" id="drop-zone" onclick="document.getElementById('file-input').click()">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
                    <span>Tap or drag files / photos here</span>
                    <span style="font-size:11px;color:#94a3b8;">Multi-select → gallery + ZIP download</span>
                    <input type="file" id="file-input" multiple accept="*/*" style="display:none;" onchange="handleFileSelect(event)">
                </div>

                <div id="progress-wrap" style="display:none;">
                    <div class="row" style="justify-content:space-between;font-size:11px;color:#64748b;">
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
                <button class="flat icon-only" onclick="clearLogs()" title="Clear Logs" style="width:24px;height:24px;padding:2px;">
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
            <div style="font-size:14px;font-weight:700;margin-bottom:8px;">Transfer Request</div>
            <div id="request-details" style="font-size:12px;color:#64748b;margin-bottom:16px;"></div>
            <div class="row" style="justify-content:center;">
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
            <button class="lightbox-nav" onclick="lightboxNav(-1)" title="Previous">‹</button>
            <button class="lightbox-nav" onclick="lightboxNav(1)" title="Next">›</button>
        </div>
        <div class="lightbox-stage" id="lightbox-stage"></div>
    </div>

<script>
var socket = null;
var mySid = "";
var myPeerId = "";
var peerList = [];
var connections = {};
var pendingRequest = null;
var pendingFileQueue = {};
var relayFileQueue = {};
var relayBuffer = {};
var relayMeta = {};
var textStore = {};
var batchStore = {};          // batchId -> { sender, items: [], total, cardEl }
var mediaRegistry = {};       // mediaId -> { url, name, type, size, blob }
var lightboxItems = [];
var lightboxIndex = 0;
var CHUNK_SIZE = 16384;
var STUN_SERVERS = {
    iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" },
        {
            urls: [
                "turn:openrelay.metered.ca:80",
                "turn:openrelay.metered.ca:443",
                "turn:openrelay.metered.ca:443?transport=tcp"
            ],
            username: "openrelayproject",
            credential: "openrelayproject"
        }
    ]
};
var WEBRTC_SUPPORTED = (typeof window.RTCPeerConnection === "function");
var DEVICE_FP = null;

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
        btnElement.style.color = "#cbd5e1";
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

// -- Device fingerprinting -------------------------------------------------
// Combines several stable, low-entropy-collision signals into one hash so
// the same physical device is recognized even after clearing localStorage
// or switching browser (Safari/Chrome on the same iPhone share canvas/GPU/
// font rendering quirks closely enough to usually collide on hardware-level
// signals). Falls back to a random UUID if the browser blocks these APIs.
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
        log("ID: " + myPeerId, "info");
    });

    socket.on("peers", function(data) {
        peerList = data;
        renderPeers();
    });

    socket.on("signal", handleSignal);

    socket.on("transfer_request", function(data) {
        pendingRequest = data;
        document.getElementById("request-details").textContent = data.from_name + " -> " + data.file_name + " (" + formatBytes(data.file_size) + ")";
        document.getElementById("request-modal").style.display = "flex";
    });

    socket.on("transfer_response", function(data) {
        if (data.accepted) {
            log("Accepted by peer", "success");
            startDataTransfer(data.from, data.transfer_id);
        } else {
            log("Declined by peer", "warn");
        }
    });

    socket.on("room_joined", function(data) {
        document.getElementById("room-name").textContent = data.code;
        log("Joined room " + data.code, "success");
    });

    socket.on("room_left", function() {
        document.getElementById("room-name").textContent = "Lobby";
        log("Switched to Lobby", "info");
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
            var binary = atob(data.chunk);
            var bytes = new Uint8Array(binary.length);
            for (var i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
            relayBuffer[data.transfer_id][data.seq] = bytes.buffer;
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
            log("Received " + meta.file_name, "success");
            delete relayBuffer[data.transfer_id];
            delete relayMeta[data.transfer_id];
        }
    });

    socket.on("rate_limited", function(data) {
        log("Too many requests, slow down (" + data.event + ")", "warn");
    });
}

function renderPeers() {
    var list = document.getElementById("peer-list");
    var select = document.getElementById("peer-select");
    list.innerHTML = "";
    select.innerHTML = '<option value="">-- All Devices --</option>';

    if (peerList.length === 0) {
        list.innerHTML = '<div style="color:#94a3b8;font-size:12px;">No devices detected</div>';
        return;
    }

    peerList.forEach(function(p) {
        var item = document.createElement("div");
        item.className = "peer-item";
        item.onclick = function() { selectPeer(p.sid); };
        item.innerHTML = '<div class="peer-info"><span class="peer-name">' + escapeHtml(p.name) + '</span><span class="peer-id">' + p.id + '</span></div>' +
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg>';
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
        connectPeer(sid);
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
    if (connections[targetSid]) return connections[targetSid];

    var pc = new RTCPeerConnection(STUN_SERVERS);
    pc.iceQueue = [];
    pc.targetSid = targetSid;
    pc.receiveBuffer = {};

    pc.onicecandidate = function(e) {
        if (e.candidate) {
            socket.emit("signal", { to: targetSid, signal: { type: "ice", candidate: e.candidate } });
        }
    };

    pc.ondatachannel = function(e) { setupDataChannel(pc, e.channel, targetSid); };

    if (isInitiator) {
        var channel = pc.createDataChannel("pairme", { ordered: true });
        setupDataChannel(pc, channel, targetSid);
    }

    connections[targetSid] = pc;
    return pc;
}

function setupDataChannel(pc, channel, targetSid) {
    pc.dataChannel = channel;
    channel.onopen = function() { log("P2P open with " + targetSid.slice(0, 4), "p2p"); };
    channel.onmessage = function(e) { handleDataMessage(e.data, targetSid); };
    channel.onerror = function(err) { log("Channel error: " + err.message, "error"); };
}

function handleSignal(data) {
    var fromSid = data.from;
    var signal = data.signal;
    var pc = getOrCreateConnection(fromSid, false);

    if (signal.type === "offer") {
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .then(function() {
                while (pc.iceQueue.length) pc.addIceCandidate(pc.iceQueue.shift());
                return pc.createAnswer();
            })
            .then(function(ans) { return pc.setLocalDescription(ans); })
            .then(function() { socket.emit("signal", { to: fromSid, signal: { type: "answer", sdp: pc.localDescription } }); })
            .catch(function(err) { log("Offer err: " + err.message, "error"); });
    } else if (signal.type === "answer") {
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .then(function() {
                while (pc.iceQueue.length) pc.addIceCandidate(pc.iceQueue.shift());
            })
            .catch(function(err) { log("Answer err: " + err.message, "error"); });
    } else if (signal.type === "ice") {
        var candidate = new RTCIceCandidate(signal.candidate);
        if (pc.remoteDescription && pc.remoteDescription.type) pc.addIceCandidate(candidate);
        else pc.iceQueue.push(candidate);
    }
}

function connectPeer(targetSid) {
    if (!WEBRTC_SUPPORTED) {
        log("WebRTC not supported on this browser, using relay only", "warn");
        return;
    }
    var pc = getOrCreateConnection(targetSid, true);
    pc.createOffer()
        .then(function(offer) { return pc.setLocalDescription(offer); })
        .then(function() { socket.emit("signal", { to: targetSid, signal: { type: "offer", sdp: pc.localDescription } }); })
        .catch(function(err) { log("Offer err: " + err.message, "error"); });
}

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
    var pc = WEBRTC_SUPPORTED ? connections[targetSid] : null;
    if (pc && pc.dataChannel && pc.dataChannel.readyState === "open") {
        pc.dataChannel.send(JSON.stringify({ t: "txt", c: text }));
        log("Sent text (P2P)", "p2p");
    } else {
        socket.emit("relay_text", { to: targetSid, text: text });
        log("Sent text (Relay)", "info");
    }
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
    var meta = {
        file: file,
        transfer_id: transferId,
        batch_id: batchId,
        batch_total: batchTotal,
        batch_index: batchIndex
    };
    var pc = WEBRTC_SUPPORTED ? connections[targetSid] : null;
    if (pc && pc.dataChannel && pc.dataChannel.readyState === "open") {
        if (!pendingFileQueue[targetSid]) pendingFileQueue[targetSid] = [];
        pendingFileQueue[targetSid].push(meta);
        if (pendingFileQueue[targetSid].length === 1) {
            socket.emit("broadcast_request", {
                to: targetSid,
                file_name: file.name,
                file_size: file.size,
                file_type: file.type,
                transfer_id: transferId,
                batch_id: batchId,
                batch_total: batchTotal,
                batch_index: batchIndex
            });
        }
    } else {
        if (!relayFileQueue[targetSid]) relayFileQueue[targetSid] = [];
        relayFileQueue[targetSid].push(meta);
        if (relayFileQueue[targetSid].length === 1) {
            processRelayQueue(targetSid);
        }
    }
}

function processRelayQueue(targetSid) {
    var queue = relayFileQueue[targetSid];
    if (!queue || !queue.length) return;
    var item = queue[0];
    relaySendFile(targetSid, item.file, item.transfer_id, item.batch_id, item.batch_total, item.batch_index, function() {
        queue.shift();
        if (queue.length) processRelayQueue(targetSid);
    });
}

function startDataTransfer(targetSid, transferId) {
    var queue = pendingFileQueue[targetSid];
    if (!queue || !queue.length) return;
    var item = queue[0];
    var file = item.file;
    var pc = connections[targetSid];
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
        document.getElementById("send-status").textContent = "Sending " + file.name + (item.batch_total > 1 ? " (" + (item.batch_index + 1) + "/" + item.batch_total + ")" : "");

        function sendChunk() {
            while (offset < buffer.byteLength) {
                if (channel.bufferedAmount > CHUNK_SIZE * 8) {
                    setTimeout(sendChunk, 20);
                    return;
                }
                var chunk = buffer.slice(offset, offset + CHUNK_SIZE);
                channel.send(chunk);
                offset += CHUNK_SIZE;
                var pct = Math.min(100, Math.round((offset / buffer.byteLength) * 100));
                document.getElementById("progress-fill").style.width = pct + "%";
                document.getElementById("send-pct").textContent = pct + "%";
            }
            channel.send(JSON.stringify({ t: "fe", id: item.transfer_id }));
            log("Sent " + file.name + " (P2P)", "success");
            setTimeout(function() { document.getElementById("progress-wrap").style.display = "none"; }, 800);
            queue.shift();
            if (queue.length) {
                var next = queue[0];
                socket.emit("broadcast_request", {
                    to: targetSid,
                    file_name: next.file.name,
                    file_size: next.file.size,
                    file_type: next.file.type,
                    transfer_id: next.transfer_id,
                    batch_id: next.batch_id,
                    batch_total: next.batch_total,
                    batch_index: next.batch_index
                });
            }
        };
        sendChunk();
    };
    reader.readAsArrayBuffer(file);
}

function relaySendFile(targetSid, file, transferId, batchId, batchTotal, batchIndex, onComplete) {
    socket.emit("relay_file_start", {
        to: targetSid,
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
        if (onComplete) onComplete();
    };
    reader.onload = function(e) {
        var bytes = new Uint8Array(e.target.result);
        var total = bytes.length;
        var offset = 0;
        var inFlight = 0;
        var MAX_IN_FLIGHT = 4;
        var MAX_RETRIES = 6;
        var aborted = false;
        var sentBytes = 0;
        var seqCounter = 0;
        document.getElementById("progress-wrap").style.display = "block";
        document.getElementById("send-status").textContent = "Sending " + file.name;

        function finishIfDone() {
            if (!aborted && offset >= total && inFlight === 0) {
                socket.emit("relay_file_done", { to: targetSid, transfer_id: transferId });
                log("Sent " + file.name + " (Relay)", "success");
                setTimeout(function() { document.getElementById("progress-wrap").style.display = "none"; }, 800);
                if (onComplete) onComplete();
            }
        }

        function sendOneChunk(seq, b64, chunkLen) {
            var attempts = 0;
            function attempt() {
                if (aborted) return;
                socket.emit("relay_file_chunk", { to: targetSid, transfer_id: transferId, chunk: b64, seq: seq }, function(ack) {
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
                        setTimeout(attempt, 150 * attempts);
                    } else {
                        inFlight--;
                        aborted = true;
                        log("Giving up on " + file.name + " after repeated failures", "error");
                        document.getElementById("progress-wrap").style.display = "none";
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
        var msg = JSON.parse(data);
        if (msg.t === "txt") {
            addReceived("text", msg.c, fromSid);
        } else if (msg.t === "fs") {
            var pc = connections[fromSid];
            pc.activeMeta = msg;
            pc.receiveBuffer[msg.id] = [];
        } else if (msg.t === "fe") {
            var pc = connections[fromSid];
            var buffers = pc.receiveBuffer[msg.id];
            if (buffers && pc.activeMeta) {
                var blob = new Blob(buffers, { type: pc.activeMeta.m });
                var url = URL.createObjectURL(blob);
                addReceived("file", {
                    name: pc.activeMeta.n,
                    size: pc.activeMeta.s,
                    url: url,
                    type: pc.activeMeta.m,
                    blob: blob,
                    batch_id: pc.activeMeta.bid || "",
                    batch_total: pc.activeMeta.bt || 1,
                    batch_index: pc.activeMeta.bi || 0
                }, fromSid);
                log("Received " + pc.activeMeta.n, "success");
                delete pc.receiveBuffer[msg.id];
            }
        }
    } else {
        var pc = connections[fromSid];
        if (pc && pc.activeMeta && pc.receiveBuffer[pc.activeMeta.id]) {
            pc.receiveBuffer[pc.activeMeta.id].push(data);
        }
    }
}

function isMediaType(type) {
    if (!type) return false;
    return type.indexOf("image/") === 0 || type.indexOf("video/") === 0;
}

function registerMedia(item) {
    var id = "m_" + generateTransferId();
    mediaRegistry[id] = item;
    return id;
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
        '<div class="gallery-meta" id="batch-meta-' + batchId + '">Receiving 0 / ' + total + '…</div>' +
        '<div class="media-gallery" id="batch-grid-' + batchId + '"></div>' +
        '<div class="gallery-actions" id="batch-actions-' + batchId + '" style="display:none;">' +
            '<button class="action-btn" onclick="downloadBatchIndividual(\'' + batchId + '\')">Download all</button>' +
            '<button class="action-btn primary" onclick="downloadBatchZip(\'' + batchId + '\')">Download ZIP</button>' +
        '</div>';
    list.insertBefore(li, list.firstChild);
    batchStore[batchId] = { sender: sender, items: [], total: total, cardEl: li, mediaIds: [] };
    return batchStore[batchId];
}

function addMediaToBatch(batchId, item, sender) {
    var total = item.batch_total || 1;
    var batch = ensureBatchCard(batchId, sender, total);
    var mediaId = registerMedia(item);
    batch.mediaIds.push(mediaId);
    batch.items.push(item);

    var grid = document.getElementById("batch-grid-" + batchId);
    var thumb = document.createElement("div");
    thumb.className = "media-thumb";
    thumb.onclick = function() { openLightbox(batch.mediaIds, batch.mediaIds.indexOf(mediaId)); };

    if (item.type && item.type.indexOf("image/") === 0) {
        thumb.innerHTML = '<img src="' + item.url + '" alt="' + escapeHtml(item.name) + '" loading="lazy">';
    } else if (item.type && item.type.indexOf("video/") === 0) {
        thumb.innerHTML =
            '<video src="' + item.url + '" muted preload="metadata"></video>' +
            '<div class="play-badge"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg></div>';
    } else {
        thumb.innerHTML = '<div class="file-badge">' + escapeHtml(item.name) + '</div>';
    }
    grid.appendChild(thumb);

    var metaEl = document.getElementById("batch-meta-" + batchId);
    var done = batch.items.length;
    if (done >= total) {
        metaEl.textContent = done + " file" + (done > 1 ? "s" : "") + " · " + formatBytes(batch.items.reduce(function(s, x) { return s + (x.size || 0); }, 0));
        document.getElementById("batch-actions-" + batchId).style.display = "flex";
    } else {
        metaEl.textContent = "Receiving " + done + " / " + total + "…";
    }
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

    // File / media
    var batchId = data.batch_id;
    var batchTotal = data.batch_total || 1;

    // Group media (and multi-file batches) into one gallery card
    if (batchId && (batchTotal > 1 || isMediaType(data.type))) {
        addMediaToBatch(batchId, data, sender);
        return;
    }

    // Single non-batch file (or single media without batch)
    var li = document.createElement("li");
    li.className = "feed-item";
    var isImage = data.type && data.type.indexOf("image/") === 0;
    var isVideo = data.type && data.type.indexOf("video/") === 0;
    var mediaId = registerMedia(data);
    var previewHtml = "";
    if (isImage) {
        previewHtml = '<div class="file-preview" style="cursor:pointer" onclick="openLightbox([\'' + mediaId + '\'], 0)"><img src="' + data.url + '" class="preview-img" alt="preview" /></div>';
    } else if (isVideo) {
        previewHtml = '<div class="file-preview" style="cursor:pointer" onclick="openLightbox([\'' + mediaId + '\'], 0)"><video src="' + data.url + '" class="preview-img" muted playsinline></video></div>';
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
                '<span class="file-title" title="' + escapeHtml(data.name) + '">' + escapeHtml(data.name) + '</span>' +
                '<span class="file-size">' + formatBytes(data.size) + '</span>' +
            '</div>' +
            '<a href="' + data.url + '" download="' + escapeHtml(data.name) + '" class="action-btn" style="text-decoration:none;display:inline-flex;align-items:center;gap:4px;">' +
                '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>' +
                'Download' +
            '</a>' +
        '</div>' + previewHtml;
    list.insertBefore(li, list.firstChild);
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
    if (item.type && item.type.indexOf("video/") === 0) {
        var v = document.createElement("video");
        v.src = item.url;
        v.controls = true;
        v.autoplay = true;
        v.playsInline = true;
        stage.appendChild(v);
    } else {
        var img = document.createElement("img");
        img.src = item.url;
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
    if (btn) { btn.textContent = "Zipping…"; btn.disabled = true; }
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
dropZone.addEventListener("dragover", function(e) { e.preventDefault(); });
dropZone.addEventListener("drop", function(e) {
    e.preventDefault();
    handleFileSelect({ target: { files: e.target.files || e.dataTransfer.files } });
});

window.onload = initSocket;
</script>
</body>
</html>
"""

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
