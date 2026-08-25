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
    max_http_buffer_size=30 * 1024 * 1024,
)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
peers = {}                        # sid -> info dict
rooms_index = defaultdict(set)    # room -> set(sid), O(1) membership
fp_to_peer_id = {}                # fingerprint hash -> stable peer_id across reconnects
peer_id_to_sid = {}               # peer_id -> current sid (survives reconnect)
peer_last_room = {}               # peer_id -> last room (restore on reconnect)
peer_last_name = {}               # peer_id -> last display name
rate_buckets = defaultdict(list)  # sid -> [event timestamps] sliding window
STALE_TTL = 90                    # seconds idle before a dead peer is force-dropped
CLEANUP_INTERVAL = 30

NAME_RE = re.compile(r"^[\w \-.]{1,32}$", re.UNICODE)
ROOM_CODE_RE = re.compile(r"^\d{6}$")
MAX_TEXT_LEN = 20000
MAX_CHUNK_B64_LEN = 700_000       # ~500KB raw base64 or binary payload ceiling
MAX_CHUNK_BIN_LEN = 524_288       # 512KB binary chunk ceiling
RATE_LIMIT_WINDOW = 5.0
RATE_LIMIT_MAX_EVENTS = 60
FILECHUNK_RATE_LIMIT = 1200  # higher ceiling for concurrent chunk streams
FILECHUNK_WINDOW = 5.0


def generate_code():
    return str(random.randint(100000, 999999))


def resolve_target_sid(to):
    """Accept either a live sid or a stable peer_id; return current sid or None."""
    if not to:
        return None
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
    # Accept binary (bytes) or base64 string for backward compatibility
    if isinstance(chunk, (bytes, bytearray, memoryview)):
        if len(chunk) > MAX_CHUNK_BIN_LEN:
            return False
        chunk_payload = bytes(chunk)
        is_bin = True
    elif isinstance(chunk, str):
        if len(chunk) > MAX_CHUNK_B64_LEN:
            return False
        chunk_payload = chunk
        is_bin = False
    else:
        return False
    if target_sid in peers and _same_room(sid, target_sid):
        payload = {
            "from": sid,
            "transfer_id": str(data.get("transfer_id", ""))[:64],
            "chunk": chunk_payload,
            "seq": int(data.get("seq", 0) or 0),
            "bin": is_bin,
        }
        emit("relay_file_chunk", payload, room=target_sid)
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
    <script src="https://cdn.jsdelivr.net/npm/heic2any@0.0.4/dist/heic2any.min.js"></script>
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

        /* Batch: pure media gallery */
        .media-gallery { display: grid; grid-template-columns: repeat(auto-fill, minmax(88px, 1fr)); gap: 6px; margin-top: 4px; }
        .media-thumb { position: relative; aspect-ratio: 1; border-radius: 6px; overflow: hidden; background: #0f172a; cursor: pointer; border: 1px solid #e2e8f0; }
        .media-thumb img, .media-thumb video { width: 100%; height: 100%; object-fit: cover; display: block; }
        .media-thumb .play-badge { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; background: rgba(15,23,42,0.35); pointer-events: none; }
        .media-thumb .play-badge svg { width: 22px; height: 22px; color: #fff; filter: drop-shadow(0 1px 2px rgba(0,0,0,0.4)); }

        /* Batch: mixed / list mode (like WeTransfer / LocalSend) */
        .batch-file-list { display: flex; flex-direction: column; gap: 6px; margin-top: 4px; }
        .batch-file-row { display: flex; align-items: center; gap: 10px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 8px 10px; }
        .batch-file-thumb { width: 40px; height: 40px; border-radius: 5px; overflow: hidden; background: #0f172a; flex-shrink: 0; display: flex; align-items: center; justify-content: center; cursor: pointer; }
        .batch-file-thumb img, .batch-file-thumb video { width: 100%; height: 100%; object-fit: cover; }
        .batch-file-icon { width: 40px; height: 40px; border-radius: 5px; background: #e2e8f0; color: #475569; flex-shrink: 0; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px; }
        .batch-file-meta { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 1px; }
        .batch-file-name { font-weight: 600; font-size: 12px; color: #0f172a; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .batch-file-size { font-size: 11px; color: #64748b; }
        .audio-player-wrap { margin-top: 6px; width: 100%; }
        .audio-player-wrap audio { width: 100%; height: 36px; border-radius: 6px; }
        .batch-audio-row audio { width: 100%; max-width: 220px; height: 32px; }

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
var CHUNK_SIZE = 65536;          // P2P DataChannel chunk (64KB, browser-safe)
var RELAY_CHUNK_SIZE = 131072;   // Relay binary chunk (128KB)
var RELAY_BATCH_CONCURRENCY = 3;
var RELAY_MAX_IN_FLIGHT = 16;
var P2P_BUFFER_HIGH = 8 * 1024 * 1024;  // pause send above 8MB buffered
var P2P_BUFFER_LOW  = 2 * 1024 * 1024;  // resume when below 2MB
var P2P_CONNECT_TIMEOUT_MS = 10000;
var STUN_SERVERS = {
    iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" },
        { urls: "stun:stun.cloudflare.com:3478" },
        {
            urls: [
                "turn:openrelay.metered.ca:80",
                "turn:openrelay.metered.ca:443",
                "turn:openrelay.metered.ca:443?transport=tcp",
                "turns:openrelay.metered.ca:443"
            ],
            username: "openrelayproject",
            credential: "openrelayproject"
        }
    ],
    iceCandidatePoolSize: 4,
    iceTransportPolicy: "all"
};
var WEBRTC_SUPPORTED = (typeof window.RTCPeerConnection === "function");
var DEVICE_FP = null;
var p2pFailed = {};  // targetSid -> true when ICE failed / timed out → force relay

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
        if (data.name) {
            document.getElementById("my-name").value = data.name;
        }
        if (data.room && data.room !== "Lobby") {
            document.getElementById("room-name").textContent = data.room;
        } else {
            document.getElementById("room-name").textContent = "Lobby";
        }
        log("ID: " + myPeerId + (data.room && data.room !== "Lobby" ? " (room " + data.room + ")" : ""), "info");
    });

    socket.on("peers", function(data) {
        peerList = data;
        // Clear p2pFailed for peers that got new sids (reconnect)
        var liveSids = {};
        data.forEach(function(p) { liveSids[p.sid] = true; });
        Object.keys(p2pFailed).forEach(function(sid) {
            if (!liveSids[sid]) delete p2pFailed[sid];
        });
        // Drop stale connection objects for gone sids
        Object.keys(connections).forEach(function(sid) {
            if (!liveSids[sid]) destroyConnection(sid, "peer left");
        });
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
        if (!relayBuffer[data.transfer_id]) return;
        var chunk = data.chunk;
        if (chunk instanceof ArrayBuffer) {
            relayBuffer[data.transfer_id][data.seq] = chunk;
        } else if (chunk && chunk.buffer && chunk.byteLength !== undefined) {
            // Uint8Array / typed array
            relayBuffer[data.transfer_id][data.seq] = chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength);
        } else if (typeof chunk === "string") {
            var binary = atob(chunk);
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

function resolvePeerId(targetSid) {
    var p = peerList.find(function(x) { return x.sid === targetSid; });
    return p ? p.id : targetSid;
}

function destroyConnection(targetSid, reason) {
    var pc = connections[targetSid];
    if (!pc) return;
    try {
        if (pc.dataChannel) {
            try { pc.dataChannel.close(); } catch (e) {}
        }
        pc.close();
    } catch (e) {}
    delete connections[targetSid];
    if (reason) log("P2P closed " + targetSid.slice(0, 6) + ": " + reason, "warn");
}

function getOrCreateConnection(targetSid, isInitiator) {
    if (!WEBRTC_SUPPORTED) return null;
    if (p2pFailed[targetSid]) return null;
    if (connections[targetSid]) {
        var existing = connections[targetSid];
        var st = existing.connectionState || existing.iceConnectionState;
        if (st === "failed" || st === "closed") {
            destroyConnection(targetSid, st);
        } else {
            return existing;
        }
    }

    var targetPeerId = resolvePeerId(targetSid);
    var pc = new RTCPeerConnection(STUN_SERVERS);
    pc.iceQueue = [];
    pc.targetSid = targetSid;
    pc.targetPeerId = targetPeerId;
    pc.receiveBuffer = {};
    pc._makingOffer = false;
    pc._connectTimer = null;

    pc.onicecandidate = function(e) {
        if (e.candidate) {
            socket.emit("signal", {
                to: targetPeerId,
                signal: { type: "ice", candidate: e.candidate }
            });
        }
    };

    pc.oniceconnectionstatechange = function() {
        var s = pc.iceConnectionState;
        if (s === "connected" || s === "completed") {
            log("ICE " + s + " → " + targetSid.slice(0, 6), "p2p");
            if (pc._connectTimer) { clearTimeout(pc._connectTimer); pc._connectTimer = null; }
            delete p2pFailed[targetSid];
        } else if (s === "failed") {
            log("ICE failed → " + targetSid.slice(0, 6) + " (will use relay)", "warn");
            p2pFailed[targetSid] = true;
            if (pc._connectTimer) { clearTimeout(pc._connectTimer); pc._connectTimer = null; }
            // Try one ICE restart before giving up
            if (!pc._restarted) {
                pc._restarted = true;
                try {
                    pc.restartIce();
                    if (isInitiator || pc._wasInitiator) {
                        pc.createOffer({ iceRestart: true }).then(function(offer) {
                            return pc.setLocalDescription(offer);
                        }).then(function() {
                            socket.emit("signal", {
                                to: targetPeerId,
                                signal: { type: "offer", sdp: pc.localDescription }
                            });
                        }).catch(function() {
                            destroyConnection(targetSid, "ICE restart failed");
                        });
                    }
                } catch (e) {
                    destroyConnection(targetSid, "ICE failed");
                }
            } else {
                destroyConnection(targetSid, "ICE failed after restart");
            }
        } else if (s === "disconnected") {
            log("ICE disconnected → " + targetSid.slice(0, 6), "warn");
        }
    };

    pc.onconnectionstatechange = function() {
        var s = pc.connectionState;
        if (s === "failed" || s === "closed") {
            p2pFailed[targetSid] = true;
            destroyConnection(targetSid, "connection " + s);
        }
    };

    pc.ondatachannel = function(e) { setupDataChannel(pc, e.channel, targetSid); };

    if (isInitiator) {
        pc._wasInitiator = true;
        var channel = pc.createDataChannel("pairme", {
            ordered: true,
            maxRetransmits: 30
        });
        setupDataChannel(pc, channel, targetSid);
    }

    // Timeout: if not connected in time, force relay path
    pc._connectTimer = setTimeout(function() {
        if (!pc.dataChannel || pc.dataChannel.readyState !== "open") {
            var st = pc.iceConnectionState;
            if (st !== "connected" && st !== "completed") {
                log("P2P timeout (" + (P2P_CONNECT_TIMEOUT_MS / 1000) + "s) → relay for " + targetSid.slice(0, 6), "warn");
                p2pFailed[targetSid] = true;
            }
        }
    }, P2P_CONNECT_TIMEOUT_MS);

    connections[targetSid] = pc;
    return pc;
}

function setupDataChannel(pc, channel, targetSid) {
    pc.dataChannel = channel;
    channel.binaryType = "arraybuffer";
    channel.bufferedAmountLowThreshold = P2P_BUFFER_LOW;

    channel.onopen = function() {
        log("P2P open with " + targetSid.slice(0, 6), "p2p");
        delete p2pFailed[targetSid];
        if (pc._connectTimer) { clearTimeout(pc._connectTimer); pc._connectTimer = null; }
    };
    channel.onmessage = function(e) { handleDataMessage(e.data, targetSid); };
    channel.onerror = function(err) {
        log("Channel error: " + (err && err.message ? err.message : err), "error");
    };
    channel.onclose = function() {
        log("P2P channel closed " + targetSid.slice(0, 6), "warn");
    };
}

function handleSignal(data) {
    var fromSid = data.from;
    var signal = data.signal;
    if (!signal) return;

    // Prefer mapping from peer_id if from_peer provided
    if (data.from_peer) {
        var matched = peerList.find(function(p) { return p.id === data.from_peer; });
        if (matched) fromSid = matched.sid;
    }

    var pc = getOrCreateConnection(fromSid, false);
    if (!pc) return;

    var targetPeerId = pc.targetPeerId || resolvePeerId(fromSid);

    if (signal.type === "offer") {
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .then(function() {
                while (pc.iceQueue.length) {
                    try { pc.addIceCandidate(pc.iceQueue.shift()); } catch (e) {}
                }
                return pc.createAnswer();
            })
            .then(function(ans) { return pc.setLocalDescription(ans); })
            .then(function() {
                socket.emit("signal", {
                    to: targetPeerId,
                    signal: { type: "answer", sdp: pc.localDescription }
                });
            })
            .catch(function(err) { log("Offer err: " + err.message, "error"); });
    } else if (signal.type === "answer") {
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .then(function() {
                while (pc.iceQueue.length) {
                    try { pc.addIceCandidate(pc.iceQueue.shift()); } catch (e) {}
                }
            })
            .catch(function(err) { log("Answer err: " + err.message, "error"); });
    } else if (signal.type === "ice") {
        var candidate = new RTCIceCandidate(signal.candidate);
        if (pc.remoteDescription && pc.remoteDescription.type) {
            pc.addIceCandidate(candidate).catch(function() {});
        } else {
            pc.iceQueue.push(candidate);
        }
    }
}

function connectPeer(targetSid) {
    if (!WEBRTC_SUPPORTED) {
        log("WebRTC not supported, using relay only", "warn");
        return;
    }
    if (p2pFailed[targetSid]) {
        log("P2P previously failed for " + targetSid.slice(0, 6) + ", using relay", "info");
        return;
    }
    var existing = connections[targetSid];
    if (existing && existing.dataChannel && existing.dataChannel.readyState === "open") {
        return;
    }
    var pc = getOrCreateConnection(targetSid, true);
    if (!pc) return;
    var targetPeerId = pc.targetPeerId || resolvePeerId(targetSid);
    pc._makingOffer = true;
    pc.createOffer()
        .then(function(offer) { return pc.setLocalDescription(offer); })
        .then(function() {
            socket.emit("signal", {
                to: targetPeerId,
                signal: { type: "offer", sdp: pc.localDescription }
            });
        })
        .catch(function(err) {
            log("Offer err: " + err.message, "error");
            p2pFailed[targetSid] = true;
        })
        .finally(function() { pc._makingOffer = false; });
}

function isP2PReady(targetSid) {
    if (p2pFailed[targetSid]) return false;
    var pc = connections[targetSid];
    return !!(pc && pc.dataChannel && pc.dataChannel.readyState === "open");
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
    if (isP2PReady(targetSid)) {
        connections[targetSid].dataChannel.send(JSON.stringify({ t: "txt", c: text }));
        log("Sent text (P2P)", "p2p");
    } else {
        socket.emit("relay_text", { to: resolvePeerId(targetSid), text: text });
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
    var targetPeerId = resolvePeerId(targetSid);
    var meta = {
        file: file,
        transfer_id: transferId,
        batch_id: batchId,
        batch_total: batchTotal,
        batch_index: batchIndex,
        target_peer_id: targetPeerId
    };

    // Try establish P2P if not ready and not previously failed
    if (WEBRTC_SUPPORTED && !isP2PReady(targetSid) && !p2pFailed[targetSid]) {
        connectPeer(targetSid);
    }

    if (isP2PReady(targetSid)) {
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
    } else {
        // Relay path (immediate or after P2P timeout user can re-send; for now always queue relay)
        if (!relayFileQueue[targetSid]) relayFileQueue[targetSid] = [];
        relayFileQueue[targetSid].push(meta);
        processRelayQueue(targetSid);
    }
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
    var file = item.file;
    var pc = connections[targetSid];
    if (!pc || !pc.dataChannel || pc.dataChannel.readyState !== "open") {
        // Channel died — fall back remaining queue to relay
        log("P2P channel gone, falling back to relay", "warn");
        while (queue.length) {
            var q = queue.shift();
            if (!relayFileQueue[targetSid]) relayFileQueue[targetSid] = [];
            relayFileQueue[targetSid].push(q);
        }
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

    document.getElementById("progress-wrap").style.display = "block";
    document.getElementById("send-status").textContent = "Sending " + file.name + (item.batch_total > 1 ? " (" + (item.batch_index + 1) + "/" + item.batch_total + ")" : "") + " [P2P]";

    // Stream via file.slice to avoid loading entire file into RAM for huge files
    var offset = 0;
    var total = file.size;
    var waitingDrain = false;

    function finishP2P() {
        channel.send(JSON.stringify({ t: "fe", id: item.transfer_id }));
        log("Sent " + file.name + " (P2P)", "success");
        setTimeout(function() { document.getElementById("progress-wrap").style.display = "none"; }, 600);
        queue.shift();
        if (queue.length) {
            var next = queue[0];
            socket.emit("broadcast_request", {
                to: next.target_peer_id || resolvePeerId(targetSid),
                file_name: next.file.name,
                file_size: next.file.size,
                file_type: next.file.type,
                transfer_id: next.transfer_id,
                batch_id: next.batch_id,
                batch_total: next.batch_total,
                batch_index: next.batch_index
            });
        }
    }

    function pumpP2P() {
        if (channel.readyState !== "open") {
            log("Channel closed mid-transfer, fallback relay", "warn");
            p2pFailed[targetSid] = true;
            while (queue.length) {
                var q = queue.shift();
                if (!relayFileQueue[targetSid]) relayFileQueue[targetSid] = [];
                relayFileQueue[targetSid].push(q);
            }
            processRelayQueue(targetSid);
            return;
        }
        while (offset < total) {
            if (channel.bufferedAmount >= P2P_BUFFER_HIGH) {
                if (!waitingDrain) {
                    waitingDrain = true;
                    channel.onbufferedamountlow = function() {
                        waitingDrain = false;
                        channel.onbufferedamountlow = null;
                        pumpP2P();
                    };
                }
                return;
            }
            var end = Math.min(offset + CHUNK_SIZE, total);
            var blob = file.slice(offset, end);
            // Sync-ish read via FileReader for this small slice
            // Use arrayBuffer() when available (modern browsers)
            if (blob.arrayBuffer) {
                // Pause loop; continue after promise
                var curOffset = offset;
                offset = end; // advance optimistically
                blob.arrayBuffer().then(function(buf) {
                    if (channel.readyState === "open") {
                        channel.send(buf);
                        var pct = Math.min(100, Math.round((Math.max(0, curOffset + buf.byteLength - channel.bufferedAmount) / total) * 100));
                        // simpler progress by offset
                        pct = Math.min(100, Math.round((end / total) * 100));
                        document.getElementById("progress-fill").style.width = pct + "%";
                        document.getElementById("send-pct").textContent = pct + "%";
                    }
                    if (offset >= total && channel.bufferedAmount === 0) {
                        finishP2P();
                    } else {
                        pumpP2P();
                    }
                }).catch(function(err) {
                    log("P2P read error: " + err.message, "error");
                });
                return;
            }
            // Fallback FileReader path
            break;
        }
        if (offset >= total) {
            // Wait for buffer drain before done signal
            if (channel.bufferedAmount > 0) {
                channel.onbufferedamountlow = function() {
                    channel.onbufferedamountlow = null;
                    if (channel.bufferedAmount === 0) finishP2P();
                    else pumpP2P();
                };
                // Also poll in case event misses
                setTimeout(function check() {
                    if (channel.bufferedAmount === 0) finishP2P();
                    else setTimeout(check, 50);
                }, 50);
            } else {
                finishP2P();
            }
            return;
        }
        // FileReader fallback for older browsers
        var end2 = Math.min(offset + CHUNK_SIZE, total);
        var reader = new FileReader();
        reader.onload = function(e) {
            if (channel.readyState === "open") {
                channel.send(e.target.result);
                offset = end2;
                var pct = Math.min(100, Math.round((offset / total) * 100));
                document.getElementById("progress-fill").style.width = pct + "%";
                document.getElementById("send-pct").textContent = pct + "%";
                pumpP2P();
            }
        };
        reader.readAsArrayBuffer(file.slice(offset, end2));
    }
    pumpP2P();
}

function relaySendFile(targetSid, targetPeerId, file, transferId, batchId, batchTotal, batchIndex, onComplete) {
    targetPeerId = targetPeerId || targetSid;
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

    var total = file.size;
    var offset = 0;
    var inFlight = 0;
    var MAX_IN_FLIGHT = RELAY_MAX_IN_FLIGHT;
    var MAX_RETRIES = 8;
    var aborted = false;
    var sentBytes = 0;
    var seqCounter = 0;
    var useBinary = true; // Socket.IO binary support

    document.getElementById("progress-wrap").style.display = "block";
    document.getElementById("send-status").textContent = "Sending " + file.name + (batchTotal > 1 ? " (" + (batchIndex + 1) + "/" + batchTotal + ")" : "") + " [Relay]";

    function finishIfDone() {
        if (!aborted && offset >= total && inFlight === 0) {
            socket.emit("relay_file_done", { to: targetPeerId, transfer_id: transferId });
            log("Sent " + file.name + " (Relay)", "success");
            setTimeout(function() { document.getElementById("progress-wrap").style.display = "none"; }, 600);
            if (onComplete) onComplete();
        }
    }

    function sendOneChunk(seq, payload, chunkLen) {
        var attempts = 0;
        function attempt() {
            if (aborted) return;
            socket.emit("relay_file_chunk", {
                to: targetPeerId,
                transfer_id: transferId,
                chunk: payload,
                seq: seq
            }, function(ack) {
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
                    if (onComplete) onComplete();
                }
            });
        }
        attempt();
    }

    function pump() {
        while (!aborted && inFlight < MAX_IN_FLIGHT && offset < total) {
            var end = Math.min(offset + RELAY_CHUNK_SIZE, total);
            var slice = file.slice(offset, end);
            var chunkLen = end - offset;
            var seq = seqCounter++;
            offset = end;
            inFlight++;

            if (useBinary && slice.arrayBuffer) {
                slice.arrayBuffer().then(function(buf) {
                    sendOneChunk(seq, buf, chunkLen);
                }).catch(function() {
                    // fallback base64
                    var reader = new FileReader();
                    reader.onload = function(ev) {
                        var bytes = new Uint8Array(ev.target.result);
                        var binary = "";
                        for (var i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
                        sendOneChunk(seq, btoa(binary), chunkLen);
                    };
                    reader.readAsArrayBuffer(slice);
                });
            } else {
                var reader = new FileReader();
                reader.onload = function(ev) {
                    var bytes = new Uint8Array(ev.target.result);
                    var binary = "";
                    for (var i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
                    sendOneChunk(seq, btoa(binary), chunkLen);
                };
                reader.onerror = function() {
                    inFlight--;
                    aborted = true;
                    log("Failed to read " + file.name, "error");
                    if (onComplete) onComplete();
                };
                reader.readAsArrayBuffer(slice);
            }
        }
    }
    pump();
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

function isImageType(type) {
    return !!(type && type.indexOf("image/") === 0);
}

function isVideoType(type) {
    return !!(type && type.indexOf("video/") === 0);
}

function isAudioType(type) {
    return !!(type && type.indexOf("audio/") === 0);
}

function isMediaType(type) {
    // Gallery-only media (images + videos). Audio is playable but not grid-gallery.
    return isImageType(type) || isVideoType(type);
}

function isPlayableType(type) {
    return isMediaType(type) || isAudioType(type);
}

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

// Prepare preview URL. For HEIC: convert to JPEG for display (original kept for download).
// Returns a Promise that resolves to the same item with previewUrl set.
function prepareItemPreview(item) {
    return new Promise(function(resolve) {
        normalizeItemMime(item);
        item.previewUrl = item.url;
        item.previewReady = true;

        if (!isHeicType(item.type, item.name)) {
            resolve(item);
            return;
        }

        // Safari can often display HEIC natively; still try conversion for consistency
        // and for Chrome/Firefox which cannot.
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
            resolve(item); // fallback: show as file, download still works
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
    // Start with list container; mode decided when batch completes (or progressively)
    li.innerHTML =
        '<div class="feed-header">' +
            '<div class="feed-author">' +
                '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>' +
                '<span>' + escapeHtml(sender) + '</span>' +
            '</div>' +
            '<span class="feed-time">' + time + '</span>' +
        '</div>' +
        '<div class="gallery-meta" id="batch-meta-' + batchId + '">Receiving 0 / ' + total + '…</div>' +
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
        mode: null   // "gallery" | "list" decided after we know composition
    };
    return batchStore[batchId];
}

function decideBatchMode(batch) {
    // Pure image/video → gallery grid. Audio or any non-media → list.
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
            if (item.convertedFromHeic) nameLine += ' <span style="color:#94a3b8;font-weight:400;">(HEIC)</span>';
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
            metaEl.textContent = "Receiving " + done + " / " + total + "…";
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

    // File
    var batchId = data.batch_id;
    var batchTotal = data.batch_total || 1;

    // Multi-file batch → one card, mode depends on content
    if (batchId && batchTotal > 1) {
        addToBatch(batchId, data, sender);
        return;
    }

    // Single file (batch_total 1 or no batch)
    prepareItemPreview(data).then(function(item) {
        var li = document.createElement("li");
        li.className = "feed-item";
        var mediaId = registerMedia(item);
        var titleExtra = item.convertedFromHeic ? ' <span style="color:#94a3b8;font-weight:400;font-size:11px;">(HEIC → preview)</span>' : '';
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
