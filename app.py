from gevent import monkey
monkey.patch_all()

import base64
import logging
import os
import re
import secrets
import time
import uuid
from collections import defaultdict, deque
from functools import wraps

from flask import Flask, render_template_string, request
from flask_socketio import SocketIO, emit, join_room, leave_room, disconnect


APP_VERSION = "3.1.1"

MAX_HTTP_BUFFER = 50 * 1024 * 1024
MAX_FILE_BYTES = 4 * 1024 * 1024 * 1024
MAX_CHUNK_BYTES = 256 * 1024

STALE_TTL = 90
CLEANUP_INTERVAL = 30

MAX_TEXT_BYTES = 8 * 1024 * 1024
MAX_TEXT_CHARS = 8 * 1024 * 1024
TEXT_CHUNK_CHARS = 12_000
TEXT_DIRECT_MAX_CHARS = 24_000
MAX_TEXT_CHUNKS = 1024
TEXT_TRANSFER_TTL = 90
MAX_NAME_LEN = 32
MAX_FILE_NAME_LEN = 255
MAX_FILE_TYPE_LEN = 127
MAX_TRANSFER_ID_LEN = 64
MAX_BATCH_ID_LEN = 64

RATE_LIMIT_WINDOW = 5.0
RATE_LIMIT_MAX_EVENTS = 80
FILECHUNK_RATE_BYTES = 128 * 1024 * 1024
TEXTCHUNK_RATE_LIMIT = 2000
TEXTCHUNK_RATE_WINDOW = 5.0

ROOM_CODE_RE = re.compile(r"^\d{6}$")
NAME_RE = re.compile(r"^[\w .\-]{1,32}$", re.UNICODE)
DEVICE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{16,64}$")
PEER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,32}$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("pairme")

app = Flask(__name__)
app.config["SECRET_KEY"] = (
    os.environ.get("SECRET_KEY")
    or secrets.token_hex(32)
)

allowed_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
cors_origins = (
    [x.strip() for x in allowed_origins.split(",") if x.strip()]
    if allowed_origins != "*"
    else "*"
)

socketio = SocketIO(
    app,
    cors_allowed_origins=cors_origins,
    async_mode="gevent",
    ping_timeout=45,
    ping_interval=20,
    max_http_buffer_size=MAX_HTTP_BUFFER,
    engineio_logger=False,
    logger=False,
)

peers = {}
rooms_index = defaultdict(set)
device_to_peer_id = {}
peer_id_to_sid = {}
peer_last_room = {}
peer_last_name = {}

rate_buckets = defaultdict(deque)
rate_totals = defaultdict(int)
relay_text_sessions = {}

START_TIME = time.monotonic()


def generate_code():
    return f"{secrets.randbelow(900000) + 100000:06d}"


def resolve_target_sid(target):
    if target in peers:
        return target
    return peer_id_to_sid.get(str(target or ""))


def _coerce_int(value, default=0, minimum=None, maximum=None):
    try:
        result = int(value)
    except (TypeError, ValueError):
        result = default

    if minimum is not None:
        result = max(minimum, result)

    if maximum is not None:
        result = min(maximum, result)

    return result


def _clean_text(value, limit):
    value = str(value or "")
    value = value.replace("\x00", "")
    return value[:limit]


def _valid_device_id(value):
    value = _clean_text(value, 64)
    return value if DEVICE_ID_RE.fullmatch(value) else ""


def _valid_peer_id(value):
    value = _clean_text(value, 32)
    return value if PEER_ID_RE.fullmatch(value) else ""


def _valid_room(value):
    value = _clean_text(value, 6)
    return value if ROOM_CODE_RE.fullmatch(value) else ""


def _safe_filename(value):
    value = _clean_text(value, MAX_FILE_NAME_LEN)
    value = value.replace("\r", " ").replace("\n", " ")
    return value or "file"


def _safe_file_type(value):
    value = _clean_text(value, MAX_FILE_TYPE_LEN)
    return value.replace("\r", "").replace("\n", "")


def _safe_transfer_id(value):
    return _clean_text(value, MAX_TRANSFER_ID_LEN)


def _safe_batch_id(value):
    return _clean_text(value, MAX_BATCH_ID_LEN)


def rate_limited(
    sid,
    weight=1,
    bucket="default",
    limit=RATE_LIMIT_MAX_EVENTS,
    window=RATE_LIMIT_WINDOW,
):
    if weight <= 0:
        return False

    if weight > limit:
        return True

    key = (sid, bucket)
    now = time.monotonic()
    queue = rate_buckets[key]
    cutoff = now - window

    while queue and queue[0][0] <= cutoff:
        _, expired_weight = queue.popleft()
        rate_totals[key] -= expired_weight

    used = rate_totals[key]

    if used + weight > limit:
        return True

    queue.append((now, weight))
    rate_totals[key] += weight
    return False


def guarded(
    weight=1,
    bucket="default",
    limit=None,
    window=None,
):
    lim = RATE_LIMIT_MAX_EVENTS if limit is None else limit
    win = RATE_LIMIT_WINDOW if window is None else window

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            sid = request.sid

            info = peers.get(sid)
            if info is not None:
                info["last_seen"] = time.monotonic()

            if rate_limited(
                sid,
                weight,
                bucket,
                lim,
                win,
            ):
                log.warning(
                    "rate limit sid=%s event=%s bucket=%s",
                    sid,
                    fn.__name__,
                    bucket,
                )
                emit(
                    "rate_limited",
                    {"event": fn.__name__},
                )
                return False

            return fn(*args, **kwargs)

        return wrapper

    return decorator


def leave_current_room(sid):
    info = peers.get(sid)
    if not info:
        return

    room = info.get("room") or "Lobby"

    leave_room(room)

    members = rooms_index.get(room)
    if members is not None:
        members.discard(sid)
        if not members:
            rooms_index.pop(room, None)


def drop_rate_buckets(sid):
    for key in tuple(rate_buckets):
        if key[0] == sid:
            rate_buckets.pop(key, None)
            rate_totals.pop(key, None)


def broadcast_peers(room):
    members = rooms_index.get(room)

    if not members:
        return

    payload = [
        {
            "sid": sid,
            "id": info["id"],
            "name": info["name"],
        }
        for sid in members
        if (info := peers.get(sid)) is not None
    ]

    socketio.emit("peers", payload, room=room)


def remove_peer(sid, announce=True):
    info = peers.pop(sid, None)
    if not info:
        return

    room = info.get("room") or "Lobby"
    peer_id = info.get("id")
    device_id = info.get("device_id")

    leave_room(room)

    members = rooms_index.get(room)
    if members is not None:
        members.discard(sid)
        if not members:
            rooms_index.pop(room, None)

    drop_rate_buckets(sid)

    if peer_id and peer_id_to_sid.get(peer_id) == sid:
        peer_id_to_sid.pop(peer_id, None)

    if device_id and device_to_peer_id.get(device_id) == peer_id:
        device_to_peer_id.pop(device_id, None)

    if announce:
        broadcast_peers(room)


def cleanup_stale_peers():
    while True:
        socketio.sleep(CLEANUP_INTERVAL)

        now = time.monotonic()

        expired_text = [
            key
            for key, session in tuple(relay_text_sessions.items())
            if session.get("expires_at", 0) <= now
        ]
        for key in expired_text:
            relay_text_sessions.pop(key, None)

        stale = [
            sid
            for sid, info in tuple(peers.items())
            if now - info.get("last_seen", now) > STALE_TTL
        ]

        for sid in stale:
            log.info("dropping stale peer sid=%s", sid)
            remove_peer(sid)


@app.route("/")
def index():
    return render_template_string(
        HTML_TEMPLATE,
        app_version=APP_VERSION,
    )


@app.route("/health")
def health():
    return {
        "status": "ok",
        "peers": len(peers),
        "version": APP_VERSION,
        "uptime": int(time.monotonic() - START_TIME),
    }, 200


@socketio.on("connect")
def handle_connect():
    sid = request.sid

    device_id = _valid_device_id(
        request.args.get("did")
        or request.args.get("fp")
        or ""
    )

    requested_peer_id = _valid_peer_id(
        request.args.get("pid")
        or ""
    )

    peer_id = device_to_peer_id.get(
        device_id
    ) or requested_peer_id

    if not peer_id:
        peer_id = uuid.uuid4().hex[:12]

    old_sid = peer_id_to_sid.get(peer_id)

    if old_sid and old_sid != sid:
        remove_peer(old_sid, announce=False)
        try:
            disconnect(
                old_sid,
                namespace="/",
                silent=True,
            )
        except Exception:
            pass

    if not device_id:
        device_id = secrets.token_urlsafe(18).replace("-", "_")[:24]

    room = peer_last_room.get(peer_id, "Lobby")
    name = peer_last_name.get(
        peer_id,
        "Device " + peer_id[-4:].upper(),
    )

    peers[sid] = {
        "id": peer_id,
        "name": name,
        "joined": time.monotonic(),
        "last_seen": time.monotonic(),
        "room": room,
        "device_id": device_id,
    }

    peer_id_to_sid[peer_id] = sid
    device_to_peer_id[device_id] = peer_id

    join_room(room)
    rooms_index[room].add(sid)

    emit(
        "init",
        {
            "peer_id": peer_id,
            "sid": sid,
            "room": room,
            "name": name,
        },
    )

    broadcast_peers(room)


@socketio.on("disconnect")
def handle_disconnect():
    remove_peer(request.sid)


@socketio.on("set_name")
@guarded()
def handle_set_name(data):
    sid = request.sid

    if sid not in peers or not isinstance(data, dict):
        return

    raw = _clean_text(
        data.get("name", ""),
        MAX_NAME_LEN,
    ).strip()

    if raw and NAME_RE.fullmatch(raw):
        peers[sid]["name"] = raw
        peer_last_name[peers[sid]["id"]] = raw
        broadcast_peers(peers[sid]["room"])


@socketio.on("join_room_code")
@guarded()
def handle_join_room_code(data):
    sid = request.sid

    if sid not in peers or not isinstance(data, dict):
        return

    code = _valid_room(data.get("code"))

    if not code:
        emit(
            "room_error",
            {"msg": "Invalid code"},
        )
        return

    old_room = peers[sid].get("room")

    if old_room == code:
        emit("room_joined", {"code": code})
        return

    leave_current_room(sid)

    join_room(code)

    peers[sid]["room"] = code
    peer_last_room[peers[sid]["id"]] = code
    rooms_index[code].add(sid)

    emit(
        "room_joined",
        {"code": code},
    )

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

    emit(
        "room_joined",
        {
            "code": code,
            "created": True,
        },
    )

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
    a = peers.get(sid_a)
    b = peers.get(sid_b)

    return bool(
        a
        and b
        and a.get("room") == b.get("room")
    )


@socketio.on("signal")
@guarded(weight=2)
def handle_signal(data):
    sid = request.sid

    if sid not in peers or not isinstance(data, dict):
        return

    target_sid = resolve_target_sid(data.get("to"))

    if (
        target_sid in peers
        and target_sid != sid
        and _same_room(sid, target_sid)
    ):
        emit(
            "signal",
            {
                "from": sid,
                "from_peer": peers[sid]["id"],
                "from_name": peers[sid]["name"],
                "signal": data.get("signal"),
            },
            room=target_sid,
        )


@socketio.on("broadcast_request")
@guarded()
def handle_broadcast_request(data):
    if not isinstance(data, dict):
        return

    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))

    if (
        target_sid in peers
        and target_sid != sid
        and _same_room(sid, target_sid)
    ):
        emit(
            "transfer_request",
            {
                "from": sid,
                "from_peer": peers[sid]["id"],
                "from_name": peers[sid]["name"],
                "file_name": _safe_filename(
                    data.get("file_name", "file")
                ),
                "file_size": _coerce_int(
                    data.get("file_size"),
                    maximum=MAX_FILE_BYTES,
                ),
                "file_type": _safe_file_type(
                    data.get("file_type", "")
                ),
                "transfer_id": _safe_transfer_id(
                    data.get("transfer_id")
                ),
                "batch_id": _safe_batch_id(
                    data.get("batch_id")
                ),
                "batch_total": _coerce_int(
                    data.get("batch_total"),
                    default=1,
                    minimum=1,
                    maximum=10_000,
                ),
                "batch_index": _coerce_int(
                    data.get("batch_index"),
                    maximum=10_000,
                ),
            },
            room=target_sid,
        )


@socketio.on("broadcast_response")
@guarded()
def handle_broadcast_response(data):
    if not isinstance(data, dict):
        return

    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))

    if (
        target_sid in peers
        and target_sid != sid
        and _same_room(sid, target_sid)
    ):
        emit(
            "transfer_response",
            {
                "from": sid,
                "accepted": bool(
                    data.get("accepted", False)
                ),
                "transfer_id": _safe_transfer_id(
                    data.get("transfer_id")
                ),
            },
            room=target_sid,
        )


@socketio.on("relay_text")
@guarded(weight=2)
def handle_relay_text(data):
    if not isinstance(data, dict):
        return

    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))
    text = str(data.get("text", "") or "")

    if (
        target_sid in peers
        and target_sid != sid
        and _same_room(sid, target_sid)
        and text
        and len(text) <= TEXT_DIRECT_MAX_CHARS
        and len(text.encode("utf-8")) <= TEXT_DIRECT_MAX_CHARS * 4
    ):
        emit(
            "relay_text",
            {
                "from": sid,
                "from_name": peers[sid]["name"],
                "text": text,
            },
            room=target_sid,
        )


@socketio.on("relay_text_start")
@guarded(weight=1, bucket="textstart")
def handle_relay_text_start(data):
    if not isinstance(data, dict):
        return

    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))

    if (
        target_sid not in peers
        or target_sid == sid
        or not _same_room(sid, target_sid)
    ):
        return

    transfer_id = _clean_text(
        data.get("transfer_id"),
        MAX_TRANSFER_ID_LEN,
    )

    total_chunks = _coerce_int(
        data.get("total_chunks"),
        minimum=1,
        maximum=MAX_TEXT_CHUNKS,
    )

    total_chars = _coerce_int(
        data.get("total_chars"),
        minimum=1,
        maximum=MAX_TEXT_CHARS,
    )

    if not transfer_id or not total_chunks or not total_chars:
        return

    relay_text_sessions[(sid, transfer_id)] = {
        "target_sid": target_sid,
        "total_chunks": total_chunks,
        "total_chars": total_chars,
        "received_chunks": 0,
        "received_chars": 0,
        "expires_at": time.monotonic() + TEXT_TRANSFER_TTL,
    }

    emit(
        "relay_text_start",
        {
            "from": sid,
            "from_name": peers[sid]["name"],
            "transfer_id": transfer_id,
            "total_chunks": total_chunks,
            "total_chars": total_chars,
        },
        room=target_sid,
    )


@socketio.on("relay_text_chunk")
@guarded(
    weight=1,
    bucket="textchunk",
    limit=TEXTCHUNK_RATE_LIMIT,
    window=TEXTCHUNK_RATE_WINDOW,
)
def handle_relay_text_chunk(data):
    if not isinstance(data, dict):
        return

    sid = request.sid
    transfer_id = _clean_text(
        data.get("transfer_id"),
        MAX_TRANSFER_ID_LEN,
    )
    session = relay_text_sessions.get((sid, transfer_id))

    if not session:
        return

    if time.monotonic() > session["expires_at"]:
        relay_text_sessions.pop((sid, transfer_id), None)
        return

    seq = _coerce_int(
        data.get("seq"),
        minimum=0,
        maximum=session["total_chunks"] - 1,
    )

    chunk = data.get("chunk", "")

    if not isinstance(chunk, str) or not chunk:
        return

    if len(chunk) > TEXT_CHUNK_CHARS:
        return

    if session["received_chunks"] >= session["total_chunks"]:
        return

    new_chars = session["received_chars"] + len(chunk)

    if new_chars > session["total_chars"]:
        relay_text_sessions.pop((sid, transfer_id), None)
        return

    session["received_chunks"] += 1
    session["received_chars"] = new_chars

    emit(
        "relay_text_chunk",
        {
            "from": sid,
            "transfer_id": transfer_id,
            "seq": seq,
            "chunk": chunk,
        },
        room=session["target_sid"],
    )


@socketio.on("relay_text_done")
@guarded(weight=1, bucket="textdone")
def handle_relay_text_done(data):
    if not isinstance(data, dict):
        return

    sid = request.sid
    transfer_id = _clean_text(
        data.get("transfer_id"),
        MAX_TRANSFER_ID_LEN,
    )
    session = relay_text_sessions.pop((sid, transfer_id), None)

    if not session:
        return

    if (
        session["received_chunks"] != session["total_chunks"]
        or session["received_chars"] != session["total_chars"]
    ):
        emit(
            "text_transfer_error",
            {
                "transfer_id": transfer_id,
                "reason": "Incomplete text transfer",
            },
            room=sid,
        )
        return

    emit(
        "relay_text_done",
        {
            "from": sid,
            "transfer_id": transfer_id,
            "total_chunks": session["total_chunks"],
            "total_chars": session["total_chars"],
        },
        room=session["target_sid"],
    )


@socketio.on("relay_file_start")
@guarded()
def handle_relay_file_start(data):
    if not isinstance(data, dict):
        return

    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))

    if (
        target_sid not in peers
        or target_sid == sid
        or not _same_room(sid, target_sid)
    ):
        return

    file_size = _coerce_int(
        data.get("file_size"),
        maximum=MAX_FILE_BYTES,
    )

    if file_size <= 0:
        return

    chunk_size = _coerce_int(
        data.get("chunk_size"),
        default=96 * 1024,
        minimum=1024,
        maximum=MAX_CHUNK_BYTES,
    )

    total_chunks = max(
        1,
        (file_size + chunk_size - 1) // chunk_size,
    )

    emit(
        "relay_file_start",
        {
            "from": sid,
            "from_name": peers[sid]["name"],
            "file_name": _safe_filename(
                data.get("file_name", "file")
            ),
            "file_size": file_size,
            "file_type": _safe_file_type(
                data.get("file_type", "")
            ),
            "transfer_id": _safe_transfer_id(
                data.get("transfer_id")
            ),
            "batch_id": _safe_batch_id(
                data.get("batch_id")
            ),
            "batch_total": _coerce_int(
                data.get("batch_total"),
                default=1,
                minimum=1,
                maximum=10_000,
            ),
            "batch_index": _coerce_int(
                data.get("batch_index"),
                maximum=10_000,
            ),
            "chunk_size": chunk_size,
            "total_chunks": total_chunks,
        },
        room=target_sid,
    )


@socketio.on("relay_file_chunk")
@guarded(
    bucket="file_events",
    limit=1200,
    window=5.0,
)
def handle_relay_file_chunk(data):
    if not isinstance(data, dict):
        return False

    sid = request.sid
    target_sid = resolve_target_sid(
        data.get("to")
    )

    if (
        target_sid not in peers
        or target_sid == sid
        or not _same_room(sid, target_sid)
    ):
        return False

    raw_chunk = data.get("chunk")

    if isinstance(raw_chunk, (bytes, bytearray, memoryview)):
        chunk = bytes(raw_chunk)
    elif isinstance(raw_chunk, str):
        try:
            chunk = base64.b64decode(
                raw_chunk,
                validate=True,
            )
        except Exception:
            return False
    else:
        return False

    if not chunk or len(chunk) > MAX_CHUNK_BYTES:
        return False

    if rate_limited(
        sid,
        len(chunk),
        bucket="file_bytes",
        limit=FILECHUNK_RATE_BYTES,
        window=RATE_LIMIT_WINDOW,
    ):
        emit(
            "rate_limited",
            {"event": "relay_file_chunk"},
        )
        return False

    seq = _coerce_int(
        data.get("seq"),
        minimum=0,
        maximum=(MAX_FILE_BYTES // 1024) + 1,
    )

    transfer_id = _safe_transfer_id(
        data.get("transfer_id")
    )

    socketio.emit(
        "relay_file_chunk",
        {
            "from": sid,
            "transfer_id": transfer_id,
            "chunk": chunk,
            "seq": seq,
        },
        room=target_sid,
    )

    return True


@socketio.on("relay_file_done")
@guarded()
def handle_relay_file_done(data):
    if not isinstance(data, dict):
        return

    sid = request.sid
    target_sid = resolve_target_sid(data.get("to"))

    if (
        target_sid in peers
        and target_sid != sid
        and _same_room(sid, target_sid)
    ):
        emit(
            "relay_file_done",
            {
                "from": sid,
                "transfer_id": _safe_transfer_id(
                    data.get("transfer_id")
                ),
                "total_chunks": _coerce_int(
                    data.get("total_chunks"),
                    default=1,
                    minimum=1,
                ),
            },
            room=target_sid,
        )


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
        .code-block { color: #f8fafc; padding: 10px; font-family: ui-monospace, Consolas, Monaco, monospace; font-size: 12px; line-height: 1.45; overflow: auto; max-height: 420px; white-space: pre; word-break: normal; -webkit-overflow-scrolling: touch; }
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
                    <span style="font-size:11px;color:var(--muted2);">Live Photo: select both the HEIC and MOV files</span>
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
<script>
/* ========== PairMe client v3.1 ========== */
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
var relayReceivedBytes = {};
var relayReceivedCount = {};
var relayCleanupTimers = {};
var relayTextBuffers = {};
var relayTextMeta = {};
var relayTextTimers = {};
var textSendChains = {};
var textStore = {};
var textStoreOrder = [];
var textStoreBytes = 0;
var MAX_TEXT_STORE_BYTES = 32 * 1024 * 1024;
var batchStore = {};
var mediaRegistry = {};
var lightboxItems = [];
var lightboxIndex = 0;
var lightboxMode = {};

var P2P_CHUNK_SIZE = 32 * 1024;
var RELAY_CHUNK_SIZE = 96 * 1024;
var P2P_LOW_WATER = 256 * 1024;
var P2P_HIGH_WATER = 1024 * 1024;
var RELAY_WINDOW = 16;
var RELAY_MAX_RETRIES = 7;
var RELAY_RETRY_BASE = 80;
var TEXT_CHUNK_CHARS = 12000;
var TEXT_DIRECT_MAX_CHARS = 24000;
var MAX_TEXT_BYTES = 8 * 1024 * 1024;
var TEXT_TRANSFER_TIMEOUT = 90000;

var STUN_SERVERS = {
    iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun.cloudflare.com:3478" }
    ],
    iceCandidatePoolSize: 2
};

var WEBRTC_SUPPORTED =
    (typeof window.RTCPeerConnection === "function");

var activeTransfers = 0;
var peerConnState = {};
var MAX_PREWARM_PEERS = 2;
var peerRenderRaf = 0;
var progressRaf = 0;
var progressState = null;
var p2pFallbackTimers = {};
var prewarmTimers = {};
var toastTimer = null;
var pendingShareOpen = false;
var urlRoomCode = null;
var openDlPop = null;
var P2P_CONNECT_TIMEOUT = 7000;
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

    var el =
        document.getElementById(
            "log-container"
        );

    if (!el) return;

    var wasAtBottom =
        el.scrollHeight
        - el.scrollTop
        - el.clientHeight
        < 32;

    var now = new Date();

    var time =
        now.getHours()
        + ":"
        + ("0" + now.getMinutes()).slice(-2)
        + ":"
        + ("0" + now.getSeconds()).slice(-2);

    var entry =
        document.createElement("div");

    entry.className = "log-entry";

    var timeEl =
        document.createElement("span");

    timeEl.className = "log-time";
    timeEl.textContent = time;

    var tagEl =
        document.createElement("span");

    tagEl.className =
        "log-tag tag-" + type;
    tagEl.textContent = type;

    var msgEl =
        document.createElement("span");

    msgEl.style.wordBreak =
        "break-all";
    msgEl.textContent =
        String(msg);

    entry.appendChild(timeEl);
    entry.appendChild(tagEl);
    entry.appendChild(msgEl);

    el.appendChild(entry);

    while (el.childElementCount > 160) {
        el.firstElementChild.remove();
    }

    if (wasAtBottom) {
        el.scrollTop =
            el.scrollHeight;
    }
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
    bytes = Number(bytes) || 0;

    if (bytes <= 0) {
        return "0 B";
    }

    var units = [
        "B",
        "KB",
        "MB",
        "GB",
        "TB"
    ];

    var index = Math.floor(
        Math.log(bytes) / Math.log(1024)
    );

    index = Math.max(
        0,
        Math.min(
            units.length - 1,
            index
        )
    );

    var value =
        bytes / Math.pow(1024, index);

    return (
        parseFloat(
            value.toFixed(
                index === 0 ? 0 : 1
            )
        )
        + " "
        + units[index]
    );
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

/* ---------- QR / Share ---------- */

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

/* ---------- Formatting ---------- */

function rememberText(textId, text) {
    if (Object.prototype.hasOwnProperty.call(textStore, textId)) {
        textStoreBytes -= utf8ByteLength(textStore[textId]);
        var oldPos = textStoreOrder.indexOf(textId);
        if (oldPos >= 0) textStoreOrder.splice(oldPos, 1);
    }

    rememberText(textId, text);
    textStoreOrder.push(textId);
    textStoreBytes += utf8ByteLength(text);

    while (textStoreBytes > MAX_TEXT_STORE_BYTES && textStoreOrder.length > 1) {
        var oldest = textStoreOrder.shift();
        if (!oldest || oldest === textId) continue;
        textStoreBytes -= utf8ByteLength(textStore[oldest] || "");
        delete textStore[oldest];
    }
}

function clearTextRelayState(id) {
    if (relayTextTimers[id]) {
        clearTimeout(relayTextTimers[id]);
        delete relayTextTimers[id];
    }
    delete relayTextBuffers[id];
    delete relayTextMeta[id];
}

function renderFormattedContent(text, textId) {
    if (!text) return "";
    rememberText(textId, text);
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

/* ---------- Adaptive transport core ---------- */

function setProgress(label, completed, total) {
    progressState = {
        label: label || "",
        completed: Math.max(0, completed || 0),
        total: Math.max(1, total || 1)
    };

    if (progressRaf) return;

    var flush = function() {
        progressRaf = 0;

        var state = progressState;
        if (!state) return;

        var wrap = document.getElementById("progress-wrap");
        var status = document.getElementById("send-status");
        var pct = document.getElementById("send-pct");
        var fill = document.getElementById("progress-fill");

        if (!wrap || !status || !pct || !fill) return;

        var ratio = Math.min(
            1,
            state.completed / state.total
        );

        status.textContent = state.label;
        pct.textContent = Math.round(ratio * 100) + "%";
        fill.style.width = (ratio * 100) + "%";
    };

    if (typeof requestAnimationFrame === "function") {
        progressRaf = requestAnimationFrame(flush);
    } else {
        progressRaf = setTimeout(flush, 32);
    }
}

function hideProgressSoon() {
    setTimeout(function() {
        var wrap = document.getElementById("progress-wrap");
        if (wrap && activeTransfers <= 0) {
            wrap.style.display = "none";
        }
    }, 400);
}

function makeRandomId() {
    try {
        if (window.crypto && crypto.randomUUID) {
            return crypto.randomUUID();
        }

        if (window.crypto && crypto.getRandomValues) {
            var bytes = new Uint8Array(18);
            crypto.getRandomValues(bytes);

            var output = "";
            for (var i = 0; i < bytes.length; i++) {
                output += bytes[i].toString(16).padStart(2, "0");
            }
            return output;
        }
    } catch (e) {}

    return (
        Date.now().toString(36)
        + "_"
        + Math.random().toString(36).slice(2)
    );
}

function generateTransferId() {
    return makeRandomId();
}

/* ---------- Local identity ---------- */

function createLocalDeviceId() {
    return makeRandomId().replace(
        /[^A-Za-z0-9_-]/g,
        ""
    ).slice(0, 48);
}

function getOrCreateDeviceId() {
    try {
        var value = localStorage.getItem(
            "pairme_device_id"
        );

        if (
            value
            && /^[A-Za-z0-9_-]{16,64}$/.test(value)
        ) {
            return Promise.resolve(value);
        }

        var created = createLocalDeviceId();

        localStorage.setItem(
            "pairme_device_id",
            created
        );

        return Promise.resolve(created);
    } catch (e) {
        return Promise.resolve(
            createLocalDeviceId()
        );
    }
}

/* ---------- Socket ---------- */

function initSocket() {
    getOrCreateDeviceId().then(function(deviceId) {
        var savedPeerId = "";

        try {
            savedPeerId =
                localStorage.getItem(
                    "pairme_peer_id"
                ) || "";
        } catch (e) {}

        var query = { did: deviceId };

        if (
            /^[A-Za-z0-9_-]{8,32}$/.test(
                savedPeerId
            )
        ) {
            query.pid = savedPeerId;
        }

        socket = io({
            transports: ["websocket", "polling"],
            upgrade: true,
            rememberUpgrade: true,
            query: query,
            reconnection: true,
            reconnectionAttempts: Infinity,
            reconnectionDelay: 300,
            reconnectionDelayMax: 5000,
            randomizationFactor: 0.35,
            timeout: 12000
        });

        bindSocketEvents();
    });
}

function closeAllConnections() {
    Object.keys(connections).forEach(function(sid) {
        var pc = connections[sid];

        if (pc) {
            pc._manualClose = true;

            if (pc.receiveTimers) {
                Object.keys(pc.receiveTimers).forEach(
                    function(id) {
                        clearTimeout(
                            pc.receiveTimers[id]
                        );
                    }
                );
            }

            try {
                pc.close();
            } catch (e) {}
        }

        delete connections[sid];
        delete peerConnState[sid];
    });

    Object.keys(p2pFallbackTimers).forEach(function(sid) {
        clearTimeout(
            p2pFallbackTimers[sid]
        );
        delete p2pFallbackTimers[sid];
    });

    schedulePeerRender();
}

function bindSocketEvents() {
    socket.on("connect", function() {
        log("Connected to server", "success");

        var saved = localStorage.getItem(
            "pairme_name"
        );

        if (saved) {
            document.getElementById(
                "my-name"
            ).value = saved;

            socket.emit(
                "set_name",
                { name: saved }
            );
        }
    });

    socket.on("init", function(data) {
        mySid = data.sid;
        myPeerId = data.peer_id;
        currentRoom = data.room || "Lobby";

        try {
            localStorage.setItem(
                "pairme_peer_id",
                myPeerId
            );
        } catch (e) {}

        document.getElementById(
            "my-id"
        ).textContent = myPeerId;

        document.getElementById(
            "room-name"
        ).textContent = currentRoom;

        if (data.name) {
            document.getElementById(
                "my-name"
            ).value = data.name;
        }

        if (
            urlRoomCode
            && urlRoomCode !== currentRoom
        ) {
            socket.emit(
                "join_room_code",
                { code: urlRoomCode }
            );
        } else {
            syncUrlWithRoom(currentRoom);
        }

        urlRoomCode = null;
    });

    socket.on("peers", function(data) {
        peerList = Array.isArray(data)
            ? data.filter(function(p) {
                return p
                    && p.sid
                    && p.sid !== mySid;
            })
            : [];

        renderPeers();
        pruneConnections();
        prewarmPeers();
    });

    socket.on("signal", handleSignal);

    socket.on("transfer_request", function(data) {
        socket.emit(
            "broadcast_response",
            {
                to: data.from,
                accepted: true,
                transfer_id: data.transfer_id
            }
        );
    });

    socket.on("transfer_response", function(data) {
        if (data.accepted) {
            startDataTransfer(
                data.from,
                data.transfer_id
            );
        }
    });

    socket.on("room_joined", function(data) {
        currentRoom = data.code;

        document.getElementById(
            "room-name"
        ).textContent = data.code;

        syncUrlWithRoom(data.code);
        closeAllConnections();

        if (pendingShareOpen) {
            pendingShareOpen = false;
            populateShareModal();

            document.getElementById(
                "share-modal"
            ).style.display = "flex";
        } else if (
            document.getElementById(
                "share-modal"
            ).style.display === "flex"
        ) {
            populateShareModal();
        }
    });

    socket.on("room_left", function() {
        currentRoom = "Lobby";

        document.getElementById(
            "room-name"
        ).textContent = "Lobby";

        syncUrlWithRoom("Lobby");
        closeShareModal();
        closeAllConnections();
    });

    socket.on("room_error", function(data) {
        log(
            "Room error: "
            + (
                data && data.msg
                    ? data.msg
                    : "unknown"
            ),
            "error"
        );
        showToast("Invalid room code");
    });

    socket.on("relay_text", function(data) {
        if (!data || typeof data.text !== "string") return;
        addReceived(
            "text",
            data.text,
            data.from_name
        );
    });

    socket.on("relay_text_start", function(data) {
        var id = String(data && data.transfer_id || "");
        var totalChunks = Number(data && data.total_chunks || 0);
        var totalChars = Number(data && data.total_chars || 0);

        if (
            !id
            || !Number.isInteger(totalChunks)
            || totalChunks < 1
            || totalChunks > 1024
            || !Number.isInteger(totalChars)
            || totalChars < 1
            || totalChars > MAX_TEXT_BYTES
        ) {
            return;
        }

        clearTextRelayState(id);

        relayTextBuffers[id] = new Array(totalChunks);
        relayTextMeta[id] = {
            sender: data.from_name || "Unknown",
            totalChunks: totalChunks,
            totalChars: totalChars,
            received: 0,
            receivedChars: 0
        };

        relayTextTimers[id] = setTimeout(
            function() {
                clearTextRelayState(id);
            },
            TEXT_TRANSFER_TIMEOUT
        );
    });

    socket.on("relay_text_chunk", function(data) {
        var id = String(data && data.transfer_id || "");
        var meta = relayTextMeta[id];
        var chunks = relayTextBuffers[id];

        if (!meta || !chunks || !Array.isArray(chunks)) return;

        var seq = Number(data.seq);
        var chunk = data.chunk;

        if (
            !Number.isInteger(seq)
            || seq < 0
            || seq >= meta.totalChunks
            || typeof chunk !== "string"
            || !chunk
            || chunk.length > TEXT_CHUNK_CHARS
        ) {
            return;
        }

        if (chunks[seq] !== undefined) return;

        var nextChars = meta.receivedChars + chunk.length;

        if (nextChars > meta.totalChars) {
            clearTextRelayState(id);
            return;
        }

        chunks[seq] = chunk;
        meta.received++;
        meta.receivedChars = nextChars;
    });

    socket.on("relay_text_done", function(data) {
        var id = String(data && data.transfer_id || "");
        var meta = relayTextMeta[id];
        var chunks = relayTextBuffers[id];

        if (!meta || !chunks) return;

        if (
            meta.received !== meta.totalChunks
            || meta.receivedChars !== meta.totalChars
        ) {
            log("Text transfer incomplete", "error");
            clearTextRelayState(id);
            return;
        }

        var assembled = chunks.join("");

        addReceived(
            "text",
            assembled,
            meta.sender
        );

        clearTextRelayState(id);
    });

    socket.on("text_transfer_error", function(data) {
        var id = String(data && data.transfer_id || "");
        clearTextRelayState(id);
        log(
            "Text transfer failed: "
            + String(data && data.reason || "unknown error"),
            "error"
        );
    });

    socket.on("relay_file_start", function(data) {
        var id = String(
            data.transfer_id || ""
        );

        if (relayBuffer[id]) {
            delete relayBuffer[id];
            delete relayMeta[id];
            delete relayReceivedBytes[id];
            delete relayReceivedCount[id];
        }

        var size = Number(
            data.file_size || 0
        );

        if (!id || size <= 0) return;

        var chunkSize = Math.max(
            1024,
            Number(data.chunk_size)
                || RELAY_CHUNK_SIZE
        );

        var count = Number(
            data.total_chunks
        );

        if (
            !Number.isInteger(count)
            || count < 1
        ) {
            count = Math.ceil(
                size / chunkSize
            );
        }

        if (relayCleanupTimers[id]) {
            clearTimeout(
                relayCleanupTimers[id]
            );
        }

        relayBuffer[id] = new Array(count);
        relayMeta[id] = data;
        relayReceivedBytes[id] = 0;
        relayReceivedCount[id] = 0;

        relayCleanupTimers[id] = setTimeout(
            function() {
                delete relayBuffer[id];
                delete relayMeta[id];
                delete relayReceivedBytes[id];
                delete relayReceivedCount[id];
                delete relayCleanupTimers[id];
            },
            10 * 60 * 1000
        );

        log(
            "Receiving " + data.file_name,
            "info"
        );
    });

    socket.on("relay_file_chunk", function(data) {
        var id = String(
            data.transfer_id || ""
        );

        var chunks = relayBuffer[id];

        if (!chunks) return;

        var seq = Number(data.seq);

        if (
            !Number.isInteger(seq)
            || seq < 0
            || seq >= chunks.length
            || chunks[seq]
        ) {
            return;
        }

        var chunk = data.chunk;
        var buffer = null;

        if (chunk instanceof ArrayBuffer) {
            buffer = chunk;
        } else if (
            typeof ArrayBuffer !== "undefined"
            && ArrayBuffer.isView
            && ArrayBuffer.isView(chunk)
        ) {
            buffer = chunk.buffer.slice(
                chunk.byteOffset,
                chunk.byteOffset
                    + chunk.byteLength
            );
        } else if (
            chunk instanceof Blob
        ) {
            var localId = id;
            var localSeq = seq;

            chunk.arrayBuffer().then(
                function(ab) {
                    var current =
                        relayBuffer[localId];

                    if (
                        !current
                        || current[localSeq]
                    ) {
                        return;
                    }

                    current[localSeq] = ab;
                    relayReceivedCount[localId]++;
                    relayReceivedBytes[localId] += ab.byteLength;
                }
            ).catch(function() {});

            return;
        }

        if (!buffer) return;

        chunks[seq] = buffer;
        relayReceivedCount[id]++;
        relayReceivedBytes[id] += buffer.byteLength;
    });

    socket.on("relay_file_done", function(data) {
        var id = String(
            data.transfer_id || ""
        );

        var meta = relayMeta[id];
        var chunks = relayBuffer[id];

        if (!meta || !chunks) return;

        var expectedChunks =
            Number(meta.total_chunks)
            || chunks.length;

        var complete =
            relayReceivedCount[id]
                === expectedChunks
            && relayReceivedBytes[id]
                === Number(meta.file_size)
            && chunks.every(Boolean);

        if (!complete) {
            log(
                "Relay integrity failure: "
                + meta.file_name,
                "error"
            );

            delete relayBuffer[id];
            delete relayMeta[id];
            delete relayReceivedBytes[id];
            delete relayReceivedCount[id];

            if (relayCleanupTimers[id]) {
                clearTimeout(
                    relayCleanupTimers[id]
                );
                delete relayCleanupTimers[id];
            }

            return;
        }

        var blob = new Blob(
            chunks,
            {
                type:
                    meta.file_type
                    || "application/octet-stream"
            }
        );

        var url = trackObjectUrl(URL.createObjectURL(blob));

        addReceived(
            "file",
            {
                name: meta.file_name,
                size: meta.file_size,
                url: url,
                type: meta.file_type,
                blob: blob,
                batch_id:
                    meta.batch_id || "",
                batch_total:
                    meta.batch_total || 1,
                batch_index:
                    meta.batch_index || 0
            },
            meta.from_name
        );

        log(
            "Received "
            + meta.file_name
            + " (Relay)",
            "success"
        );

        delete relayBuffer[id];
        delete relayMeta[id];
        delete relayReceivedBytes[id];
        delete relayReceivedCount[id];

        if (relayCleanupTimers[id]) {
            clearTimeout(
                relayCleanupTimers[id]
            );
            delete relayCleanupTimers[id];
        }
    });

    socket.on("rate_limited", function(data) {
        log(
            "Rate limited: "
            + (
                data && data.event
                    ? data.event
                    : "request"
            ),
            "warn"
        );
    });

    socket.on("disconnect", function() {
        Object.keys(connections).forEach(
            function(sid) {
                setPeerConnState(
                    sid,
                    "relay"
                );
            }
        );

        log(
            "Disconnected from server",
            "warn"
        );
    });
}

/* ---------- Peer management ---------- */

function isDataChannelOpen(sid) {
    var pc = connections[sid];

    return !!(
        pc
        && pc.dataChannel
        && pc.dataChannel.readyState === "open"
    );
}

function setPeerConnState(sid, state) {
    peerConnState[sid] = state;
    schedulePeerRender();
}

function pruneConnections() {
    var known = {};

    peerList.forEach(function(p) {
        known[p.sid] = true;
    });

    Object.keys(connections).forEach(
        function(sid) {
            if (known[sid]) return;

            try {
                connections[sid].close();
            } catch (e) {}

            delete connections[sid];
            delete peerConnState[sid];
        }
    );
}

function renderPeers() {
    var list = document.getElementById(
        "peer-list"
    );

    var select = document.getElementById(
        "peer-select"
    );

    if (!list || !select) return;

    var selected = select.value;

    var fragment =
        document.createDocumentFragment();

    var empty = !peerList.length;

    if (empty) {
        var hint = document.createElement("div");
        hint.className = "empty-hint";
        hint.textContent =
            "No devices detected";
        fragment.appendChild(hint);
    } else {
        peerList.forEach(function(p) {
            var state =
                peerConnState[p.sid]
                || (
                    isDataChannelOpen(p.sid)
                        ? "p2p"
                        : "relay"
                );

            var item =
                document.createElement("div");

            item.className = "peer-item";
            item.onclick = function() {
                selectPeer(p.sid);
            };

            var info =
                document.createElement("div");

            info.className = "peer-info";

            var name =
                document.createElement("span");

            name.className = "peer-name";
            name.textContent = p.name || "Device";

            var id =
                document.createElement("span");

            id.className = "peer-id";
            id.textContent = p.id || "";

            info.appendChild(name);
            info.appendChild(id);

            var right =
                document.createElement("div");

            right.style.cssText =
                "display:flex;align-items:center;gap:6px;";

            var status =
                document.createElement("span");

            status.className =
                "peer-status "
                + (
                    state === "p2p"
                        ? "status-p2p"
                        : state === "connecting"
                            ? "status-conn"
                            : "status-relay"
                );

            status.textContent =
                state === "p2p"
                    ? "P2P"
                    : state === "connecting"
                        ? "…"
                        : "Relay";

            right.appendChild(status);

            var arrow =
                document.createElement("span");

            arrow.textContent = "›";
            arrow.style.fontSize = "20px";
            arrow.style.lineHeight = "1";
            right.appendChild(arrow);

            item.appendChild(info);
            item.appendChild(right);
            fragment.appendChild(item);

            var opt =
                document.createElement("option");

            opt.value = p.sid;
            opt.textContent =
                (p.name || "Device")
                + " ("
                + (p.id || "")
                + ")";

            select.appendChild(opt);
        });
    }

    list.replaceChildren(fragment);

    if (selected) {
        select.value = selected;
    }
}

function selectPeer(sid) {
    var select = document.getElementById(
        "peer-select"
    );

    select.value = sid;
    onPeerSelectChange();

    if (window.innerWidth <= 768) {
        switchTab("transfer");
    }
}

function onPeerSelectChange() {
    var sid = document.getElementById(
        "peer-select"
    ).value;

    var label = document.getElementById(
        "target-peer-label"
    );

    if (sid) {
        var p = peerList.find(function(x) {
            return x.sid === sid;
        });

        label.textContent =
            "To: " + (
                p ? p.name : sid
            );

        connectPeer(sid, true);
    } else {
        label.textContent =
            "To: Everyone";
    }
}

function prewarmPeers() {
    if (!WEBRTC_SUPPORTED) return;

    var targets = [];

    var selected =
        document.getElementById(
            "peer-select"
        ).value;

    if (selected) {
        targets.push(selected);
    }

    for (var i = 0; i < peerList.length; i++) {
        if (
            targets.length >= MAX_PREWARM_PEERS
            || targets.indexOf(
                peerList[i].sid
            ) >= 0
        ) {
            continue;
        }

        targets.push(peerList[i].sid);
    }

    targets.forEach(function(sid, index) {
        if (isDataChannelOpen(sid)) return;
        if (prewarmTimers[sid]) return;

        prewarmTimers[sid] = setTimeout(
            function() {
                delete prewarmTimers[sid];
                connectPeer(sid, false);
            },
            index * 120
        );
    });
}

/* ---------- Device / room controls ---------- */

function updateName() {
    var name =
        document.getElementById(
            "my-name"
        ).value.trim();

    if (!name) return;

    socket.emit(
        "set_name",
        { name: name }
    );

    localStorage.setItem(
        "pairme_name",
        name
    );

    log(
        "Updated device name",
        "success"
    );
}

function joinRoom() {
    var code =
        document.getElementById(
            "room-code-input"
        ).value.trim();

    if (/^\d{6}$/.test(code)) {
        socket.emit(
            "join_room_code",
            { code: code }
        );
    }
}

function createRoom() {
    socket.emit("create_room_code");
}

function leaveRoom() {
    socket.emit("leave_room_code");
}

/* ---------- WebRTC ---------- */

function scheduleConnectionRetry(
    targetSid,
    pc
) {
    if (
        pc._manualClose
        || pc.connectionState === "connected"
        || !connections[targetSid]
        || connections[targetSid] !== pc
    ) {
        return;
    }

    pc._retryCount = (
        pc._retryCount || 0
    );

    if (pc._retryCount >= 3) {
        fallbackPendingToRelay(targetSid);
        return;
    }

    pc._retryCount++;

    var delay =
        Math.min(
            3500,
            400 * Math.pow(
                2,
                pc._retryCount - 1
            )
        );

    setTimeout(function() {
        if (
            pc._manualClose
            || !connections[targetSid]
            || connections[targetSid] !== pc
        ) {
            return;
        }

        try {
            pc.restartIce();
        } catch (e) {}

        makeOffer(
            pc,
            targetSid
        );
    }, delay);
}

function getOrCreateConnection(
    targetSid,
    isInitiator
) {
    if (!WEBRTC_SUPPORTED) {
        return null;
    }

    var existing =
        connections[targetSid];

    if (
        existing
        && existing.connectionState !== "closed"
        && existing.connectionState !== "failed"
    ) {
        return existing;
    }

    if (existing) {
        try {
            existing.close();
        } catch (e) {}
    }

    var pc = new RTCPeerConnection(
        STUN_SERVERS
    );

    pc.iceQueue = [];
    pc.targetSid = targetSid;
    pc.receiveTransfers = {};
    pc.receiveTimers = {};
    pc.activeReceiveId = null;
    pc._makingOffer = false;
    pc._ignoreOffer = false;
    pc._manualClose = false;
    pc._polite = (
        String(mySid)
        > String(targetSid)
    );
    pc._retryCount = 0;

    connections[targetSid] = pc;

    setPeerConnState(
        targetSid,
        "connecting"
    );

    pc.onicecandidate = function(e) {
        if (!e.candidate || !socket) return;

        socket.emit(
            "signal",
            {
                to: targetSid,
                signal: {
                    type: "ice",
                    candidate: e.candidate
                }
            }
        );
    };

    pc.onconnectionstatechange =
        function() {
            var state =
                pc.connectionState;

            if (state === "connected") {
                pc._retryCount = 0;
                setPeerConnState(
                    targetSid,
                    isDataChannelOpen(targetSid)
                        ? "p2p"
                        : "connecting"
                );
                startNextP2PTransfer(
                    targetSid
                );
            } else if (
                state === "disconnected"
                || state === "failed"
            ) {
                setPeerConnState(
                    targetSid,
                    "relay"
                );

                scheduleConnectionRetry(
                    targetSid,
                    pc
                );
            } else if (state === "closed") {
                setPeerConnState(
                    targetSid,
                    "relay"
                );

                fallbackPendingToRelay(
                    targetSid
                );
            }
        };

    pc.oniceconnectionstatechange =
        function() {
            var state =
                pc.iceConnectionState;

            if (state === "failed") {
                scheduleConnectionRetry(
                    targetSid,
                    pc
                );
            }
        };

    pc.ondatachannel = function(e) {
        setupDataChannel(
            pc,
            e.channel,
            targetSid
        );
    };

    if (isInitiator) {
        var channel =
            pc.createDataChannel(
                "pairme",
                {
                    ordered: true,
                    negotiated: false
                }
            );

        setupDataChannel(
            pc,
            channel,
            targetSid
        );
    }

    return pc;
}

function setupDataChannel(
    pc,
    channel,
    targetSid
) {
    pc.dataChannel = channel;
    channel.binaryType = "arraybuffer";

    try {
        channel.bufferedAmountLowThreshold =
            P2P_LOW_WATER;
    } catch (e) {}

    channel.onopen = function() {
        pc._retryCount = 0;

        setPeerConnState(
            targetSid,
            "p2p"
        );

        startNextP2PTransfer(
            targetSid
        );
    };

    channel.onclose = function() {
        setPeerConnState(
            targetSid,
            "relay"
        );

        if (!pc._manualClose) {
            fallbackPendingToRelay(
                targetSid
            );
        }
    };

    channel.onerror = function() {
        setPeerConnState(
            targetSid,
            "failed"
        );
    };

    channel.onmessage = function(e) {
        handleDataMessage(
            e.data,
            targetSid
        );
    };
}

function makeOffer(
    pc,
    targetSid
) {
    if (
        pc._makingOffer
        || pc._manualClose
    ) {
        return;
    }

    if (
        pc.signalingState !== "stable"
        && pc.signalingState !== "have-local-offer"
    ) {
        return;
    }

    pc._makingOffer = true;

    pc.createOffer()
        .then(function(offer) {
            return pc.setLocalDescription(
                offer
            );
        })
        .then(function() {
            if (!socket) return;

            socket.emit(
                "signal",
                {
                    to: targetSid,
                    signal: {
                        type: "offer",
                        sdp: pc.localDescription
                    }
                }
            );
        })
        .catch(function(err) {
            log(
                "Offer failed: "
                + (
                    err.message || err
                ),
                "warn"
            );
        })
        .finally(function() {
            pc._makingOffer = false;
        });
}

function flushIceQueue(pc) {
    if (
        !pc.remoteDescription
        || !pc.remoteDescription.type
    ) {
        return Promise.resolve();
    }

    var queue = pc.iceQueue.splice(
        0,
        pc.iceQueue.length
    );

    return Promise.all(
        queue.map(function(candidate) {
            return pc.addIceCandidate(
                candidate
            ).catch(function() {});
        })
    ).then(function() {});
}

function handleSignal(data) {
    if (!data || !data.signal) return;

    var fromSid = data.from;
    var signal = data.signal;

    if (!fromSid || fromSid === mySid) {
        return;
    }

    var pc =
        getOrCreateConnection(
            fromSid,
            false
        );

    if (!pc) return;

    if (signal.type === "offer") {
        var offerCollision =
            pc._makingOffer
            || pc.signalingState !== "stable";

        pc._ignoreOffer =
            !pc._polite
            && offerCollision;

        if (pc._ignoreOffer) {
            return;
        }

        var rollback =
            offerCollision
            && pc._polite;

        var promise = Promise.resolve();

        if (rollback) {
            promise =
                pc.setLocalDescription({
                    type: "rollback"
                }).catch(function() {});
        }

        promise
            .then(function() {
                return pc.setRemoteDescription(
                    signal.sdp
                );
            })
            .then(function() {
                return flushIceQueue(
                    pc
                );
            })
            .then(function() {
                return pc.createAnswer();
            })
            .then(function(answer) {
                return pc.setLocalDescription(
                    answer
                );
            })
            .then(function() {
                if (!socket) return;

                socket.emit(
                    "signal",
                    {
                        to: fromSid,
                        signal: {
                            type: "answer",
                            sdp: pc.localDescription
                        }
                    }
                );
            })
            .catch(function(err) {
                log(
                    "Negotiation failed: "
                    + (
                        err.message || err
                    ),
                    "warn"
                );
            });

    } else if (signal.type === "answer") {
        if (
            pc.signalingState !==
            "have-local-offer"
        ) {
            return;
        }

        pc.setRemoteDescription(
            signal.sdp
        )
            .then(function() {
                return flushIceQueue(
                    pc
                );
            })
            .catch(function(err) {
                log(
                    "Answer failed: "
                    + (
                        err.message || err
                    ),
                    "warn"
                );
            });

    } else if (signal.type === "ice") {
        try {
            var candidate =
                new RTCIceCandidate(
                    signal.candidate
                );

            if (
                pc.remoteDescription
                && pc.remoteDescription.type
            ) {
                pc.addIceCandidate(
                    candidate
                ).catch(function() {});
            } else {
                pc.iceQueue.push(
                    candidate
                );
            }
        } catch (e) {}
    }
}

function connectPeer(
    targetSid,
    force
) {
    if (!WEBRTC_SUPPORTED) {
        return;
    }

    if (
        isDataChannelOpen(targetSid)
        && !force
    ) {
        return;
    }

    var pc =
        getOrCreateConnection(
            targetSid,
            true
        );

    if (!pc) return;

    if (
        pc.signalingState === "stable"
    ) {
        makeOffer(
            pc,
            targetSid
        );
    }
}

/* ---------- Text / file routing ---------- */

function utf8ByteLength(text) {
    try {
        return new TextEncoder().encode(text).byteLength;
    } catch (e) {
        return unescape(encodeURIComponent(text)).length;
    }
}

function splitTextForTransport(text) {
    var parts = [];

    for (var i = 0; i < text.length; i += TEXT_CHUNK_CHARS) {
        parts.push(
            text.slice(i, i + TEXT_CHUNK_CHARS)
        );
    }

    return parts;
}

function sendText() {
    var input = document.getElementById("text-input");
    var text = input.value;

    if (!text.trim()) return;

    var size = utf8ByteLength(text);

    if (size > MAX_TEXT_BYTES) {
        showToast("Text is too large. Maximum is 8 MB.");
        return;
    }

    var targetSid = document.getElementById("peer-select").value;

    if (targetSid) {
        queueTextSend(
            targetSid,
            text
        );
    } else {
        peerList.forEach(function(p) {
            queueTextSend(
                p.sid,
                text
            );
        });
    }

    input.value = "";
}

function queueTextSend(targetSid, text) {
    var chain = textSendChains[targetSid] || Promise.resolve();

    textSendChains[targetSid] = chain
        .then(function() {
            return sendTextTo(
                targetSid,
                text
            );
        })
        .catch(function() {
            log("Text send failed", "error");
        });
}

async function sendTextTo(
    targetSid,
    text
) {
    var chunks = splitTextForTransport(text);

    if (chunks.length === 1 && text.length <= TEXT_DIRECT_MAX_CHARS) {
        if (isDataChannelOpen(targetSid)) {
            try {
                connections[targetSid].dataChannel.send(
                    JSON.stringify({
                        t: "txt",
                        c: text
                    })
                );
                return;
            } catch (e) {}
        }

        var pInfo = peerList.find(function(x) {
            return x.sid === targetSid;
        });

        if (socket && socket.connected) {
            socket.emit(
                "relay_text",
                {
                    to: pInfo ? pInfo.id : targetSid,
                    text: text
                }
            );
            return;
        }

        throw new Error("No transport available");
    }

    var transferId = generateTransferId();

    if (isDataChannelOpen(targetSid)) {
        var channel = connections[targetSid].dataChannel;

        channel.send(
            JSON.stringify({
                t: "txs",
                id: transferId,
                tc: chunks.length,
                n: text.length
            })
        );

        for (var i = 0; i < chunks.length; i++) {
            if (channel.readyState !== "open") {
                throw new Error("Data channel closed");
            }

            if (channel.bufferedAmount >= P2P_HIGH_WATER) {
                await waitForDataChannelDrain(channel);
            }

            channel.send(
                JSON.stringify({
                    t: "txc",
                    id: transferId,
                    q: i,
                    c: chunks[i]
                })
            );
        }

        channel.send(
            JSON.stringify({
                t: "txe",
                id: transferId
            })
        );

        return;
    }

    var pInfo = peerList.find(function(x) {
        return x.sid === targetSid;
    });

    if (!socket || !socket.connected) {
        throw new Error("No transport available");
    }

    socket.emit(
        "relay_text_start",
        {
            to: pInfo ? pInfo.id : targetSid,
            transfer_id: transferId,
            total_chunks: chunks.length,
            total_chars: text.length
        }
    );

    for (var j = 0; j < chunks.length; j++) {
        socket.emit(
            "relay_text_chunk",
            {
                to: pInfo ? pInfo.id : targetSid,
                transfer_id: transferId,
                seq: j,
                chunk: chunks[j]
            }
        );

        if ((j & 31) === 31) {
            await new Promise(function(resolve) {
                setTimeout(resolve, 0);
            });
        }
    }

    socket.emit(
        "relay_text_done",
        {
            to: pInfo ? pInfo.id : targetSid,
            transfer_id: transferId
        }
    );
}

function handleFileSelect(e) {
    var files =
        e.target.files
        || (
            e.dataTransfer
            && e.dataTransfer.files
        );

    if (!files || !files.length) {
        return;
    }

    if (!peerList.length) {
        showToast(
            "No devices yet. Tap QR to invite one."
        );
        return;
    }

    var targetSid =
        document.getElementById(
            "peer-select"
        ).value;

    var batchId =
        "b_" + generateTransferId();

    var fileArr =
        Array.prototype.slice.call(
            files
        );

    var total =
        fileArr.length;

    fileArr.forEach(function(
        file,
        index
    ) {
        if (targetSid) {
            sendFileTo(
                targetSid,
                file,
                batchId,
                total,
                index
            );
        } else {
            peerList.forEach(function(p) {
                sendFileTo(
                    p.sid,
                    file,
                    batchId,
                    total,
                    index
                );
            });
        }
    });

    document.getElementById(
        "file-input"
    ).value = "";

    if (total > 1) {
        log(
            "Queued batch of "
            + total
            + " files",
            "info"
        );
    }
}

function sendFileTo(
    targetSid,
    file,
    batchId,
    batchTotal,
    batchIndex
) {
    if (
        !file
        || !Number.isFinite(file.size)
        || file.size <= 0
    ) {
        return;
    }

    if (
        file.size
        > 4 * 1024 * 1024 * 1024
    ) {
        showToast(
            "File exceeds 4 GB limit"
        );
        return;
    }

    batchId =
        batchId
        || ("b_" + generateTransferId());

    batchTotal =
        batchTotal || 1;

    batchIndex =
        typeof batchIndex === "number"
            ? batchIndex
            : 0;

    var transferId =
        generateTransferId();

    var pInfo = peerList.find(
        function(x) {
            return x.sid === targetSid;
        }
    );

    var targetPeerId =
        pInfo
            ? pInfo.id
            : targetSid;

    var meta = {
        file: file,
        transfer_id: transferId,
        batch_id: batchId,
        batch_total: batchTotal,
        batch_index: batchIndex,
        target_peer_id: targetPeerId
    };

    if (!pendingFileQueue[targetSid]) {
        pendingFileQueue[targetSid] = [];
    }

    pendingFileQueue[targetSid].push(
        meta
    );

    if (
        isDataChannelOpen(targetSid)
    ) {
        startNextP2PTransfer(
            targetSid
        );
        return;
    }

    connectPeer(
        targetSid,
        true
    );

    armP2PFallback(
        targetSid
    );
}

function armP2PFallback(targetSid) {
    if (
        p2pFallbackTimers[targetSid]
    ) {
        return;
    }

    p2pFallbackTimers[targetSid] =
        setTimeout(function() {
            delete p2pFallbackTimers[
                targetSid
            ];

            if (
                isDataChannelOpen(targetSid)
            ) {
                startNextP2PTransfer(
                    targetSid
                );
                return;
            }

            fallbackPendingToRelay(
                targetSid
            );
        }, P2P_CONNECT_TIMEOUT);
}

function fallbackPendingToRelay(
    targetSid
) {
    var queue =
        pendingFileQueue[targetSid];

    if (
        !queue
        || !queue.length
    ) {
        return;
    }

    var copy = queue.splice(
        0,
        queue.length
    );

    if (!relayFileQueue[targetSid]) {
        relayFileQueue[targetSid] = [];
    }

    Array.prototype.push.apply(
        relayFileQueue[targetSid],
        copy
    );

    setPeerConnState(
        targetSid,
        "relay"
    );

    processRelayQueue(
        targetSid
    );
}

function processRelayQueue(
    targetSid
) {
    var queue =
        relayFileQueue[targetSid];

    if (!queue) return;

    if (queue._active === undefined) {
        queue._active = 0;
    }

    while (
        queue._active < 3
        && queue.length
    ) {
        var item = queue.shift();

        queue._active++;

        relaySendFile(
            targetSid,
            item.target_peer_id,
            item.file,
            item.transfer_id,
            item.batch_id,
            item.batch_total,
            item.batch_index,
            function() {
                queue._active--;

                processRelayQueue(
                    targetSid
                );
            }
        );
    }
}

function waitForDataChannelDrain(
    channel
) {
    return new Promise(function(resolve) {
        if (
            channel.readyState !== "open"
            || channel.bufferedAmount
                <= P2P_LOW_WATER
        ) {
            resolve();
            return;
        }

        var settled = false;

        function finish() {
            if (settled) return;

            settled = true;

            try {
                channel.removeEventListener(
                    "bufferedamountlow",
                    finish
                );
            } catch (e) {}

            clearTimeout(timer);
            resolve();
        }

        channel.addEventListener(
            "bufferedamountlow",
            finish,
            { once: true }
        );

        var timer = setTimeout(
            finish,
            120
        );
    });
}

async function startNextP2PTransfer(
    targetSid
) {
    var queue =
        pendingFileQueue[targetSid];

    if (
        !queue
        || !queue.length
    ) {
        return;
    }

    var pc =
        connections[targetSid];

    if (
        !pc
        || !pc.dataChannel
        || pc.dataChannel.readyState
            !== "open"
    ) {
        return;
    }

    if (pc._sending) {
        return;
    }

    pc._sending = true;

    var item = queue[0];
    var file = item.file;
    var channel = pc.dataChannel;

    var totalBytes = file.size;
    var totalChunks =
        Math.ceil(
            totalBytes
            / P2P_CHUNK_SIZE
        );

    try {
        channel.send(
            JSON.stringify({
                t: "fs",
                n: file.name,
                s: totalBytes,
                m:
                    file.type
                    || "application/octet-stream",
                id: item.transfer_id,
                bid: item.batch_id,
                bt: item.batch_total,
                bi: item.batch_index,
                tc: totalChunks
            })
        );

        activeTransfers++;
        updateTransferBadge();

        var offset = 0;

        while (
            offset < totalBytes
            && channel.readyState === "open"
        ) {
            if (
                channel.bufferedAmount
                >= P2P_HIGH_WATER
            ) {
                await waitForDataChannelDrain(
                    channel
                );
            }

            if (
                channel.readyState !== "open"
            ) {
                throw new Error(
                    "Data channel closed"
                );
            }

            var end = Math.min(
                offset
                + P2P_CHUNK_SIZE,
                totalBytes
            );

            var buffer =
                await file.slice(
                    offset,
                    end
                ).arrayBuffer();

            channel.send(buffer);

            offset = end;

            setProgress(
                "P2P " + file.name,
                offset,
                totalBytes
            );
        }

        if (
            channel.readyState !== "open"
        ) {
            throw new Error(
                "Data channel closed"
            );
        }

        channel.send(
            JSON.stringify({
                t: "fe",
                id: item.transfer_id
            })
        );

        queue.shift();

        activeTransfers--;
        updateTransferBadge();
        hideProgressSoon();

        log(
            "Sent " + file.name + " (P2P)",
            "success"
        );

        pc._retryCount = 0;

        setPeerConnState(
            targetSid,
            "p2p"
        );

    } catch (err) {
        try {
            if (
                channel.readyState
                === "open"
            ) {
                channel.send(
                    JSON.stringify({
                        t: "fa",
                        id: item.transfer_id
                    })
                );
            }
        } catch (e) {}

        if (queue[0] === item) {
            queue.shift();
        }

        activeTransfers = Math.max(
            0,
            activeTransfers - 1
        );
        updateTransferBadge();

        var relayItem = {
            file: file,
            transfer_id:
                generateTransferId(),
            batch_id:
                item.batch_id,
            batch_total:
                item.batch_total,
            batch_index:
                item.batch_index,
            target_peer_id:
                item.target_peer_id
        };

        if (!relayFileQueue[targetSid]) {
            relayFileQueue[targetSid] = [];
        }

        relayFileQueue[targetSid].unshift(
            relayItem
        );

        setPeerConnState(
            targetSid,
            "relay"
        );

        log(
            "P2P failed, resumed through relay: "
            + file.name,
            "warn"
        );

        processRelayQueue(
            targetSid
        );
    } finally {
        pc._sending = false;

        if (
            pendingFileQueue[targetSid]
            && pendingFileQueue[targetSid].length
            && isDataChannelOpen(targetSid)
        ) {
            queueMicrotask(function() {
                startNextP2PTransfer(
                    targetSid
                );
            });
        }
    }
}

function startDataTransfer(
    targetSid,
    transferId
) {
    var queue =
        pendingFileQueue[targetSid];

    if (
        queue
        && queue.length
        && (
            !transferId
            || queue[0].transfer_id
                === transferId
        )
    ) {
        startNextP2PTransfer(
            targetSid
        );
    }
}

function relaySendFile(
    targetSid,
    targetPeerId,
    file,
    transferId,
    batchId,
    batchTotal,
    batchIndex,
    onComplete
) {
    if (
        !socket
        || !socket.connected
    ) {
        if (onComplete) onComplete();
        return;
    }

    var chunkSize =
        RELAY_CHUNK_SIZE;

    var totalBytes =
        file.size;

    var totalChunks =
        Math.ceil(
            totalBytes
            / chunkSize
        );

    socket.emit(
        "relay_file_start",
        {
            to: targetPeerId,
            transfer_id: transferId,
            file_name: file.name,
            file_size: totalBytes,
            file_type:
                file.type
                || "application/octet-stream",
            batch_id: batchId,
            batch_total: batchTotal,
            batch_index: batchIndex,
            chunk_size: chunkSize,
            total_chunks: totalChunks
        }
    );

    activeTransfers++;
    updateTransferBadge();

    var offset = 0;
    var nextSeq = 0;
    var inFlight = 0;
    var completedBytes = 0;
    var settled = false;
    var filling = false;
    var pending = new Map();

    function cleanup() {
        pending.forEach(
            function(entry) {
                if (entry.timer) {
                    clearTimeout(
                        entry.timer
                    );
                }
            }
        );

        pending.clear();
    }

    function finish(ok) {
        if (settled) return;

        settled = true;
        cleanup();

        if (ok) {
            socket.emit(
                "relay_file_done",
                {
                    to: targetPeerId,
                    transfer_id:
                        transferId,
                    total_chunks:
                        totalChunks
                }
            );

            log(
                "Sent " + file.name
                + " (Relay)",
                "success"
            );
        } else {
            log(
                "Relay failed: "
                + file.name,
                "error"
            );
        }

        activeTransfers = Math.max(
            0,
            activeTransfers - 1
        );

        updateTransferBadge();
        hideProgressSoon();

        if (onComplete) {
            onComplete();
        }
    }

    function sendChunk(
        seq,
        buffer,
        byteLength
    ) {
        var entry = {
            buffer: buffer,
            byteLength: byteLength,
            attempts: 0,
            timer: null,
            acknowledged: false
        };

        pending.set(
            seq,
            entry
        );

        function attempt() {
            if (settled) return;

            entry.attempts++;

            var completed = false;

            if (entry.timer) {
                clearTimeout(entry.timer);
            }

            entry.timer = setTimeout(
                function() {
                    if (completed || settled) {
                        return;
                    }

                    completed = true;

                    if (
                        entry.attempts
                        <= RELAY_MAX_RETRIES
                    ) {
                        setTimeout(
                            attempt,
                            Math.min(
                                1500,
                                RELAY_RETRY_BASE
                                * Math.pow(
                                    1.7,
                                    entry.attempts - 1
                                )
                            )
                        );
                    } else {
                        finish(false);
                    }
                },
                9000
            );

            socket.emit(
                "relay_file_chunk",
                {
                    to: targetPeerId,
                    transfer_id:
                        transferId,
                    chunk: buffer,
                    seq: seq
                },
                function(ack) {
                    if (
                        completed
                        || settled
                    ) {
                        return;
                    }

                    completed = true;
                    clearTimeout(
                        entry.timer
                    );
                    entry.timer = null;

                    if (!ack) {
                        if (
                            entry.attempts
                            <= RELAY_MAX_RETRIES
                        ) {
                            setTimeout(
                                attempt,
                                Math.min(
                                    1500,
                                    RELAY_RETRY_BASE
                                    * Math.pow(
                                        1.7,
                                        entry.attempts - 1
                                    )
                                )
                            );
                        } else {
                            finish(false);
                        }
                        return;
                    }

                    pending.delete(seq);
                    inFlight--;
                    completedBytes +=
                        byteLength;

                    setProgress(
                        "Relay " + file.name,
                        completedBytes,
                        totalBytes
                    );

                    pump();
                }
            );
        }

        attempt();
    }

    async function pump() {
        if (
            filling
            || settled
        ) {
            return;
        }

        filling = true;

        try {
            while (
                !settled
                && inFlight < RELAY_WINDOW
                && offset < totalBytes
            ) {
                var start = offset;
                var end = Math.min(
                    offset + chunkSize,
                    totalBytes
                );

                offset = end;

                var buffer =
                    await file.slice(
                        start,
                        end
                    ).arrayBuffer();

                if (settled) {
                    return;
                }

                var seq = nextSeq++;

                inFlight++;

                sendChunk(
                    seq,
                    buffer,
                    buffer.byteLength
                );
            }
        } catch (err) {
            finish(false);
        } finally {
            filling = false;
        }

        if (
            !settled
            && offset >= totalBytes
            && inFlight === 0
        ) {
            finish(true);
        }
    }

    setProgress(
        "Relay " + file.name,
        0,
        totalBytes
    );

    pump();
}

/* ---------- P2P receive ---------- */

function finalizeP2PReceive(
    pc,
    fromSid,
    transferId
) {
    var transfer =
        pc.receiveTransfers[
            transferId
        ];

    if (!transfer) return;

    var complete =
        transfer.receivedBytes
            === transfer.meta.s
        && transfer.receivedChunks
            === transfer.totalChunks;

    if (!complete) {
        delete pc.receiveTransfers[
            transferId
        ];

        if (
            pc.receiveTimers
            && pc.receiveTimers[transferId]
        ) {
            clearTimeout(
                pc.receiveTimers[transferId]
            );
            delete pc.receiveTimers[
                transferId
            ];
        }

        if (
            pc.activeReceiveId
            === transferId
        ) {
            pc.activeReceiveId = null;
        }

        log(
            "P2P integrity failure for "
            + transfer.meta.n,
            "error"
        );

        return;
    }

    var blob = new Blob(
        transfer.chunks,
        {
            type:
                transfer.meta.m
                || "application/octet-stream"
        }
    );

    var url =
        URL.createObjectURL(blob);

    var sender =
        peerList.find(function(p) {
            return p.sid === fromSid;
        });

    addReceived(
        "file",
        {
            name: transfer.meta.n,
            size: transfer.meta.s,
            url: url,
            type: transfer.meta.m,
            blob: blob,
            batch_id:
                transfer.meta.bid || "",
            batch_total:
                transfer.meta.bt || 1,
            batch_index:
                transfer.meta.bi || 0
        },
        sender
            ? sender.name
            : fromSid.slice(0, 6)
    );

    delete pc.receiveTransfers[
        transferId
    ];

    if (
        pc.activeReceiveId
        === transferId
    ) {
        pc.activeReceiveId = null;
    }

    log(
        "Received "
        + transfer.meta.n
        + " (P2P)",
        "success"
    );
}

function handleDataMessage(
    data,
    fromSid
) {
    var pc =
        connections[fromSid];

    if (!pc) return;

    if (typeof data === "string") {
        try {
            var msg = JSON.parse(data);

            if (msg.t === "txt") {
                var sender =
                    peerList.find(
                        function(p) {
                            return p.sid === fromSid;
                        }
                    );

                addReceived(
                    "text",
                    String(msg.c || ""),
                    sender
                        ? sender.name
                        : fromSid.slice(0, 6)
                );

            } else if (msg.t === "txs") {
                var textId = String(msg.id || "");
                var textTotal = Number(msg.n) || 0;
                var textChunks = Number(msg.tc) || 0;

                if (
                    !textId
                    || !textTotal
                    || textTotal > MAX_TEXT_BYTES
                    || !textChunks
                    || textChunks > 1024
                ) {
                    return;
                }

                if (pc.textTransfers && pc.textTransfers[textId]) {
                    delete pc.textTransfers[textId];
                }

                pc.textTransfers = pc.textTransfers || {};
                pc.textTransfers[textId] = {
                    chunks: new Array(textChunks),
                    totalChunks: textChunks,
                    totalChars: textTotal,
                    receivedChunks: 0,
                    receivedChars: 0,
                    timer: setTimeout(function() {
                        if (pc.textTransfers) {
                            delete pc.textTransfers[textId];
                        }
                    }, TEXT_TRANSFER_TIMEOUT)
                };

            } else if (msg.t === "txc") {
                var current = pc.textTransfers && pc.textTransfers[String(msg.id || "")];
                if (!current) return;

                var seq = Number(msg.q);
                var chunk = typeof msg.c === "string" ? msg.c : "";

                if (
                    !Number.isInteger(seq)
                    || seq < 0
                    || seq >= current.totalChunks
                    || !chunk
                    || chunk.length > TEXT_CHUNK_CHARS
                    || current.chunks[seq] !== undefined
                ) {
                    return;
                }

                var receivedChars =
                    current.receivedChars + chunk.length;

                if (receivedChars > current.totalChars) {
                    clearTimeout(current.timer);
                    delete pc.textTransfers[String(msg.id || "")];
                    return;
                }

                current.chunks[seq] = chunk;
                current.receivedChunks++;
                current.receivedChars = receivedChars;

            } else if (msg.t === "txe") {
                var transfer = pc.textTransfers && pc.textTransfers[String(msg.id || "")];
                if (!transfer) return;

                if (
                    transfer.receivedChunks !== transfer.totalChunks
                    || transfer.receivedChars !== transfer.totalChars
                ) {
                    clearTimeout(transfer.timer);
                    delete pc.textTransfers[String(msg.id || "")];
                    log("P2P text transfer incomplete", "error");
                    return;
                }

                var assembledText = transfer.chunks.join("");
                clearTimeout(transfer.timer);
                delete pc.textTransfers[String(msg.id || "")];

                var textSender = peerList.find(
                    function(p) {
                        return p.sid === fromSid;
                    }
                );

                addReceived(
                    "text",
                    assembledText,
                    textSender
                        ? textSender.name
                        : fromSid.slice(0, 6)
                );

            } else if (msg.t === "fs") {
                var total =
                    Number(msg.s) || 0;

                if (
                    !msg.id
                    || total <= 0
                ) {
                    return;
                }

                var totalChunks =
                    Number(msg.tc)
                    || Math.ceil(
                        total
                        / P2P_CHUNK_SIZE
                    );

                pc.receiveTransfers[
                    msg.id
                ] = {
                    meta: msg,
                    chunks: [],
                    receivedBytes: 0,
                    receivedChunks: 0,
                    totalChunks: totalChunks
                };

                if (
                    pc.receiveTimers
                    && pc.receiveTimers[msg.id]
                ) {
                    clearTimeout(
                        pc.receiveTimers[msg.id]
                    );
                }

                pc.receiveTimers[msg.id] =
                    setTimeout(
                        function() {
                            delete pc.receiveTransfers[
                                msg.id
                            ];

                            if (
                                pc.activeReceiveId
                                === msg.id
                            ) {
                                pc.activeReceiveId =
                                    null;
                            }

                            delete pc.receiveTimers[
                                msg.id
                            ];
                        },
                        10 * 60 * 1000
                    );

                pc.activeReceiveId =
                    msg.id;

            } else if (
                msg.t === "fe"
            ) {
                finalizeP2PReceive(
                    pc,
                    fromSid,
                    msg.id
                );

            } else if (
                msg.t === "fa"
            ) {
                delete pc.receiveTransfers[
                    msg.id
                ];

                if (
                    pc.receiveTimers
                    && pc.receiveTimers[msg.id]
                ) {
                    clearTimeout(
                        pc.receiveTimers[msg.id]
                    );
                    delete pc.receiveTimers[
                        msg.id
                    ];
                }

                if (
                    pc.activeReceiveId
                    === msg.id
                ) {
                    pc.activeReceiveId = null;
                }
            }
        } catch (e) {
            log(
                "Invalid P2P control message",
                "error"
            );
        }

        return;
    }

    var transferId =
        pc.activeReceiveId;

    if (!transferId) return;

    var transfer =
        pc.receiveTransfers[
            transferId
        ];

    if (!transfer) return;

    var buffer = null;

    if (data instanceof ArrayBuffer) {
        buffer = data;
    } else if (
        ArrayBuffer.isView(data)
    ) {
        buffer = data.buffer.slice(
            data.byteOffset,
            data.byteOffset
                + data.byteLength
        );
    } else if (
        data instanceof Blob
    ) {
        data.arrayBuffer().then(
            function(bufferFromBlob) {
                var current =
                    pc.receiveTransfers[
                        transferId
                    ];

                if (!current) return;

                current.chunks.push(
                    bufferFromBlob
                );

                current.receivedBytes +=
                    bufferFromBlob.byteLength;

                current.receivedChunks++;
            }
        ).catch(function() {});
        return;
    }

    if (!buffer) return;

    transfer.chunks.push(buffer);
    transfer.receivedBytes +=
        buffer.byteLength;
    transfer.receivedChunks++;
}

function updateTransferBadge() {
    var badge =
        document.getElementById(
            "transfer-badge"
        );

    if (!badge) return;

    if (activeTransfers > 0) {
        badge.style.display =
            "inline-block";

        badge.textContent =
            "Transferring "
            + activeTransfers;
    } else {
        badge.style.display =
            "none";
    }
}

/* ---------- End adaptive transport core ---------- */

/* ---------- Media helpers ---------- */

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
    var id = "m_" + generateTransferId() + "_" + Math.floor(Math.random() * 1000);
    mediaRegistry[id] = item;
    return id;
}

/* ---------- HEIC: two-tier lazy conversion with cache ---------- */

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

// Low-level fallback: decode with libheif-js directly, draw to canvas, export JPEG.
// Catches HDR / 10-bit HEIC variants that heic2any's bundled libheif can't parse
// (e.g. ERR_LIBHEIF format not supported from newer iPhone camera output).
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
        return tryHeic2any(blob).catch(function(err1) {
            log("HEIC convert (heic2any) failed, trying fallback decoder: " + (err1 && err1.message ? err1.message : err1), "warn");
            return tryLibheifJs(blob).catch(function(err2) {
                log("HEIC convert (fallback) failed: " + (err2 && err2.message ? err2.message : err2), "warn");
                item.previewFailed = true;
                throw err2;
            });
        });
    }).then(function(jpegBlob) {
        item.jpegBlob = jpegBlob;
        item.jpegUrl = trackObjectUrl(URL.createObjectURL(jpegBlob));
        item.previewUrl = item.jpegUrl;
        item.convertedFromHeic = true;
        item.previewFailed = false;
        return jpegBlob;
    });

    item._jpegPromise.catch(function() {
        // already logged above; swallow so .catch() callers don't see unhandled rejection noise
    });
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

// HTML for a thumbnail-sized "no preview" placeholder (grid / batch rows).
function heicFallbackThumbHtml() {
    return '<div class="heic-fallback"><span class="heic-chip">HEIC</span></div>';
}

// HTML for a larger "no preview" placeholder (single-file card / feed).
function heicFallbackCardHtml() {
    return '<div class="heic-fallback">' +
        '<span class="heic-chip">HEIC</span>' +
        '<span class="heic-sub">Preview not supported in this browser.<br>Original file is intact, download to view.</span>' +
    '</div>';
}

/* ---------- Live Photo pairing ---------- */

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

/* ---------- Download helpers ---------- */

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

/* ---------- Live Photo UI ---------- */

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

/* ---------- Batch UI ---------- */

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
    batchStore[batchId] = {
        sender: sender,
        items: [],
        total: total,
        cardEl: li,
        mediaIds: [],
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
    });
}

/* ---------- Lightbox ---------- */

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

/* ---------- Batch download ---------- */

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

var ownedObjectUrls = new Set();

function trackObjectUrl(url) {
    if (url) ownedObjectUrls.add(url);
    return url;
}

function revokeTrackedObjectUrls() {
    ownedObjectUrls.forEach(function(url) {
        try {
            URL.revokeObjectURL(url);
        } catch (e) {}
    });
    ownedObjectUrls.clear();
}

window.addEventListener("beforeunload", function(e) {
    revokeTrackedObjectUrls();

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
    urlRoomCode = readRoomFromUrl();
    if (urlRoomCode) {
        switchTab("transfer");
    }
    initSocket();
};
</script>
</body>
</html>
"""

if __name__ == "__main__":
    socketio.start_background_task(cleanup_stale_peers)
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
