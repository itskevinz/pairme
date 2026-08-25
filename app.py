import os
import re
import time
import uuid
import hashlib
import base64
import json
from collections import defaultdict
from datetime import datetime, timedelta
from flask import Flask, request, send_from_directory, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room

app = Flask(__name__)
app.config["SECRET_KEY"] = os.urandom(24)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", max_http_buffer_size=50 * 1024 * 1024)

# ─── Config ───────────────────────────────────────────────────────────────────
ROOM_CODE_LEN = 6
NAME_RE = re.compile(r"^[\w\-\s]{1,20}$")
RATE_LIMIT = 60
RATE_WINDOW = 60
FILECHUNK_RATE_LIMIT = 900
CHUNK_SIZE = 65536
MAX_IN_FLIGHT = 10
RELAY_BATCH_CONCURRENCY = 2

# ─── State ────────────────────────────────────────────────────────────────────
peers = {}
rooms_index = defaultdict(set)
fp_to_peer_id = {}
peer_id_to_sid = {}
peer_last_room = {}
peer_last_name = {}
rate_buckets = defaultdict(list)

# ─── Helpers ──────────────────────────────────────────────────────────────────
def generate_code():
    return hashlib.sha256(os.urandom(32)).hexdigest()[:ROOM_CODE_LEN].upper()

def resolve_target_sid(to):
    if to in peers:
        return to
    return peer_id_to_sid.get(to)

def drop_rate_buckets(sid):
    rate_buckets.pop(sid, None)

def check_rate(sid, limit):
    now = time.time()
    bucket = rate_buckets[sid]
    cutoff = now - RATE_WINDOW
    while bucket and bucket[0] < cutoff:
        bucket.pop(0)
    if len(bucket) >= limit:
        return False
    bucket.append(now)
    return True

def leave_current_room(sid):
    info = peers.get(sid, {})
    room = info.get("room", "Lobby")
    if room and room in rooms_index:
        rooms_index[room].discard(sid)
        if not rooms_index[room]:
            rooms_index.pop(room, None)
    try:
        leave_room(room, sid=sid)
    except Exception:
        pass

def broadcast_peers(room):
    if room not in rooms_index:
        return
    members = []
    for sid in list(rooms_index[room]):
        p = peers.get(sid)
        if p:
            members.append({"sid": sid, "id": p["id"], "name": p["name"]})
    for sid in list(rooms_index[room]):
        emit("peers", {"room": room, "peers": members}, room=sid, namespace="/")

# ─── Socket Events ────────────────────────────────────────────────────────────
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
        peer_id_to_sid.pop(peer_id, None)
    if room:
        broadcast_peers(room)

@socketio.on("ping_keepalive")
def handle_ping_keepalive():
    sid = request.sid
    if sid in peers:
        peers[sid]["last_seen"] = time.time()

@socketio.on("set_name")
def handle_set_name(data):
    sid = request.sid
    if sid not in peers:
        return
    raw = str(data.get("name", "")).strip()
    if raw and NAME_RE.match(raw):
        peers[sid]["name"] = raw
        peer_last_name[peers[sid]["id"]] = raw
        broadcast_peers(peers[sid]["room"])

@socketio.on("create_room_code")
def handle_create_room_code():
    sid = request.sid
    if sid not in peers:
        return
    code = generate_code()
    leave_current_room(sid)
    peers[sid]["room"] = code
    peer_last_room[peers[sid]["id"]] = code
    join_room(code)
    rooms_index[code].add(sid)
    emit("room_created", {"code": code})
    broadcast_peers(code)

@socketio.on("join_room_code")
def handle_join_room_code(data):
    sid = request.sid
    if sid not in peers:
        return
    code = str(data.get("code", "")).upper().strip()
    if not code or len(code) != ROOM_CODE_LEN:
        emit("error", {"msg": "Invalid room code"})
        return
    leave_current_room(sid)
    peers[sid]["room"] = code
    peer_last_room[peers[sid]["id"]] = code
    join_room(code)
    rooms_index[code].add(sid)
    emit("room_joined", {"code": code})
    broadcast_peers(code)

@socketio.on("leave_room_code")
def handle_leave_room_code():
    sid = request.sid
    if sid not in peers:
        return
    leave_current_room(sid)
    peers[sid]["room"] = "Lobby"
    peer_last_room[peers[sid]["id"]] = "Lobby"
    join_room("Lobby")
    rooms_index["Lobby"].add(sid)
    emit("room_left", {"room": "Lobby"})
    broadcast_peers("Lobby")

@socketio.on("signal")
def handle_signal(data):
    sid = request.sid
    if sid not in peers:
        return
    target_sid = resolve_target_sid(data.get("to"))
    if not target_sid or target_sid not in peers:
        return
    if peers[sid]["room"] != peers[target_sid]["room"]:
        return
    if not check_rate(sid, RATE_LIMIT):
        return
    emit("signal", {"from": sid, "data": data.get("data")}, room=target_sid)

@socketio.on("broadcast_request")
def handle_broadcast_request(data):
    sid = request.sid
    if sid not in peers:
        return
    target_sid = resolve_target_sid(data.get("to"))
    if not target_sid or target_sid not in peers:
        return
    if peers[sid]["room"] != peers[target_sid]["room"]:
        return
    emit("broadcast_request", {
        "from": sid,
        "from_id": peers[sid]["id"],
        "from_name": peers[sid]["name"],
        "transfer_id": data.get("transfer_id"),
        "batch_id": data.get("batch_id"),
        "batch_total": data.get("batch_total"),
        "batch_index": data.get("batch_index"),
        "file_name": data.get("file_name"),
        "file_size": data.get("file_size"),
        "file_type": data.get("file_type"),
    }, room=target_sid)

@socketio.on("broadcast_response")
def handle_broadcast_response(data):
    sid = request.sid
    if sid not in peers:
        return
    target_sid = resolve_target_sid(data.get("to"))
    if not target_sid or target_sid not in peers:
        return
    emit("broadcast_response", {
        "from": sid,
        "from_id": peers[sid]["id"],
        "accepted": data.get("accepted"),
        "transfer_id": data.get("transfer_id"),
    }, room=target_sid)

@socketio.on("relay_text")
def handle_relay_text(data):
    sid = request.sid
    if sid not in peers:
        return
    target_sid = resolve_target_sid(data.get("to"))
    if not target_sid or target_sid not in peers:
        return
    if peers[sid]["room"] != peers[target_sid]["room"]:
        return
    if not check_rate(sid, RATE_LIMIT):
        return
    emit("relay_text", {
        "from": sid,
        "from_id": peers[sid]["id"],
        "from_name": peers[sid]["name"],
        "text": data.get("text"),
    }, room=target_sid)

@socketio.on("relay_file_start")
def handle_relay_file_start(data):
    sid = request.sid
    if sid not in peers:
        return
    target_sid = resolve_target_sid(data.get("to"))
    if not target_sid or target_sid not in peers:
        return
    if peers[sid]["room"] != peers[target_sid]["room"]:
        return
    emit("relay_file_start", {
        "from": sid,
        "from_id": peers[sid]["id"],
        "from_name": peers[sid]["name"],
        "transfer_id": data.get("transfer_id"),
        "batch_id": data.get("batch_id"),
        "batch_total": data.get("batch_total"),
        "batch_index": data.get("batch_index"),
        "file_name": data.get("file_name"),
        "file_size": data.get("file_size"),
        "file_type": data.get("file_type"),
        "chunk_size": data.get("chunk_size"),
        "total_chunks": data.get("total_chunks"),
    }, room=target_sid)

@socketio.on("relay_file_chunk")
def handle_relay_file_chunk(data):
    sid = request.sid
    if sid not in peers:
        return
    target_sid = resolve_target_sid(data.get("to"))
    if not target_sid or target_sid not in peers:
        return
    if peers[sid]["room"] != peers[target_sid]["room"]:
        return
    if not check_rate(sid, FILECHUNK_RATE_LIMIT):
        return
    emit("relay_file_chunk", {
        "from": sid,
        "transfer_id": data.get("transfer_id"),
        "chunk_index": data.get("chunk_index"),
        "data": data.get("data"),
    }, room=target_sid)

@socketio.on("relay_file_done")
def handle_relay_file_done(data):
    sid = request.sid
    if sid not in peers:
        return
    target_sid = resolve_target_sid(data.get("to"))
    if not target_sid or target_sid not in peers:
        return
    if peers[sid]["room"] != peers[target_sid]["room"]:
        return
    emit("relay_file_done", {
        "from": sid,
        "transfer_id": data.get("transfer_id"),
        "batch_id": data.get("batch_id"),
    }, room=target_sid)

# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return HTML_TEMPLATE

@app.route("/sw.js")
def service_worker():
    return SW_JS, 200, {"Content-Type": "application/javascript", "Service-Worker-Allowed": "/"}

# ─── Service Worker ───────────────────────────────────────────────────────────
SW_JS = """
const CACHE_NAME = "pairme-v1";
const urlsToCache = ["/"];

self.addEventListener("install", (event) => {
    event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(urlsToCache)));
    self.skipWaiting();
});

self.addEventListener("activate", (event) => {
    event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (event) => {
    event.respondWith(
        caches.match(event.request).then((response) => {
            return response || fetch(event.request);
        })
    );
});

self.addEventListener("sync", (event) => {
    if (event.tag === "pairme-keepalive") {
        event.waitUntil(Promise.resolve());
    }
});
"""

# ─── HTML Template ────────────────────────────────────────────────────────────
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<meta name="theme-color" content="#0f0f0f">
<link rel="manifest" href="data:application/json;base64,eyJuYW1lIjoiUGFpck1lIiwic2hvcnRfbmFtZSI6IlBhaXJNZSIsInN0YXJ0X3VybCI6Ii4iLCJkaXNwbGF5Ijoic3RhbmRhbG9uZSIsImJhY2tncm91bmRfY29sb3IiOiIjMGYwZjBmIn0=">
<title>PairMe — Fast P2P Transfer</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html,body{height:100%;background:#0f0f0f;color:#e8e8e8;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;overflow:hidden}
#app{max-width:900px;margin:0 auto;height:100%;display:flex;flex-direction:column;padding:12px}
header{display:flex;align-items:center;justify-content:space-between;padding:12px 0;border-bottom:1px solid #222}
header h1{font-size:20px;font-weight:700;letter-spacing:-0.5px}
#room-bar{display:flex;gap:8px;align-items:center}
#room-bar input{background:#1a1a1a;border:1px solid #333;color:#e8e8e8;padding:8px 12px;border-radius:10px;font-size:14px;width:120px;text-transform:uppercase}
#room-bar button{background:#2a2a2a;border:1px solid #444;color:#e8e8e8;padding:8px 14px;border-radius:10px;font-size:13px;cursor:pointer}
#room-bar button:active{transform:scale(0.96)}
#peer-list{display:flex;gap:10px;overflow-x:auto;padding:12px 0;scrollbar-width:none}
#peer-list::-webkit-scrollbar{display:none}
.peer-card{flex:0 0 auto;background:#161616;border:1px solid #2a2a2a;border-radius:14px;padding:14px 16px;min-width:140px;text-align:center;cursor:pointer;transition:all .15s}
.peer-card:hover{border-color:#444}
.peer-card .pid{font-size:11px;color:#888;margin-bottom:4px}
.peer-card .pname{font-size:14px;font-weight:600}
.peer-card .status{font-size:11px;color:#4caf50;margin-top:4px}
#drop-zone{flex:1;border:2px dashed #333;border-radius:20px;margin:12px 0;display:flex;flex-direction:column;align-items:center;justify-content:center;transition:all .2s;position:relative;overflow:hidden}
#drop-zone.dragover{border-color:#4caf50;background:#0a1f0a}
#drop-zone input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer}
#drop-zone .hint{color:#666;font-size:15px;text-align:center;padding:0 20px}
#drop-zone .hint b{color:#e8e8e8}
#log{height:160px;background:#111;border:1px solid #222;border-radius:14px;padding:12px;overflow-y:auto;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;font-size:12px;line-height:1.5;color:#aaa}
#log .entry{margin-bottom:2px}
#log .info{color:#64b5f6}
#log .success{color:#81c784}
#log .error{color:#e57373}
#log .p2p{color:#ba68c8}
#log .warn{color:#ffb74d}
#request-modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:100;align-items:center;justify-content:center}
#request-modal .box{background:#1a1a1a;border:1px solid #333;border-radius:20px;padding:24px;max-width:340px;width:90%;text-align:center}
#request-modal .box h3{margin-bottom:8px;font-size:18px}
#request-modal .box p{color:#aaa;font-size:14px;margin-bottom:16px}
#request-modal .box .actions{display:flex;gap:10px;justify-content:center}
#request-modal .box button{padding:10px 20px;border-radius:12px;border:none;font-size:14px;cursor:pointer}
#request-modal .box .accept{background:#4caf50;color:#fff}
#request-modal .box .deny{background:#e57373;color:#fff}
#lightbox{display:none;position:fixed;inset:0;background:rgba(0,0,0,.95);z-index:200;align-items:center;justify-content:center;flex-direction:column}
#lightbox.open{display:flex}
#lightbox img{max-width:90%;max-height:80%;border-radius:12px;object-fit:contain}
#lightbox .close{position:absolute;top:16px;right:16px;color:#fff;font-size:28px;cursor:pointer}
#lightbox .nav{position:absolute;top:50%;transform:translateY(-50%);color:#fff;font-size:36px;cursor:pointer;padding:16px}
#lightbox .prev{left:8px}
#lightbox .next{right:8px}
#transfers{display:flex;flex-direction:column;gap:8px;margin:8px 0}
.transfer-item{background:#161616;border:1px solid #222;border-radius:12px;padding:10px 14px;display:flex;align-items:center;justify-content:space-between}
.transfer-item .info{flex:1}
.transfer-item .name{font-size:13px;font-weight:600;margin-bottom:2px}
.transfer-item .meta{font-size:11px;color:#888}
.transfer-item .progress{width:120px;height:4px;background:#222;border-radius:2px;overflow:hidden;margin-left:12px}
.transfer-item .progress .bar{height:100%;background:#4caf50;width:0%;transition:width .2s}
#name-input{background:#1a1a1a;border:1px solid #333;color:#e8e8e8;padding:8px 12px;border-radius:10px;font-size:14px;width:140px}
#top-controls{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
button.primary{background:#4caf50 !important;border-color:#4caf50 !important;color:#fff !important}
</style>
</head>
<body>
<div id="app">
<header>
<h1>PairMe</h1>
<div id="top-controls">
<input id="name-input" placeholder="Your name" maxlength="20">
<button onclick="saveName()">Save</button>
<div id="room-bar">
<input id="room-code" placeholder="ROOM" maxlength="6">
<button onclick="joinRoom()">Join</button>
<button onclick="createRoom()">Create</button>
<button onclick="leaveRoom()">Leave</button>
</div>
</div>
</header>
<div id="peer-list"></div>
<div id="transfers"></div>
<div id="drop-zone">
<input type="file" id="file-input" multiple onchange="handleFileSelect(event)">
<div class="hint"><b>Tap or drop files here</b><br><span style="font-size:12px;color:#555">Up to 19 files. Photos send full-res first, preview async.</span></div>
</div>
<div id="log"></div>
</div>

<div id="request-modal">
<div class="box">
<h3 id="req-title">Incoming</h3>
<p id="req-body"></p>
<div class="actions">
<button class="deny" onclick="respondRequest(false)">Deny</button>
<button class="accept" onclick="respondRequest(true)">Accept</button>
</div>
</div>
</div>

<div id="lightbox">
<div class="close" onclick="closeLightbox()">&times;</div>
<div class="nav prev" onclick="lightboxNav(-1)">&#10094;</div>
<img id="lightbox-img" src="" alt="">
<div class="nav next" onclick="lightboxNav(1)">&#10095;</div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js"></script>
<script>
// ─── Config ─────────────────────────────────────────────────────────────────
var CHUNK_SIZE = 65536;
var MAX_IN_FLIGHT = 10;
var RELAY_BATCH_CONCURRENCY = 2;
var WEBRTC_SUPPORTED = !!(window.RTCPeerConnection);
var socket = null;
var myPeerId = null;
var mySid = null;
var peerList = [];
var connections = {};
var relayFileQueue = {};
var pendingRequest = null;
var receivedFiles = {};
var receivedBatches = {};
var lightboxImages = [];
var lightboxIndex = 0;
var wakeLock = null;
var visibilityKeepAlive = null;
var db = null;

// ─── IndexedDB for resumable / temp storage ─────────────────────────────────
function openDB() {
    return new Promise(function(resolve) {
        var req = indexedDB.open("pairme", 1);
        req.onupgradeneeded = function(e) {
            var d = e.target.result;
            if (!d.objectStoreNames.contains("chunks")) d.createObjectStore("chunks", {keyPath: "id"});
        };
        req.onsuccess = function(e) { db = e.target.result; resolve(db); };
        req.onerror = function() { resolve(null); };
    });
}

// ─── Logging ────────────────────────────────────────────────────────────────
function log(msg, type) {
    var el = document.getElementById("log");
    var line = document.createElement("div");
    line.className = "entry " + (type || "info");
    var t = new Date().toLocaleTimeString("en-GB", {hour12:false});
    line.textContent = t + " " + msg;
    el.appendChild(line);
    el.scrollTop = el.scrollHeight;
}

// ─── Wake Lock ──────────────────────────────────────────────────────────────
async function requestWakeLock() {
    if ("wakeLock" in navigator && !wakeLock) {
        try {
            wakeLock = await navigator.wakeLock.request("screen");
            log("Wake lock active", "success");
            wakeLock.addEventListener("release", function() {
                wakeLock = null;
                log("Wake lock released", "warn");
            });
        } catch (e) {}
    }
}
function releaseWakeLock() {
    if (wakeLock) { wakeLock.release(); wakeLock = null; }
}

// ─── Visibility / Keep-alive ────────────────────────────────────────────────
document.addEventListener("visibilitychange", function() {
    if (document.hidden) {
        if (socket && socket.connected) {
            visibilityKeepAlive = setInterval(function() {
                if (socket && socket.connected) socket.emit("ping_keepalive", {});
            }, 15000);
        }
    } else {
        if (visibilityKeepAlive) { clearInterval(visibilityKeepAlive); visibilityKeepAlive = null; }
        requestWakeLock();
        if (socket && !socket.connected) socket.connect();
    }
});

// ─── Socket Init ────────────────────────────────────────────────────────────
function initSocket() {
    openDB();
    var fp = localStorage.getItem("pairme_fp");
    if (!fp) {
        fp = "";
        var chars = "abcdef0123456789";
        for (var i = 0; i < 32; i++) fp += chars.charAt(Math.floor(Math.random() * chars.length));
        localStorage.setItem("pairme_fp", fp);
    }
    socket = io({query:{fp:fp}, transports:["websocket","polling"], reconnection:true, reconnectionAttempts:20, reconnectionDelay:1000, reconnectionDelayMax:5000});

    socket.on("connect", function() {
        log("Connected to server", "success");
        requestWakeLock();
    });
    socket.on("disconnect", function(reason) {
        log("Disconnected: " + reason, "warn");
        releaseWakeLock();
    });
    socket.on("init", function(data) {
        myPeerId = data.peer_id;
        mySid = data.sid;
        if (data.name) document.getElementById("name-input").value = data.name;
        log("ID: " + myPeerId + " | Room: " + (data.room || "Lobby"), "info");
    });
    socket.on("peers", function(data) {
        peerList = data.peers || [];
        renderPeers();
    });
    socket.on("signal", function(data) {
        handleSignal(data.from, data.data);
    });
    socket.on("broadcast_request", function(data) {
        pendingRequest = data;
        document.getElementById("req-title").textContent = "From " + (data.from_name || data.from_id);
        document.getElementById("req-body").textContent = (data.file_name || "File") + " (" + formatBytes(data.file_size || 0) + ")";
        document.getElementById("request-modal").style.display = "flex";
    });
    socket.on("broadcast_response", function(data) {
        if (data.accepted) {
            log("Transfer accepted", "success");
            var queueKey = data.from_id || data.from;
            startRelayQueue(queueKey);
        } else {
            log("Transfer denied", "warn");
            var qk = data.from_id || data.from;
            if (relayFileQueue[qk]) delete relayFileQueue[qk];
        }
    });
    socket.on("relay_text", function(data) {
        log("Text from " + (data.from_name || data.from_id) + ": " + data.text, "p2p");
    });
    socket.on("relay_file_start", function(data) {
        receiveFileStart(data);
    });
    socket.on("relay_file_chunk", function(data) {
        receiveFileChunk(data);
    });
    socket.on("relay_file_done", function(data) {
        receiveFileDone(data);
    });

    if ("serviceWorker" in navigator) {
        navigator.serviceWorker.register("/sw.js").catch(function(e){});
    }
}

// ─── Peer Rendering ─────────────────────────────────────────────────────────
function renderPeers() {
    var el = document.getElementById("peer-list");
    el.innerHTML = "";
    peerList.forEach(function(p) {
        if (p.id === myPeerId) return;
        var card = document.createElement("div");
        card.className = "peer-card";
        card.innerHTML = '<div class="pid">' + p.id + '</div><div class="pname">' + escapeHtml(p.name || p.id) + '</div><div class="status">Tap to send</div>';
        card.onclick = function() { promptSendTo(p.sid); };
        el.appendChild(card);
    });
}

function escapeHtml(t) {
    var d = document.createElement("div");
    d.textContent = t;
    return d.innerHTML;
}

// ─── Room Controls ──────────────────────────────────────────────────────────
function saveName() {
    var v = document.getElementById("name-input").value.trim();
    if (v) socket.emit("set_name", {name:v});
}
function createRoom() {
    socket.emit("create_room_code");
}
function joinRoom() {
    var c = document.getElementById("room-code").value.trim().toUpperCase();
    if (c) socket.emit("join_room_code", {code:c});
}
function leaveRoom() {
    socket.emit("leave_room_code");
}

// ─── WebRTC (P2P) ───────────────────────────────────────────────────────────
function createPeerConnection(targetSid) {
    var pc = new RTCPeerConnection({iceServers:[{urls:"stun:stun.l.google.com:19302"}]});
    pc.onicecandidate = function(e) {
        if (e.candidate) socket.emit("signal", {to:targetSid, data:{type:"ice", candidate:e.candidate}});
    };
    var dc = pc.createDataChannel("file", {ordered:true});
    dc.onopen = function() { log("P2P channel open to " + targetSid, "p2p"); };
    dc.onmessage = function(e) {
        var msg = JSON.parse(e.data);
        if (msg.t === "txt") log("P2P text: " + msg.c, "p2p");
    };
    pc.dataChannel = dc;
    connections[targetSid] = pc;
    return pc;
}
function handleSignal(from, data) {
    var pc = connections[from];
    if (!pc) {
        pc = new RTCPeerConnection({iceServers:[{urls:"stun:stun.l.google.com:19302"}]});
        pc.ondatachannel = function(e) {
            var dc = e.channel;
            dc.onmessage = function(ev) {
                var msg = JSON.parse(ev.data);
                if (msg.t === "txt") log("P2P text: " + msg.c, "p2p");
            };
            pc.dataChannel = dc;
        };
        connections[from] = pc;
    }
    if (data.type === "offer") {
        pc.setRemoteDescription(new RTCSessionDescription(data));
        pc.createAnswer().then(function(ans) {
            pc.setLocalDescription(ans);
            socket.emit("signal", {to:from, data:{type:"answer", sdp:ans.sdp}});
        });
    } else if (data.type === "answer") {
        pc.setRemoteDescription(new RTCSessionDescription(data));
    } else if (data.type === "ice" && data.candidate) {
        pc.addIceCandidate(new RTCIceCandidate(data.candidate));
    }
}

// ─── Transfer Initiation ────────────────────────────────────────────────────
function promptSendTo(targetSid) {
    var files = document.getElementById("file-input").files;
    if (!files || !files.length) { log("Select files first", "warn"); return; }
    sendFilesTo(targetSid, Array.from(files));
}

function handleFileSelect(e) {
    var files = e.target.files;
    if (!files || !files.length) return;
    if (peerList.length === 1) {
        var target = peerList.find(function(x){return x.id !== myPeerId;});
        if (target) sendFilesTo(target.sid, Array.from(files));
    } else {
        log("Select a peer to send to", "warn");
    }
}

function sendFilesTo(targetSid, files) {
    if (!files.length) return;
    var batchId = "b_" + generateTransferId();
    var pInfo = peerList.find(function(x){return x.sid === targetSid;});
    var targetPeerId = pInfo ? pInfo.id : targetSid;
    if (!relayFileQueue[targetPeerId]) relayFileQueue[targetPeerId] = [];
    files.forEach(function(file, idx) {
        relayFileQueue[targetPeerId].push({
            target_peer_id: targetPeerId,
            target_sid: targetSid,
            file: file,
            transfer_id: generateTransferId(),
            batch_id: batchId,
            batch_total: files.length,
            batch_index: idx
        });
    });
    socket.emit("broadcast_request", {
        to: targetPeerId,
        transfer_id: batchId,
        batch_id: batchId,
        batch_total: files.length,
        batch_index: 0,
        file_name: files[0].name,
        file_size: files.reduce(function(a,b){return a+b.size;},0),
        file_type: files[0].type || "application/octet-stream"
    });
    log("Batch request sent (" + files.length + " files)", "info");
}

function startRelayQueue(targetPeerId) {
    processRelayQueue(targetPeerId);
}

// ─── Relay Queue (Parallel Batch) ───────────────────────────────────────────
function processRelayQueue(targetPeerId) {
    var queue = relayFileQueue[targetPeerId];
    if (!queue) return;
    if (queue._active === undefined) queue._active = 0;
    while (queue._active < RELAY_BATCH_CONCURRENCY && queue.length) {
        var item = queue.shift();
        queue._active++;
        relaySendFile(item.target_sid, item.target_peer_id, item.file, item.transfer_id, item.batch_id, item.batch_total, item.batch_index, function() {
            queue._active--;
            processRelayQueue(targetPeerId);
        });
    }
}

// ─── File Sending ─────────────────────────────────────────────────────────────
function generateTransferId() {
    return Math.random().toString(36).slice(2,10) + Date.now().toString(36).slice(-4);
}

function formatBytes(b) {
    if (b < 1024) return b + " B";
    if (b < 1048576) return (b/1024).toFixed(1) + " KB";
    if (b < 1073741824) return (b/1048576).toFixed(1) + " MB";
    return (b/1073741824).toFixed(2) + " GB";
}

function relaySendFile(targetSid, targetPeerId, file, transferId, batchId, batchTotal, batchIndex, onComplete) {
    var totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    var inFlight = 0;
    var nextChunk = 0;
    var acked = 0;
    var done = false;
    var startTime = Date.now();
    var retries = {};

    socket.emit("relay_file_start", {
        to: targetPeerId,
        transfer_id: transferId,
        batch_id: batchId,
        batch_total: batchTotal,
        batch_index: batchIndex,
        file_name: file.name,
        file_size: file.size,
        file_type: file.type || "application/octet-stream",
        chunk_size: CHUNK_SIZE,
        total_chunks: totalChunks
    });

    updateTransferUI(transferId, file.name, 0, file.size);

    function emitChunk(idx, buf) {
        var b64 = arrayBufferToBase64(buf);
        socket.emit("relay_file_chunk", {
            to: targetPeerId,
            transfer_id: transferId,
            chunk_index: idx,
            data: b64
        });
    }

    function trySendChunk(idx) {
        if (done) return;
        inFlight++;
        var start = idx * CHUNK_SIZE;
        var end = Math.min(start + CHUNK_SIZE, file.size);
        var slice = file.slice(start, end);
        var reader = new FileReader();
        reader.onload = function(e) {
            emitChunk(idx, e.target.result);
            inFlight--;
            acked++;
            updateTransferUI(transferId, file.name, Math.min(acked * CHUNK_SIZE, file.size), file.size);
            if (acked >= totalChunks && !done) {
                done = true;
                socket.emit("relay_file_done", {to: targetPeerId, transfer_id: transferId, batch_id: batchId});
                var elapsed = (Date.now() - startTime) / 1000;
                var speed = formatBytes(file.size / elapsed) + "/s";
                log("Sent " + file.name + " in " + elapsed.toFixed(1) + "s (" + speed + ")", "success");
                if (onComplete) onComplete();
            } else {
                sendNext();
            }
        };
        reader.onerror = function() {
            inFlight--;
            retries[idx] = (retries[idx] || 0) + 1;
            if (retries[idx] <= 3) {
                setTimeout(function(){ trySendChunk(idx); }, 500 * retries[idx]);
            } else {
                log("Chunk " + idx + " failed after 3 retries", "error");
            }
            sendNext();
        };
        reader.readAsArrayBuffer(slice);
    }

    function sendNext() {
        if (done) return;
        while (inFlight < MAX_IN_FLIGHT && nextChunk < totalChunks) {
            trySendChunk(nextChunk);
            nextChunk++;
        }
    }
    sendNext();
}

function arrayBufferToBase64(buffer) {
    var bytes = new Uint8Array(buffer);
    var binary = "";
    var len = bytes.byteLength;
    for (var i = 0; i < len; i++) binary += String.fromCharCode(bytes[i]);
    return btoa(binary);
}

function base64ToArrayBuffer(b64) {
    var binary = atob(b64);
    var len = binary.length;
    var bytes = new Uint8Array(len);
    for (var i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i);
    return bytes.buffer;
}

// ─── Transfer UI ────────────────────────────────────────────────────────────
function updateTransferUI(id, name, loaded, total) {
    var el = document.getElementById("t_" + id);
    if (!el) {
        el = document.createElement("div");
        el.id = "t_" + id;
        el.className = "transfer-item";
        el.innerHTML = '<div class="info"><div class="name"></div><div class="meta"></div></div><div class="progress"><div class="bar"></div></div>';
        document.getElementById("transfers").appendChild(el);
    }
    el.querySelector(".name").textContent = name;
    el.querySelector(".meta").textContent = formatBytes(loaded) + " / " + formatBytes(total);
    var pct = total ? (loaded / total * 100) : 0;
    el.querySelector(".bar").style.width = pct + "%";
    if (pct >= 100) setTimeout(function(){el.remove();}, 3000);
}

// ─── File Receiving ─────────────────────────────────────────────────────────
function receiveFileStart(data) {
    receivedFiles[data.transfer_id] = {
        chunks: [],
        received: 0,
        total: data.total_chunks || 0,
        meta: data,
        startTime: Date.now()
    };
    log("Receiving " + data.file_name + " (" + formatBytes(data.file_size || 0) + ")", "info");
    updateTransferUI(data.transfer_id, data.file_name, 0, data.file_size || 0);
}

function receiveFileChunk(data) {
    var f = receivedFiles[data.transfer_id];
    if (!f) return;
    var buf = base64ToArrayBuffer(data.data);
    f.chunks[data.chunk_index] = buf;
    f.received++;
    var loaded = f.received * CHUNK_SIZE;
    if (loaded > f.meta.file_size) loaded = f.meta.file_size;
    updateTransferUI(data.transfer_id, f.meta.file_name, loaded, f.meta.file_size);
}

function receiveFileDone(data) {
    var f = receivedFiles[data.transfer_id];
    if (!f) return;
    var blob = new Blob(f.chunks, {type: f.meta.file_type || "application/octet-stream"});
    var url = URL.createObjectURL(blob);

    var batchId = f.meta.batch_id;
    if (!receivedBatches[batchId]) receivedBatches[batchId] = {items:[], total:f.meta.batch_total};
    receivedBatches[batchId].items.push({
        name: f.meta.file_name,
        url: url,
        blob: blob,
        type: f.meta.file_type,
        transfer_id: data.transfer_id
    });

    var elapsed = (Date.now() - f.startTime) / 1000;
    log("Received " + f.meta.file_name + " in " + elapsed.toFixed(1) + "s", "success");

    if (receivedBatches[batchId].items.length >= receivedBatches[batchId].total) {
        finalizeBatch(batchId);
    }

    delete receivedFiles[data.transfer_id];
}

// ─── Batch Finalization ─────────────────────────────────────────────────────
function finalizeBatch(batchId) {
    var batch = receivedBatches[batchId];
    if (!batch) return;
    log("Batch complete (" + batch.items.length + " files)", "success");

    var hasImages = batch.items.some(function(i){return i.type && i.type.startsWith("image/");});
    if (hasImages) {
        batch.items.forEach(function(item) {
            if (item.type && item.type.startsWith("image/")) {
                lightboxImages.push({url:item.url, name:item.name});
            }
        });
    }

    if (batch.items.length === 1) {
        triggerDownload(batch.items[0].url, batch.items[0].name);
    } else {
        buildZip(batchId);
    }
}

function triggerDownload(url, name) {
    var a = document.createElement("a");
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    setTimeout(function(){document.body.removeChild(a);}, 100);
}

function buildZip(batchId) {
    var batch = receivedBatches[batchId];
    if (!batch || !window.JSZip) {
        downloadBatchIndividual(batchId);
        return;
    }
    var zip = new JSZip();
    var folder = zip.folder("pairme_" + batchId.slice(-6));
    var promises = batch.items.map(function(item) {
        return Promise.resolve(item.blob).then(function(b){ folder.file(item.name || "file", b); });
    });
    Promise.all(promises).then(function() {
        return zip.generateAsync({type:"blob", compression:"DEFLATE", compressionOptions:{level:6}});
    }).then(function(content) {
        var url = URL.createObjectURL(content);
        triggerDownload(url, "pairme_" + batchId.slice(-6) + ".zip");
        setTimeout(function(){URL.revokeObjectURL(url);}, 5000);
        log("ZIP ready (" + batch.items.length + " files)", "success");
    }).catch(function(err) {
        log("ZIP failed: " + (err.message || err), "error");
        downloadBatchIndividual(batchId);
    });
}

function downloadBatchIndividual(batchId) {
    var batch = receivedBatches[batchId];
    if (!batch) return;
    batch.items.forEach(function(item){ triggerDownload(item.url, item.name); });
}

// ─── Text Sending ───────────────────────────────────────────────────────────
function sendTextTo(targetSid, text) {
    var pc = WEBRTC_SUPPORTED ? connections[targetSid] : null;
    if (pc && pc.dataChannel && pc.dataChannel.readyState === "open") {
        pc.dataChannel.send(JSON.stringify({t:"txt", c:text}));
        log("Sent text (P2P)", "p2p");
    } else {
        var pInfo = peerList.find(function(x){return x.sid === targetSid;});
        socket.emit("relay_text", {to: (pInfo ? pInfo.id : targetSid), text: text});
        log("Sent text (Relay)", "info");
    }
}

// ─── Request Modal ──────────────────────────────────────────────────────────
function respondRequest(accepted) {
    document.getElementById("request-modal").style.display = "none";
    if (pendingRequest) {
        socket.emit("broadcast_response", {to: pendingRequest.from_id, accepted: accepted, transfer_id: pendingRequest.transfer_id});
        pendingRequest = null;
    }
}

// ─── Lightbox ───────────────────────────────────────────────────────────────
function closeLightbox() {
    document.getElementById("lightbox").classList.remove("open");
}
function lightboxNav(dir) {
    if (!lightboxImages.length) return;
    lightboxIndex = (lightboxIndex + dir + lightboxImages.length) % lightboxImages.length;
    document.getElementById("lightbox-img").src = lightboxImages[lightboxIndex].url;
}
document.addEventListener("keydown", function(e) {
    var lb = document.getElementById("lightbox");
    if (!lb || !lb.classList.contains("open")) return;
    if (e.key === "Escape") closeLightbox();
    if (e.key === "ArrowLeft") lightboxNav(-1);
    if (e.key === "ArrowRight") lightboxNav(1);
});

// ─── Drop Zone ──────────────────────────────────────────────────────────────
var dropZone = document.getElementById("drop-zone");
dropZone.addEventListener("dragover", function(e){e.preventDefault();dropZone.classList.add("dragover");});
dropZone.addEventListener("dragleave", function(e){e.preventDefault();dropZone.classList.remove("dragover");});
dropZone.addEventListener("drop", function(e){
    e.preventDefault();
    dropZone.classList.remove("dragover");
    handleFileSelect({target:{files:e.dataTransfer.files}});
});

window.onload = initSocket;
</script>
</body>
</html>
"""

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
