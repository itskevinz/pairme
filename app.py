from gevent import monkey
monkey.patch_all()

from flask import Flask, render_template_string, request
from flask_socketio import SocketIO, emit, join_room, leave_room
import uuid, time, random, logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pairme")

app = Flask(__name__)
app.config["SECRET_KEY"] = "pairme-secret-key"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent", ping_timeout=60, ping_interval=25)

peers = {}
rooms = {}

def generate_code():
    return str(random.randint(100000, 999999))

def broadcast_peers():
    for sid, info in list(peers.items()):
        room = info.get("room")
        peer_list = []
        for other_sid, other_info in peers.items():
            if other_sid == sid:
                continue
            if other_info.get("room") == room:
                peer_list.append({
                    "sid": other_sid,
                    "id": other_info["id"],
                    "name": other_info["name"]
                })
        socketio.emit("peers", peer_list, room=sid)

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)

@socketio.on("connect")
def handle_connect():
    peer_id = str(uuid.uuid4())[:8]
    peers[request.sid] = {
        "id": peer_id,
        "name": "Device " + peer_id[-4:].upper(),
        "joined": time.time(),
        "room": "Lobby"
    }
    join_room("Lobby")
    emit("init", {"peer_id": peer_id, "sid": request.sid})
    broadcast_peers()

@socketio.on("disconnect")
def handle_disconnect():
    sid = request.sid
    if sid in peers:
        room = peers[sid].get("room")
        if room and room in rooms and sid in rooms[room]:
            rooms[room].remove(sid)
            if not rooms[room]:
                del rooms[room]
        del peers[sid]
    broadcast_peers()

@socketio.on("set_name")
def handle_set_name(data):
    sid = request.sid
    if sid in peers:
        peers[sid]["name"] = data.get("name", peers[sid]["name"])[:20]
        broadcast_peers()

@socketio.on("join_room_code")
def handle_join_room_code(data):
    sid = request.sid
    code = str(data.get("code", "")).strip()
    if not code or len(code) != 6 or not code.isdigit():
        emit("room_error", {"msg": "Invalid code"})
        return
    old_room = peers[sid].get("room")
    if old_room:
        leave_room(old_room)
        if old_room in rooms and sid in rooms[old_room]:
            rooms[old_room].remove(sid)
            if not rooms[old_room]:
                del rooms[old_room]
    join_room(code)
    peers[sid]["room"] = code
    if code not in rooms:
        rooms[code] = []
    rooms[code].append(sid)
    emit("room_joined", {"code": code})
    broadcast_peers()

@socketio.on("create_room_code")
def handle_create_room_code():
    sid = request.sid
    code = generate_code()
    while code in rooms:
        code = generate_code()
    old_room = peers[sid].get("room")
    if old_room:
        leave_room(old_room)
        if old_room in rooms and sid in rooms[old_room]:
            rooms[old_room].remove(sid)
            if not rooms[old_room]:
                del rooms[old_room]
    join_room(code)
    peers[sid]["room"] = code
    rooms[code] = [sid]
    emit("room_joined", {"code": code})
    broadcast_peers()

@socketio.on("leave_room_code")
def handle_leave_room_code():
    sid = request.sid
    old_room = peers[sid].get("room")
    if old_room:
        leave_room(old_room)
        if old_room in rooms and sid in rooms[old_room]:
            rooms[old_room].remove(sid)
            if not rooms[old_room]:
                del rooms[old_room]
    join_room("Lobby")
    peers[sid]["room"] = "Lobby"
    emit("room_left", {})
    broadcast_peers()

@socketio.on("signal")
def handle_signal(data):
    target_sid = data.get("to")
    if target_sid in peers:
        emit("signal", {
            "from": request.sid,
            "from_peer": peers[request.sid]["id"],
            "from_name": peers[request.sid]["name"],
            "signal": data.get("signal")
        }, room=target_sid)

@socketio.on("broadcast_request")
def handle_broadcast_request(data):
    target_sid = data.get("to")
    if target_sid in peers:
        emit("transfer_request", {
            "from": request.sid,
            "from_peer": peers[request.sid]["id"],
            "from_name": peers[request.sid]["name"],
            "file_name": data.get("file_name", "file"),
            "file_size": data.get("file_size", 0),
            "file_type": data.get("file_type", ""),
            "transfer_id": data.get("transfer_id")
        }, room=target_sid)

@socketio.on("broadcast_response")
def handle_broadcast_response(data):
    target_sid = data.get("to")
    if target_sid in peers:
        emit("transfer_response", {
            "from": request.sid,
            "accepted": data.get("accepted", False),
            "transfer_id": data.get("transfer_id")
        }, room=target_sid)

@socketio.on("relay_text")
def handle_relay_text(data):
    target_sid = data.get("to")
    if target_sid in peers:
        emit("relay_text", {
            "from": request.sid,
            "from_name": peers[request.sid]["name"],
            "text": data.get("text", "")
        }, room=target_sid)

@socketio.on("relay_file_start")
def handle_relay_file_start(data):
    target_sid = data.get("to")
    if target_sid in peers:
        emit("relay_file_start", {
            "from": request.sid,
            "from_name": peers[request.sid]["name"],
            "file_name": data.get("file_name", "file"),
            "file_size": data.get("file_size", 0),
            "file_type": data.get("file_type", ""),
            "transfer_id": data.get("transfer_id")
        }, room=target_sid)

@socketio.on("relay_file_chunk")
def handle_relay_file_chunk(data):
    target_sid = data.get("to")
    if target_sid in peers:
        emit("relay_file_chunk", {
            "from": request.sid,
            "transfer_id": data.get("transfer_id"),
            "chunk": data.get("chunk", "")
        }, room=target_sid)

@socketio.on("relay_file_done")
def handle_relay_file_done(data):
    target_sid = data.get("to")
    if target_sid in peers:
        emit("relay_file_done", {
            "from": request.sid,
            "transfer_id": data.get("transfer_id")
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
    <meta property="og:image" content="https://imgg.fr/r/qVsZ8JGy.png">
    
    <link rel="icon" type="image/png" href="https://imgg.fr/r/qVsZ8JGy.png">
    <link rel="apple-touch-icon" href="https://imgg.fr/r/qVsZ8JGy.png">

    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
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

        .feed-list { list-style: none; display: flex; flex-direction: column; gap: 8px; }
        .feed-item { background: #f8fafc; border: 1px solid #e2e8f0; padding: 10px; border-radius: 6px; font-size: 13px; }
        .feed-meta { font-size: 11px; color: #64748b; margin-bottom: 6px; display: flex; justify-content: space-between; }
        .feed-body { word-break: break-word; white-space: pre-wrap; margin-bottom: 6px; color: #1e293b; font-size: 13px; line-height: 1.4; }

        .text-link { color: #2563eb; text-decoration: underline; word-break: break-all; }
        
        .code-wrapper { position: relative; margin: 8px 0; border-radius: 6px; overflow: hidden; background: #0f172a; border: 1px solid #1e293b; }
        .code-header { display: flex; justify-content: space-between; align-items: center; background: #1e293b; padding: 4px 10px; font-size: 11px; color: #94a3b8; font-family: monospace; }
        .copy-btn { background: transparent; border: 1px solid #475569; color: #cbd5e1; border-radius: 4px; padding: 2px 8px; font-size: 10px; cursor: pointer; }
        .copy-btn:active { background: #334155; }
        .code-block { color: #f8fafc; padding: 10px; font-family: Consolas, Monaco, "Andale Mono", "Ubuntu Mono", monospace; font-size: 12px; line-height: 1.4; overflow-x: auto; max-height: 300px; white-space: pre; word-break: normal; word-wrap: normal; -webkit-overflow-scrolling: touch; }
        .inline-code { background: #e2e8f0; color: #0f172a; padding: 2px 5px; border-radius: 4px; font-family: monospace; font-size: 12px; word-break: break-all; }

        .file-preview { margin: 6px 0; max-width: 100%; text-align: center; background: #edf2f7; border-radius: 6px; overflow: hidden; }
        .preview-img { max-width: 100%; max-height: 200px; display: block; margin: 0 auto; object-fit: contain; }

        .dl-btn { display: inline-flex; align-items: center; gap: 4px; padding: 6px 10px; background: #0f172a; color: #ffffff; text-decoration: none; border-radius: 4px; font-size: 11px; font-weight: 500; margin-top: 4px; }

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
            <img src="https://imgg.fr/r/qVsZ8JGy.png" alt="Logo">
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
                    <span>Tap or drag files here</span>
                    <input type="file" id="file-input" multiple style="display:none;" onchange="handleFileSelect(event)">
                </div>

                <div id="progress-wrap" style="display:none;">
                    <div class="row" style="justify-space-between;font-size:11px;color:#64748b;">
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

<script>
var socket = null;
var mySid = "";
var myPeerId = "";
var peerList = [];
var connections = {};
var pendingRequest = null;
var pendingFileQueue = {};
var relayBuffer = {};
var relayMeta = {};
var textStore = {};
var CHUNK_SIZE = 16384;
var STUN_SERVERS = {
    iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" }
    ]
};

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
    copyToClipboard(rawText, btnElement, "Copied All!");
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

function generateTransferId() {
    return Date.now() + "_" + Math.floor(Math.random() * 100000);
}

function initSocket() {
    socket = io({ transports: ["websocket", "polling"] });

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
        relayBuffer[data.transfer_id] = [];
        relayMeta[data.transfer_id] = data;
        log("Receiving " + data.file_name, "info");
    });

    socket.on("relay_file_chunk", function(data) {
        if (relayBuffer[data.transfer_id]) {
            var binary = atob(data.chunk);
            var bytes = new Uint8Array(binary.length);
            for (var i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
            relayBuffer[data.transfer_id].push(bytes.buffer);
        }
    });

    socket.on("relay_file_done", function(data) {
        var meta = relayMeta[data.transfer_id];
        var buffers = relayBuffer[data.transfer_id];
        if (meta && buffers) {
            var blob = new Blob(buffers, { type: meta.file_type });
            var url = URL.createObjectURL(blob);
            addReceived("file", { name: meta.file_name, size: meta.file_size, url: url, type: meta.file_type }, meta.from_name);
            log("Received " + meta.file_name, "success");
            delete relayBuffer[data.transfer_id];
            delete relayMeta[data.transfer_id];
        }
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
    var pc = connections[targetSid];
    if (pc && pc.dataChannel && pc.dataChannel.readyState === "open") {
        pc.dataChannel.send(JSON.stringify({ t: "txt", c: text }));
        log("Sent text (P2P)", "p2p");
    } else {
        socket.emit("relay_text", { to: targetSid, text: text });
        log("Sent text (Relay)", "info");
    }
}

function handleFileSelect(e) {
    var files = e.target.files;
    if (!files.length) return;
    var targetSid = document.getElementById("peer-select").value;

    for (var i = 0; i < files.length; i++) {
        var file = files[i];
        if (targetSid) {
            sendFileTo(targetSid, file);
        } else {
            peerList.forEach(function(p) { sendFileTo(p.sid, file); });
        }
    }
    document.getElementById("file-input").value = "";
}

function sendFileTo(targetSid, file) {
    var transferId = generateTransferId();
    var pc = connections[targetSid];
    if (pc && pc.dataChannel && pc.dataChannel.readyState === "open") {
        if (!pendingFileQueue[targetSid]) pendingFileQueue[targetSid] = [];
        pendingFileQueue[targetSid].push({ file: file, transfer_id: transferId });
        if (pendingFileQueue[targetSid].length === 1) {
            socket.emit("broadcast_request", { to: targetSid, file_name: file.name, file_size: file.size, file_type: file.type, transfer_id: transferId });
        }
    } else {
        relaySendFile(targetSid, file, transferId);
    }
}

function startDataTransfer(targetSid, transferId) {
    var queue = pendingFileQueue[targetSid];
    if (!queue || !queue.length) return;
    var item = queue[0];
    var file = item.file;
    var pc = connections[targetSid];
    var channel = pc.dataChannel;

    channel.send(JSON.stringify({ t: "fs", n: file.name, s: file.size, m: file.type, id: item.transfer_id }));

    var reader = new FileReader();
    reader.onload = function(e) {
        var buffer = e.target.result;
        var offset = 0;
        document.getElementById("progress-wrap").style.display = "block";

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
            if (queue.length) socket.emit("broadcast_request", { to: targetSid, file_name: queue[0].file.name, file_size: queue[0].file.size, transfer_id: queue[0].transfer_id });
        };
        sendChunk();
    };
    reader.readAsArrayBuffer(file);
}

function relaySendFile(targetSid, file, transferId) {
    socket.emit("relay_file_start", { to: targetSid, file_name: file.name, file_size: file.size, file_type: file.type, transfer_id: transferId });
    var reader = new FileReader();
    reader.onload = function(e) {
        var bytes = new Uint8Array(e.target.result);
        var offset = 0;
        document.getElementById("progress-wrap").style.display = "block";

        function sendRelayChunk() {
            if (offset < bytes.length) {
                var chunk = bytes.subarray(offset, offset + CHUNK_SIZE);
                var binary = '';
                for (var i = 0; i < chunk.length; i++) binary += String.fromCharCode(chunk[i]);
                socket.emit("relay_file_chunk", { to: targetSid, transfer_id: transferId, chunk: btoa(binary) });
                offset += CHUNK_SIZE;
                var pct = Math.min(100, Math.round((offset / bytes.length) * 100));
                document.getElementById("progress-fill").style.width = pct + "%";
                document.getElementById("send-pct").textContent = pct + "%";
                setTimeout(sendRelayChunk, 5);
            } else {
                socket.emit("relay_file_done", { to: targetSid, transfer_id: transferId });
                log("Sent " + file.name + " (Relay)", "success");
                setTimeout(function() { document.getElementById("progress-wrap").style.display = "none"; }, 800);
            }
        }
        sendRelayChunk();
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
            if (buffers) {
                var blob = new Blob(buffers, { type: pc.activeMeta.m });
                var url = URL.createObjectURL(blob);
                addReceived("file", { name: pc.activeMeta.n, size: pc.activeMeta.s, url: url, type: pc.activeMeta.m }, fromSid);
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

function addReceived(type, data, sender) {
    var list = document.getElementById("received-list");
    var li = document.createElement("li");
    li.className = "feed-item";
    var now = new Date();
    var time = now.getHours() + ":" + ("0" + now.getMinutes()).slice(-2);

    var actionHtml = "";
    if (type === "text") {
        var textId = "txt_" + generateTransferId();
        var formatted = renderFormattedContent(data, textId);
        actionHtml = '<div class="feed-body">' + formatted + '</div>' +
        '<button class="flat icon-only" onclick="copyFullText(\'' + textId + '\', this)" title="Copy All Text" style="width:26px;height:26px;padding:4px;">' +
            '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>' +
        '</button>';
    } else {
        var isImage = data.type && data.type.indexOf("image/") === 0;
        var previewHtml = isImage ? '<div class="file-preview"><img src="' + data.url + '" class="preview-img" alt="preview" /></div>' : '';
        actionHtml = '<div class="feed-body"><b>' + escapeHtml(data.name) + '</b> <span style="font-size:11px;color:#64748b;">(' + formatBytes(data.size) + ')</span></div>' + previewHtml +
        '<a href="' + data.url + '" download="' + escapeHtml(data.name) + '" class="dl-btn">' +
            '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>' +
            'Download' +
        '</a>';
    }

    li.innerHTML = '<div class="feed-meta"><span>' + escapeHtml(sender) + '</span><span>' + time + '</span></div>' + actionHtml;
    list.insertBefore(li, list.firstChild);
}

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
    handleFileSelect({ target: { files: e.dataTransfer.files } });
});

window.onload = initSocket;
</script>
</body>
</html>
"""

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
