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
            "file_type": data.get("file_type", "")
        }, room=target_sid)

@socketio.on("broadcast_response")
def handle_broadcast_response(data):
    target_sid = data.get("to")
    if target_sid in peers:
        emit("transfer_response", {
            "from": request.sid,
            "accepted": data.get("accepted", False)
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
            "file_type": data.get("file_type", "")
        }, room=target_sid)

@socketio.on("relay_file_chunk")
def handle_relay_file_chunk(data):
    target_sid = data.get("to")
    if target_sid in peers:
        emit("relay_file_chunk", {
            "from": request.sid,
            "chunk": data.get("chunk", "")
        }, room=target_sid)

@socketio.on("relay_file_done")
def handle_relay_file_done(data):
    target_sid = data.get("to")
    if target_sid in peers:
        emit("relay_file_done", {"from": request.sid}, room=target_sid)

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PairMe</title>
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, sans-serif; }
        body { background: #f8fafc; color: #0f172a; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }
        
        header { background: #ffffff; padding: 12px 20px; border-bottom: 1px solid #e2e8f0; display: flex; justify-content: space-between; align-items: center; }
        .brand { font-size: 16px; font-weight: 700; color: #0f172a; letter-spacing: -0.3px; display: flex; align-items: center; gap: 8px; }
        .room-tag { background: #f1f5f9; color: #475569; padding: 4px 10px; border-radius: 6px; font-size: 12px; font-weight: 600; border: 1px solid #e2e8f0; display: flex; align-items: center; gap: 6px; }

        .app-grid { display: grid; grid-template-columns: 280px 1fr 320px; gap: 16px; padding: 16px; height: calc(100vh - 57px); }
        .card { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; display: flex; flex-direction: column; overflow: hidden; }
        .card-header { padding: 12px 14px; font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; color: #64748b; border-bottom: 1px solid #f1f5f9; display: flex; justify-content: space-between; align-items: center; background: #ffffff; }
        .card-body { padding: 14px; flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 12px; }

        input, select, textarea { font-size: 13px; border-radius: 6px; border: 1px solid #cbd5e1; background: #ffffff; color: #0f172a; padding: 8px 10px; outline: none; transition: border-color 0.15s; }
        input:focus, select:focus, textarea:focus { border-color: #0f172a; }
        
        button { background: #0f172a; color: #ffffff; border: 1px solid #0f172a; border-radius: 6px; font-size: 13px; font-weight: 500; padding: 8px 12px; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; gap: 6px; transition: background-color 0.15s, border-color 0.15s; }
        button:hover { background: #334155; border-color: #334155; }
        button.flat { background: #ffffff; color: #0f172a; border: 1px solid #cbd5e1; }
        button.flat:hover { background: #f8fafc; border-color: #94a3b8; }
        button.icon-only { padding: 8px; width: 34px; height: 34px; }

        .row { display: flex; gap: 8px; align-items: center; }
        .flex-1 { flex: 1; }

        .peer-item { background: #ffffff; border: 1px solid #e2e8f0; padding: 10px 12px; border-radius: 6px; display: flex; justify-content: space-between; align-items: center; cursor: pointer; transition: all 0.15s; }
        .peer-item:hover, .peer-item.active { border-color: #0f172a; background: #f8fafc; }
        .peer-info { display: flex; flex-direction: column; }
        .peer-name { font-weight: 600; font-size: 13px; color: #0f172a; }
        .peer-id { font-size: 11px; color: #94a3b8; font-family: ui-monospace, monospace; }

        .drop-zone { border: 2px dashed #cbd5e1; border-radius: 8px; padding: 24px; text-align: center; color: #64748b; cursor: pointer; transition: all 0.15s; background: #f8fafc; display: flex; flex-direction: column; align-items: center; gap: 8px; font-size: 13px; }
        .drop-zone:hover, .drop-zone.dragover { border-color: #0f172a; background: #f1f5f9; color: #0f172a; }

        .feed-list { list-style: none; display: flex; flex-direction: column; gap: 10px; }
        .feed-item { background: #f8fafc; border: 1px solid #e2e8f0; padding: 12px; border-radius: 6px; font-size: 13px; }
        .feed-meta { font-size: 11px; color: #64748b; margin-bottom: 6px; display: flex; justify-content: space-between; }
        .feed-body { word-break: break-all; white-space: pre-wrap; margin-bottom: 8px; color: #1e293b; }

        #log-container { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 11px; flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 6px; }
        .log-entry { padding: 6px 8px; border-radius: 4px; display: flex; gap: 8px; align-items: flex-start; line-height: 1.4; border: 1px solid transparent; }
        .log-time { color: #94a3b8; flex-shrink: 0; }
        .log-tag { padding: 1px 5px; border-radius: 3px; font-weight: 700; font-size: 9px; text-transform: uppercase; flex-shrink: 0; }
        
        .tag-info { background: #e0f2fe; color: #0369a1; }
        .tag-success { background: #dcfce7; color: #15803d; }
        .tag-warn { background: #fef3c7; color: #b45309; }
        .tag-error { background: #fee2e2; color: #b91c1c; }
        .tag-p2p { background: #f3e8ff; color: #6b21a8; }

        .progress-bar { height: 4px; background: #e2e8f0; border-radius: 2px; overflow: hidden; margin-top: 6px; }
        .progress-fill { height: 100%; background: #0f172a; width: 0%; transition: width 0.1s; }

        .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(15, 23, 42, 0.4); z-index: 100; justify-content: center; align-items: center; }
        .modal { background: #ffffff; border: 1px solid #e2e8f0; padding: 20px; border-radius: 8px; width: 300px; text-align: center; }

        @media (max-width: 900px) {
            body { height: auto; overflow: auto; }
            .app-grid { grid-template-columns: 1fr; height: auto; }
        }
    </style>
</head>
<body>

    <header>
        <div class="brand">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M16 16l4-4-4-4M8 8l-4 4 4 4"/></svg>
            PairMe
        </div>
        <div class="room-tag" id="room-badge">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 8v4l3 3"/></svg>
            <span id="room-name">Lobby</span>
        </div>
    </header>

    <div class="app-grid">
        <div class="card">
            <div class="card-header">
                <span>Devices</span>
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg>
            </div>
            <div class="card-body">
                <div>
                    <div style="font-size:11px;color:#94a3b8;margin-bottom:2px;">THIS DEVICE</div>
                    <div id="my-id" style="font-family:ui-monospace,monospace;font-size:13px;font-weight:700;color:#0f172a;">---</div>
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
                <div class="card-header" style="margin:8px -14px 0 -14px;border-top:1px solid #f1f5f9;">Nearby</div>
                <div id="peer-list" style="display:flex;flex-direction:column;gap:6px;">
                    <div style="color:#94a3b8;font-size:12px;">No devices detected</div>
                </div>
            </div>
        </div>

        <div class="card">
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
                    <textarea id="text-input" rows="2" placeholder="Message, link, or snippet..." class="flex-1"></textarea>
                    <button class="icon-only" onclick="sendText()" title="Send">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
                    </button>
                </div>

                <div class="drop-zone" id="drop-zone" onclick="document.getElementById('file-input').click()">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
                    <span>Drop files here or click to browse</span>
                    <input type="file" id="file-input" multiple style="display:none;" onchange="handleFileSelect(event)">
                </div>

                <div id="progress-wrap" style="display:none;">
                    <div class="row" style="justify-content:space-between;font-size:11px;color:#64748b;">
                        <span id="send-status">Sending</span>
                        <span id="send-pct">0%</span>
                    </div>
                    <div class="progress-bar"><div class="progress-fill" id="progress-fill"></div></div>
                </div>

                <div class="card-header" style="margin:0 -14px;">Received</div>
                <ul class="feed-list" id="received-list"></ul>
            </div>
        </div>

        <div class="card">
            <div class="card-header">
                <span>Logs</span>
                <button class="flat icon-only" onclick="clearLogs()" title="Clear Logs" style="width:24px;height:24px;padding:4px;">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                </button>
            </div>
            <div class="card-body" style="padding:10px;">
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
let socket = null;
let mySid = "";
let myPeerId = "";
let peerList = [];
let connections = {};
let pendingRequest = null;
let pendingFileQueue = {};
const CHUNK_SIZE = 16384;
const STUN_SERVERS = {
    iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" }
    ]
};

function log(msg, type = "info") {
    const el = document.getElementById("log-container");
    const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const entry = document.createElement("div");
    entry.className = "log-entry";
    entry.innerHTML = `<span class="log-time">${time}</span><span class="log-tag tag-${type}">${type}</span><span style="word-break:break-all;">${escapeHtml(msg)}</span>`;
    el.appendChild(entry);
    el.scrollTop = el.scrollHeight;
}

function clearLogs() {
    document.getElementById("log-container").innerHTML = "";
}

function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
}

function formatBytes(bytes) {
    if (bytes === 0) return "0 B";
    const k = 1024, sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

function initSocket() {
    socket = io({ transports: ["websocket", "polling"] });

    socket.on("connect", () => {
        log("Connected to server", "success");
        const saved = localStorage.getItem("pairme_name");
        if (saved) {
            document.getElementById("my-name").value = saved;
            socket.emit("set_name", { name: saved });
        }
    });

    socket.on("init", data => {
        mySid = data.sid;
        myPeerId = data.peer_id;
        document.getElementById("my-id").textContent = myPeerId;
        log(`ID: ${myPeerId}`, "info");
    });

    socket.on("peers", data => {
        peerList = data;
        renderPeers();
    });

    socket.on("signal", handleSignal);

    socket.on("transfer_request", data => {
        pendingRequest = data;
        document.getElementById("request-details").textContent = `${data.from_name} -> ${data.file_name} (${formatBytes(data.file_size)})`;
        document.getElementById("request-modal").style.display = "flex";
    });

    socket.on("transfer_response", data => {
        if (data.accepted) {
            log("Accepted by peer", "success");
            startDataTransfer(data.from);
        } else {
            log("Declined by peer", "warn");
        }
    });

    socket.on("room_joined", data => {
        document.getElementById("room-name").textContent = data.code;
        log(`Joined room ${data.code}`, "success");
    });

    socket.on("room_left", () => {
        document.getElementById("room-name").textContent = "Lobby";
        log("Switched to Lobby", "info");
    });

    socket.on("relay_text", data => {
        addReceived("text", data.text, data.from_name);
        log(`Text from ${data.from_name}`, "info");
    });

    let relayBuffer = {};
    let relayMeta = {};

    socket.on("relay_file_start", data => {
        relayBuffer[data.from] = [];
        relayMeta[data.from] = data;
        log(`Receiving ${data.file_name}`, "info");
    });

    socket.on("relay_file_chunk", data => {
        if (relayBuffer[data.from]) {
            const binary = atob(data.chunk);
            const bytes = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
            relayBuffer[data.from].push(bytes.buffer);
        }
    });

    socket.on("relay_file_done", data => {
        const meta = relayMeta[data.from];
        const buffers = relayBuffer[data.from];
        if (meta && buffers) {
            const blob = new Blob(buffers, { type: meta.file_type });
            const url = URL.createObjectURL(blob);
            addReceived("file", { name: meta.file_name, size: meta.file_size, url: url, type: meta.file_type }, meta.from_name);
            log(`Received ${meta.file_name}`, "success");
            delete relayBuffer[data.from];
            delete relayMeta[data.from];
        }
    });
}

function renderPeers() {
    const list = document.getElementById("peer-list");
    const select = document.getElementById("peer-select");
    list.innerHTML = "";
    select.innerHTML = '<option value="">-- All Devices --</option>';

    if (peerList.length === 0) {
        list.innerHTML = '<div style="color:#94a3b8;font-size:12px;">No devices detected</div>';
        return;
    }

    peerList.forEach(p => {
        const item = document.createElement("div");
        item.className = "peer-item";
        item.onclick = () => selectPeer(p.sid);
        item.innerHTML = `<div class="peer-info"><span class="peer-name">${escapeHtml(p.name)}</span><span class="peer-id">${p.id}</span></div>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg>`;
        list.appendChild(item);

        const opt = document.createElement("option");
        opt.value = p.sid;
        opt.textContent = `${p.name} (${p.id})`;
        select.appendChild(opt);
    });
}

function selectPeer(sid) {
    document.getElementById("peer-select").value = sid;
    onPeerSelectChange();
}

function onPeerSelectChange() {
    const sid = document.getElementById("peer-select").value;
    const label = document.getElementById("target-peer-label");
    if (sid) {
        const p = peerList.find(x => x.sid === sid);
        label.textContent = `To: ${p ? p.name : sid}`;
        connectPeer(sid);
    } else {
        label.textContent = "To: Everyone";
    }
}

function updateName() {
    const name = document.getElementById("my-name").value.trim();
    if (name) {
        socket.emit("set_name", { name: name });
        localStorage.setItem("pairme_name", name);
        log(`Updated name: ${name}`, "success");
    }
}

function joinRoom() {
    const code = document.getElementById("room-code-input").value.trim();
    if (code.length === 6) socket.emit("join_room_code", { code: code });
}

function createRoom() { socket.emit("create_room_code"); }
function leaveRoom() { socket.emit("leave_room_code"); }

function getOrCreateConnection(targetSid, isInitiator) {
    if (connections[targetSid]) return connections[targetSid];

    const pc = new RTCPeerConnection(STUN_SERVERS);
    pc.iceQueue = [];
    pc.targetSid = targetSid;
    pc.receiveBuffer = [];

    pc.onicecandidate = e => {
        if (e.candidate) {
            socket.emit("signal", { to: targetSid, signal: { type: "ice", candidate: e.candidate } });
        }
    };

    pc.ondatachannel = e => setupDataChannel(pc, e.channel, targetSid);

    if (isInitiator) {
        const channel = pc.createDataChannel("pairme", { ordered: true });
        setupDataChannel(pc, channel, targetSid);
    }

    connections[targetSid] = pc;
    return pc;
}

function setupDataChannel(pc, channel, targetSid) {
    pc.dataChannel = channel;
    channel.onopen = () => log(`P2P open with ${targetSid.slice(0, 4)}`, "p2p");
    channel.onmessage = e => handleDataMessage(e.data, targetSid);
    channel.onerror = err => log(`Channel error: ${err.message}`, "error");
}

function handleSignal(data) {
    const fromSid = data.from;
    const signal = data.signal;
    const pc = getOrCreateConnection(fromSid, false);

    if (signal.type === "offer") {
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .then(() => {
                while (pc.iceQueue.length) pc.addIceCandidate(pc.iceQueue.shift());
                return pc.createAnswer();
            })
            .then(ans => pc.setLocalDescription(ans))
            .then(() => socket.emit("signal", { to: fromSid, signal: { type: "answer", sdp: pc.localDescription } }))
            .catch(err => log(`Offer err: ${err.message}`, "error"));
    } else if (signal.type === "answer") {
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .then(() => {
                while (pc.iceQueue.length) pc.addIceCandidate(pc.iceQueue.shift());
            })
            .catch(err => log(`Answer err: ${err.message}`, "error"));
    } else if (signal.type === "ice") {
        const candidate = new RTCIceCandidate(signal.candidate);
        if (pc.remoteDescription && pc.remoteDescription.type) pc.addIceCandidate(candidate);
        else pc.iceQueue.push(candidate);
    }
}

function connectPeer(targetSid) {
    const pc = getOrCreateConnection(targetSid, true);
    pc.createOffer()
        .then(offer => pc.setLocalDescription(offer))
        .then(() => socket.emit("signal", { to: targetSid, signal: { type: "offer", sdp: pc.localDescription } }))
        .catch(err => log(`Offer err: ${err.message}`, "error"));
}

function sendText() {
    const text = document.getElementById("text-input").value.trim();
    if (!text) return;
    const targetSid = document.getElementById("peer-select").value;

    if (targetSid) {
        sendTextTo(targetSid, text);
    } else {
        peerList.forEach(p => sendTextTo(p.sid, text));
    }
    document.getElementById("text-input").value = "";
}

function sendTextTo(targetSid, text) {
    const pc = connections[targetSid];
    if (pc && pc.dataChannel && pc.dataChannel.readyState === "open") {
        pc.dataChannel.send(JSON.stringify({ t: "txt", c: text }));
        log(`Sent text (P2P)`, "p2p");
    } else {
        socket.emit("relay_text", { to: targetSid, text: text });
        log(`Sent text (Relay)`, "info");
    }
}

function handleFileSelect(e) {
    const files = e.target.files;
    if (!files.length) return;
    const targetSid = document.getElementById("peer-select").value;

    for (let file of files) {
        if (targetSid) {
            sendFileTo(targetSid, file);
        } else {
            peerList.forEach(p => sendFileTo(p.sid, file));
        }
    }
}

function sendFileTo(targetSid, file) {
    const pc = connections[targetSid];
    if (pc && pc.dataChannel && pc.dataChannel.readyState === "open") {
        if (!pendingFileQueue[targetSid]) pendingFileQueue[targetSid] = [];
        pendingFileQueue[targetSid].push({ file: file });
        if (pendingFileQueue[targetSid].length === 1) {
            socket.emit("broadcast_request", { to: targetSid, file_name: file.name, file_size: file.size, file_type: file.type });
        }
    } else {
        relaySendFile(targetSid, file);
    }
}

function startDataTransfer(targetSid) {
    const queue = pendingFileQueue[targetSid];
    if (!queue || !queue.length) return;
    const file = queue[0].file;
    const pc = connections[targetSid];
    const channel = pc.dataChannel;

    channel.send(JSON.stringify({ t: "fs", n: file.name, s: file.size, m: file.type }));

    const reader = new FileReader();
    reader.onload = e => {
        const buffer = e.target.result;
        let offset = 0;
        document.getElementById("progress-wrap").style.display = "block";

        function sendChunk() {
            while (offset < buffer.byteLength) {
                if (channel.bufferedAmount > CHUNK_SIZE * 8) {
                    setTimeout(sendChunk, 20);
                    return;
                }
                const chunk = buffer.slice(offset, offset + CHUNK_SIZE);
                channel.send(chunk);
                offset += CHUNK_SIZE;
                const pct = Math.min(100, Math.round((offset / buffer.byteLength) * 100));
                document.getElementById("progress-fill").style.width = pct + "%";
                document.getElementById("send-pct").textContent = pct + "%";
            }
            channel.send(JSON.stringify({ t: "fe" }));
            log(`Sent ${file.name} (P2P)`, "success");
            setTimeout(() => { document.getElementById("progress-wrap").style.display = "none"; }, 800);
            queue.shift();
            if (queue.length) socket.emit("broadcast_request", { to: targetSid, file_name: queue[0].file.name, file_size: queue[0].file.size });
        };
        sendChunk();
    };
    reader.readAsArrayBuffer(file);
}

function relaySendFile(targetSid, file) {
    socket.emit("relay_file_start", { to: targetSid, file_name: file.name, file_size: file.size, file_type: file.type });
    const reader = new FileReader();
    reader.onload = e => {
        const bytes = new Uint8Array(e.target.result);
        let offset = 0;
        document.getElementById("progress-wrap").style.display = "block";

        function sendRelayChunk() {
            if (offset < bytes.length) {
                const chunk = bytes.subarray(offset, offset + CHUNK_SIZE);
                let binary = '';
                for (let i = 0; i < chunk.length; i++) binary += String.fromCharCode(chunk[i]);
                socket.emit("relay_file_chunk", { to: targetSid, chunk: btoa(binary) });
                offset += CHUNK_SIZE;
                const pct = Math.min(100, Math.round((offset / bytes.length) * 100));
                document.getElementById("progress-fill").style.width = pct + "%";
                document.getElementById("send-pct").textContent = pct + "%";
                setTimeout(sendRelayChunk, 5);
            } else {
                socket.emit("relay_file_done", { to: targetSid });
                log(`Sent ${file.name} (Relay)`, "success");
                setTimeout(() => { document.getElementById("progress-wrap").style.display = "none"; }, 800);
            }
        }
        sendRelayChunk();
    };
    reader.readAsArrayBuffer(file);
}

function handleDataMessage(data, fromSid) {
    if (typeof data === "string") {
        const msg = JSON.parse(data);
        if (msg.t === "txt") {
            addReceived("text", msg.c, fromSid);
        } else if (msg.t === "fs") {
            const pc = connections[fromSid];
            pc.fileMeta = msg;
            pc.receiveBuffer = [];
        } else if (msg.t === "fe") {
            const pc = connections[fromSid];
            const blob = new Blob(pc.receiveBuffer, { type: pc.fileMeta.m });
            const url = URL.createObjectURL(blob);
            addReceived("file", { name: pc.fileMeta.n, size: pc.fileMeta.s, url: url, type: pc.fileMeta.m }, fromSid);
            log(`Received ${pc.fileMeta.n}`, "success");
        }
    } else {
        const pc = connections[fromSid];
        if (pc && pc.receiveBuffer) pc.receiveBuffer.push(data);
    }
}

function addReceived(type, data, sender) {
    const list = document.getElementById("received-list");
    const li = document.createElement("li");
    li.className = "feed-item";
    const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

    let actionHtml = "";
    if (type === "text") {
        actionHtml = `<div class="feed-body">${escapeHtml(data)}</div>
        <button class="flat icon-only" onclick="navigator.clipboard.writeText('${escapeHtml(data)}')" title="Copy" style="width:26px;height:26px;padding:4px;">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
        </button>`;
    } else {
        actionHtml = `<div class="feed-body"><b>${escapeHtml(data.name)}</b> <span style="font-size:11px;color:#64748b;">(${formatBytes(data.size)})</span></div>
        <a href="${data.url}" download="${data.name}" style="display:inline-flex;align-items:center;gap:4px;padding:4px 8px;background:#0f172a;color:#fff;text-decoration:none;border-radius:4px;font-size:11px;font-weight:500;">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
            Download
        </a>`;
    }

    li.innerHTML = `<div class="feed-meta"><span>${escapeHtml(sender)}</span><span>${time}</span></div>${actionHtml}`;
    list.insertBefore(li, list.firstChild);
}

function respondRequest(accepted) {
    document.getElementById("request-modal").style.display = "none";
    if (pendingRequest) {
        socket.emit("broadcast_response", { to: pendingRequest.from, accepted: accepted });
        pendingRequest = null;
    }
}

const dropZone = document.getElementById("drop-zone");
dropZone.addEventListener("dragover", e => { e.preventDefault(); dropZone.classList.add("dragover"); });
dropZone.addEventListener("dragleave", () => dropZone.classList.remove("dragover"));
dropZone.addEventListener("drop", e => {
    e.preventDefault();
    dropZone.classList.remove("dragover");
    handleFileSelect({ target: { files: e.dataTransfer.files } });
});

window.onload = initSocket;
</script>
</body>
</html>
"""

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
