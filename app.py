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
        emit("room_error", {"msg": "Invalid code. Use 6 digits."})
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
    <title>PairMe - P2P Share</title>
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Segoe UI', system-ui, sans-serif; }
        body { background: #0f172a; color: #f8fafc; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }
        header { background: #1e293b; padding: 12px 24px; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: center; }
        header h1 { font-size: 20px; color: #38bdf8; letter-spacing: 0.5px; }
        header .status-badge { background: #0284c7; color: #fff; padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }
        
        .dashboard { display: grid; grid-template-columns: 280px 1fr 340px; gap: 12px; padding: 12px; height: calc(100vh - 57px); }
        .panel { background: #1e293b; border: 1px solid #334155; border-radius: 8px; display: flex; flex-direction: column; overflow: hidden; }
        .panel-header { background: #0f172a; padding: 10px 14px; font-size: 13px; font-weight: 700; text-transform: uppercase; color: #94a3b8; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: center; }
        .panel-body { padding: 12px; flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 12px; }

        input, select, textarea, button { font-size: 13px; border-radius: 6px; border: 1px solid #334155; background: #0f172a; color: #f8fafc; padding: 8px 10px; outline: none; }
        input:focus, select:focus, textarea:focus { border-color: #38bdf8; }
        button { background: #0284c7; color: #fff; border: none; font-weight: 600; cursor: pointer; transition: 0.2s; padding: 8px 14px; }
        button:hover { background: #0369a1; }
        button.secondary { background: #334155; color: #f8fafc; }
        button.secondary:hover { background: #475569; }

        .row { display: flex; gap: 8px; align-items: center; }
        .flex-1 { flex: 1; }

        .peer-item { background: #0f172a; border: 1px solid #334155; padding: 10px; border-radius: 6px; display: flex; justify-content: space-between; align-items: center; cursor: pointer; }
        .peer-item:hover, .peer-item.selected { border-color: #38bdf8; background: #1e293b; }
        .peer-name { font-weight: 600; font-size: 14px; }
        .peer-id { font-size: 11px; color: #64748b; font-family: monospace; }

        .drop-zone { border: 2px dashed #334155; border-radius: 8px; padding: 20px; text-align: center; color: #94a3b8; cursor: pointer; transition: 0.2s; }
        .drop-zone:hover, .drop-zone.dragover { border-color: #38bdf8; background: #0f172a; color: #38bdf8; }

        .received-list { list-style: none; display: flex; flex-direction: column; gap: 8px; }
        .received-item { background: #0f172a; border: 1px solid #334155; padding: 10px; border-radius: 6px; font-size: 13px; }
        .received-header { font-size: 11px; color: #64748b; margin-bottom: 4px; display: flex; justify-content: space-between; }
        .received-content { word-break: break-all; white-space: pre-wrap; margin: 6px 0; }

        #log-container { background: #090d16; font-family: monospace; font-size: 11px; padding: 8px; flex: 1; overflow-y: auto; border-radius: 6px; border: 1px solid #334155; }
        .log-line { margin-bottom: 4px; line-height: 1.4; word-break: break-all; }
        .log-time { color: #475569; }
        .log-badge { padding: 1px 4px; border-radius: 3px; font-weight: bold; font-size: 10px; margin-right: 4px; }
        .badge-info { background: #1e3a8a; color: #93c5fd; }
        .badge-success { background: #065f46; color: #6ee7b7; }
        .badge-warn { background: #78350f; color: #fde68a; }
        .badge-error { background: #881337; color: #fca5a5; }
        .badge-p2p { background: #581c87; color: #e9d5ff; }

        .progress-bar { height: 6px; background: #334155; border-radius: 3px; overflow: hidden; margin-top: 6px; }
        .progress-fill { height: 100%; background: #38bdf8; width: 0%; transition: width 0.1s; }

        .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7); z-index: 100; justify-content: center; align-items: center; }
        .modal { background: #1e293b; border: 1px solid #334155; padding: 20px; border-radius: 8px; width: 320px; text-align: center; }

        @media (max-width: 900px) {
            body { height: auto; overflow: auto; }
            .dashboard { grid-template-columns: 1fr; height: auto; }
        }
    </style>
</head>
<body>

    <header>
        <h1>PAIRME P2P</h1>
        <div class="status-badge" id="room-badge">Lobby</div>
    </header>

    <div class="dashboard">
        <div class="panel">
            <div class="panel-header">Device & Room</div>
            <div class="panel-body">
                <div>
                    <label style="font-size:11px;color:#94a3b8;">Your Device ID</label>
                    <div id="my-id" style="font-family:monospace;font-size:14px;color:#38bdf8;font-weight:bold;">Connecting...</div>
                </div>
                <div class="row">
                    <input type="text" id="my-name" placeholder="Device Name" class="flex-1">
                    <button onclick="updateName()">Save</button>
                </div>
                <hr style="border-color:#334155;">
                <div class="row">
                    <input type="text" id="room-code-input" placeholder="6-digit Code" maxlength="6" class="flex-1">
                    <button onclick="joinRoom()">Join</button>
                </div>
                <div class="row">
                    <button onclick="createRoom()" class="secondary flex-1">Create Room</button>
                    <button onclick="leaveRoom()" class="secondary">Leave</button>
                </div>
                <div class="panel-header" style="margin:-12px -12px 0 -12px;">Nearby Devices</div>
                <div id="peer-list" style="display:flex;flex-direction:column;gap:6px;">
                    <div style="color:#64748b;font-style:italic;font-size:12px;">No devices found</div>
                </div>
            </div>
        </div>

        <div class="panel">
            <div class="panel-header">
                <span>Transfer Workspace</span>
                <span id="target-peer-label" style="color:#38bdf8;text-transform:none;">To: Everyone</span>
            </div>
            <div class="panel-body">
                <div class="row">
                    <select id="peer-select" class="flex-1" onchange="onPeerSelectChange()">
                        <option value="">-- Send to All Devices --</option>
                    </select>
                </div>
                <textarea id="text-input" rows="3" placeholder="Type message, paste links or code here..."></textarea>
                <button onclick="sendText()">Send Text</button>

                <div class="drop-zone" id="drop-zone" onclick="document.getElementById('file-input').click()">
                    Drag & Drop Files Here or Click to Select
                    <input type="file" id="file-input" multiple style="display:none;" onchange="handleFileSelect(event)">
                </div>

                <div id="file-queue-list" style="font-size:12px;color:#94a3b8;"></div>

                <div id="progress-wrap" style="display:none;">
                    <div class="row" style="justify-space-between;font-size:12px;">
                        <span id="send-status">Sending...</span>
                        <span id="send-pct">0%</span>
                    </div>
                    <div class="progress-bar"><div class="progress-fill" id="progress-fill"></div></div>
                </div>

                <div class="panel-header" style="margin:0 -12px;">Received Items</div>
                <ul class="received-list" id="received-list"></ul>
            </div>
        </div>

        <div class="panel">
            <div class="panel-header">
                <span>System Logs</span>
                <button onclick="clearLogs()" class="secondary" style="padding:2px 6px;font-size:10px;">Clear</button>
            </div>
            <div class="panel-body" style="padding:8px;">
                <div id="log-container"></div>
            </div>
        </div>
    </div>

    <div class="modal-overlay" id="request-modal">
        <div class="modal">
            <h3 style="margin-bottom:10px;">Incoming Transfer</h3>
            <p id="request-details" style="font-size:13px;color:#94a3b8;margin-bottom:15px;"></p>
            <div class="row" style="justify-content:center;">
                <button class="secondary" onclick="respondRequest(false)">Decline</button>
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
        { urls: "stun:stun1.l.google.com:19302" },
        { urls: "stun:stun2.l.google.com:19302" }
    ]
};

function log(msg, type = "info") {
    const el = document.getElementById("log-container");
    const time = new Date().toLocaleTimeString();
    const line = document.createElement("div");
    line.className = "log-line";
    line.innerHTML = `<span class="log-time">[${time}]</span> <span class="log-badge badge-${type}">${type.toUpperCase()}</span> ${escapeHtml(msg)}`;
    el.appendChild(line);
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
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
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
        log(`Initialized Device ID: ${myPeerId}`, "info");
    });

    socket.on("peers", data => {
        peerList = data;
        renderPeers();
    });

    socket.on("signal", handleSignal);

    socket.on("transfer_request", data => {
        pendingRequest = data;
        document.getElementById("request-details").textContent = `${data.from_name} wants to send ${data.file_name} (${formatBytes(data.file_size)})`;
        document.getElementById("request-modal").style.display = "flex";
    });

    socket.on("transfer_response", data => {
        if (data.accepted) {
            log("Transfer accepted by peer", "success");
            startDataTransfer(data.from);
        } else {
            log("Transfer declined by peer", "warn");
            alert("Transfer declined");
        }
    });

    socket.on("room_joined", data => {
        document.getElementById("room-badge").textContent = "Room: " + data.code;
        log(`Joined room ${data.code}`, "success");
    });

    socket.on("room_left", () => {
        document.getElementById("room-badge").textContent = "Lobby";
        log("Left room back to Lobby", "info");
    });

    socket.on("relay_text", data => {
        addReceived("text", data.text, data.from_name);
        log(`Text received via Relay from ${data.from_name}`, "info");
    });

    let relayBuffer = {};
    let relayMeta = {};

    socket.on("relay_file_start", data => {
        relayBuffer[data.from] = [];
        relayMeta[data.from] = data;
        log(`Receiving file relay: ${data.file_name}`, "info");
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
            log(`File received via Relay: ${meta.file_name}`, "success");
            delete relayBuffer[data.from];
            delete relayMeta[data.from];
        }
    });
}

function renderPeers() {
    const list = document.getElementById("peer-list");
    const select = document.getElementById("peer-select");
    list.innerHTML = "";
    select.innerHTML = '<option value="">-- Send to All Devices --</option>';

    if (peerList.length === 0) {
        list.innerHTML = '<div style="color:#64748b;font-style:italic;font-size:12px;">No devices found</div>';
        return;
    }

    peerList.forEach(p => {
        const item = document.createElement("div");
        item.className = "peer-item";
        item.onclick = () => selectPeer(p.sid);
        item.innerHTML = `<div><div class="peer-name">${escapeHtml(p.name)}</div><div class="peer-id">${p.id}</div></div>`;
        list.appendChild(item);

        const opt = document.createElement("option");
        opt.value = p.sid;
        opt.textContent = `${p.name} (${p.id})`;
        select.appendChild(opt);
    });
    log(`Scanned ${peerList.length} device(s)`, "info");
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
        log(`Updated device name to: ${name}`, "success");
    }
}

function joinRoom() {
    const code = document.getElementById("room-code-input").value.trim();
    if (code.length === 6) socket.emit("join_room_code", { code: code });
    else alert("Enter 6-digit code");
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
    channel.onopen = () => log(`P2P DataChannel connected with ${targetSid.slice(0, 5)}`, "p2p");
    channel.onmessage = e => handleDataMessage(e.data, targetSid);
    channel.onerror = err => log(`DataChannel Error: ${err.message}`, "error");
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
            .catch(err => log(`Offer Signal error: ${err.message}`, "error"));
    } else if (signal.type === "answer") {
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .then(() => {
                while (pc.iceQueue.length) pc.addIceCandidate(pc.iceQueue.shift());
            })
            .catch(err => log(`Answer Signal error: ${err.message}`, "error"));
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
        .catch(err => log(`Create Offer error: ${err.message}`, "error"));
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
        log(`Text sent via P2P -> ${targetSid.slice(0, 5)}`, "p2p");
    } else {
        socket.emit("relay_text", { to: targetSid, text: text });
        log(`Text sent via Relay -> ${targetSid.slice(0, 5)}`, "info");
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
            log(`P2P File sent: ${file.name}`, "success");
            setTimeout(() => { document.getElementById("progress-wrap").style.display = "none"; }, 1000);
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
                log(`Relay File sent: ${file.name}`, "success");
                setTimeout(() => { document.getElementById("progress-wrap").style.display = "none"; }, 1000);
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
            log(`File received P2P: ${pc.fileMeta.n}`, "success");
        }
    } else {
        const pc = connections[fromSid];
        if (pc && pc.receiveBuffer) pc.receiveBuffer.push(data);
    }
}

function addReceived(type, data, sender) {
    const list = document.getElementById("received-list");
    const li = document.createElement("li");
    li.className = "received-item";
    const time = new Date().toLocaleTimeString();

    let contentHtml = "";
    if (type === "text") {
        contentHtml = `<div class="received-content">${escapeHtml(data)}</div>
        <button onclick="navigator.clipboard.writeText('${escapeHtml(data)}')" class="secondary" style="padding:2px 8px;font-size:11px;">Copy</button>`;
    } else {
        contentHtml = `<div class="received-content"><b>${escapeHtml(data.name)}</b> (${formatBytes(data.size)})</div>
        <a href="${data.url}" download="${data.name}" class="button" style="display:inline-block;padding:4px 10px;background:#0284c7;color:#fff;text-decoration:none;border-radius:4px;font-size:11px;">Download</a>`;
    }

    li.innerHTML = `<div class="received-header"><span>From: ${escapeHtml(sender)}</span><span>${time}</span></div>${contentHtml}`;
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
