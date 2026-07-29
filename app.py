from gevent import monkey
monkey.patch_all()

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, join_room, leave_room
import uuid, time, random

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pairme")

app = Flask(__name__)
app.config["SECRET_KEY"] = "pairme-secret-key-change-in-production"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent", ping_timeout=60, ping_interval=25)

peers = {}
rooms = {}

def generate_code():
    return str(random.randint(100000, 999999))

@app.route("/")
def index():
    return render_template("index.html")

@socketio.on("connect")
def handle_connect():
    log.info("Connect: %s", request.sid)
    peer_id = str(uuid.uuid4())[:8]
    peers[request.sid] = {
        "id": peer_id,
        "name": "Device " + peer_id[-4:].upper(),
        "joined": time.time(),
        "room": None
    }
    emit("init", {"peer_id": peer_id, "sid": request.sid})
    broadcast_peers()

@socketio.on("disconnect")
def handle_disconnect():
    log.info("Disconnect: %s", request.sid)
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
    if old_room and old_room in rooms and sid in rooms[old_room]:
        rooms[old_room].remove(sid)
        leave_room(old_room)
        if not rooms[old_room]:
            del rooms[old_room]
    join_room(code)
    peers[sid]["room"] = code
    if code not in rooms:
        rooms[code] = []
    if sid not in rooms[code]:
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
    if old_room and old_room in rooms and sid in rooms[old_room]:
        rooms[old_room].remove(sid)
        leave_room(old_room)
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
    room = peers[sid].get("room")
    if room and room in rooms and sid in rooms[room]:
        rooms[room].remove(sid)
        leave_room(room)
        if not rooms[room]:
            del rooms[room]
    peers[sid]["room"] = None
    emit("room_left", {})
    broadcast_peers()

@socketio.on("signal")
def handle_signal(data):
    target_sid = data.get("to")
    if target_sid and target_sid in peers:
        emit("signal", {
            "from": request.sid,
            "from_peer": peers[request.sid]["id"],
            "from_name": peers[request.sid]["name"],
            "signal": data.get("signal")
        }, room=target_sid)

@socketio.on("broadcast_request")
def handle_broadcast_request(data):
    target_sid = data.get("to")
    if target_sid and target_sid in peers:
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
    if target_sid and target_sid in peers:
        emit("transfer_response", {
            "from": request.sid,
            "accepted": data.get("accepted", False)
        }, room=target_sid)

@socketio.on("relay_text")
def handle_relay_text(data):
    target_sid = data.get("to")
    if target_sid and target_sid in peers:
        emit("relay_text", {
            "from": request.sid,
            "from_name": peers[request.sid]["name"],
            "text": data.get("text", "")
        }, room=target_sid)

@socketio.on("relay_file_start")
def handle_relay_file_start(data):
    target_sid = data.get("to")
    if target_sid and target_sid in peers:
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
    if target_sid and target_sid in peers:
        emit("relay_file_chunk", {
            "from": request.sid,
            "chunk": data.get("chunk", "")
        }, room=target_sid)

@socketio.on("relay_file_done")
def handle_relay_file_done(data):
    target_sid = data.get("to")
    if target_sid and target_sid in peers:
        emit("relay_file_done", {"from": request.sid}, room=target_sid)

# THAY TOÀN BỘ HÀM broadcast_peers() THÀNH:
def broadcast_peers():
    log.info("Broadcast peers to %s peers", len(peers))
    for sid in list(peers.keys()):
        room = peers[sid].get("room")
        peer_list = []
        for other_sid, info in peers.items():
            if other_sid == sid:
                continue
            if room is None:
                if info.get("room") is None:
                    peer_list.append({"sid": other_sid, "id": info["id"], "name": info["name"]})
            else:
                if info.get("room") == room:
                    peer_list.append({"sid": other_sid, "id": info["id"], "name": info["name"]})
        socketio.emit("peers", peer_list, room=sid)

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
