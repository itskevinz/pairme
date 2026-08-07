from flask import Flask, render_template_string, request, jsonify
import time

app = Flask(__name__)

DEVICES = {}
INBOX = {}

def cleanup_expired():
    now = time.time()
    expired_devs = [dev_id for dev_id, info in DEVICES.items() if now - info["last_seen"] > 86400]
    for dev_id in expired_devs:
        DEVICES.pop(dev_id, None)
        INBOX.pop(dev_id, None)

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route("/api/register", methods=["POST"])
def register():
    cleanup_expired()
    data = request.get_json() or {}
    device_id = data.get("device_id")
    device_name = data.get("device_name", "Unknown Device")[:30]
    pub_key = data.get("pub_key", "")

    if not device_id:
        return jsonify({"error": "Missing device_id"}), 400

    DEVICES[device_id] = {
        "name": device_name,
        "pub_key": pub_key,
        "last_seen": time.time()
    }
    if device_id not in INBOX:
        INBOX[device_id] = []

    return jsonify({"status": "ok"})

@app.route("/api/devices", methods=["GET"])
def get_devices():
    cleanup_expired()
    now = time.time()
    result = []
    for dev_id, info in DEVICES.items():
        result.append({
            "device_id": dev_id,
            "name": info["name"],
            "pub_key": info["pub_key"],
            "online": (now - info["last_seen"]) < 15
        })
    return jsonify({"devices": result})

@app.route("/api/send", methods=["POST"])
def send_msg():
    data = request.get_json() or {}
    target_id = data.get("target_id")
    sender_id = data.get("sender_id")
    payload = data.get("payload")

    if not target_id or not payload:
        return jsonify({"error": "Invalid payload"}), 400

    if target_id not in INBOX:
        INBOX[target_id] = []

    INBOX[target_id].append({
        "sender_id": sender_id,
        "sender_name": DEVICES.get(sender_id, {}).get("name", "Unknown"),
        "payload": payload,
        "timestamp": time.time()
    })
    return jsonify({"status": "queued"})

@app.route("/api/poll", methods=["POST"])
def poll_msg():
    data = request.get_json() or {}
    device_id = data.get("device_id")
    if not device_id:
        return jsonify({"error": "Missing device_id"}), 400

    if device_id in DEVICES:
        DEVICES[device_id]["last_seen"] = time.time()

    messages = INBOX.get(device_id, [])
    INBOX[device_id] = []
    return jsonify({"messages": messages})

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PairMe Ultra Security</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        body { background: #0f172a; color: #f8fafc; padding: 16px; display: flex; flex-direction: column; gap: 12px; max-width: 900px; margin: 0 auto; min-height: 100vh; }
        header { background: #1e293b; padding: 12px 16px; border-radius: 8px; display: flex; justify-content: space-between; align-items: center; border: 1px solid #334155; }
        .grid { display: grid; grid-template-columns: 280px 1fr; gap: 12px; flex: 1; }
        @media (max-width: 640px) { .grid { grid-template-columns: 1fr; } }
        .panel { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 12px; display: flex; flex-direction: column; gap: 10px; }
        input, select, textarea, button { background: #0f172a; color: #f8fafc; border: 1px solid #475569; padding: 8px 12px; border-radius: 6px; font-size: 13px; outline: none; }
        button { background: #2563eb; border: none; font-weight: 600; cursor: pointer; }
        button:hover { opacity: 0.9; }
        .device-item { background: #0f172a; border: 1px solid #334155; padding: 8px; border-radius: 6px; display: flex; justify-content: space-between; align-items: center; cursor: pointer; }
        .device-item.active { border-color: #2563eb; }
        .status-dot { width: 8px; height: 8px; border-radius: 50%; background: #64748b; }
        .status-dot.online { background: #22c55e; }
        .msg-box { background: #0f172a; border: 1px solid #334155; padding: 10px; border-radius: 6px; font-size: 13px; display: flex; flex-direction: column; gap: 4px; word-break: break-all; }
        .msg-header { font-size: 11px; color: #94a3b8; display: flex; justify-content: space-between; }
        .badge { background: #059669; color: white; padding: 2px 6px; border-radius: 4px; font-size: 10px; }
    </style>
</head>
<body>

    <header>
        <div style="font-weight:700;font-size:16px;">PairMe E2EE Serverless</div>
        <div style="font-size:11px;color:#94a3b8;font-family:monospace;" id="my-dev-id">---</div>
    </header>

    <div class="grid">
        <div class="panel">
            <div style="font-weight:600;font-size:13px;color:#94a3b8;">DEVICE CONFIG</div>
            <input type="text" id="dev-name" placeholder="Device Name" onchange="updateName()">
            <input type="password" id="secret-key" placeholder="Encryption Key / Room Secret" value="SuperSecretKey123">
            
            <div style="font-weight:600;font-size:13px;color:#94a3b8;margin-top:10px;">DEVICES</div>
            <div id="device-list" style="display:flex;flex-direction:column;gap:6px;"></div>
        </div>

        <div class="panel">
            <div style="display:flex;justify-content:space-between;align-items:center;">
                <span style="font-weight:600;font-size:13px;color:#94a3b8;" id="target-label">Target: ALL</span>
                <span class="badge">E2EE AES-GCM 256</span>
            </div>

            <textarea id="msg-input" rows="3" placeholder="Type encrypted message..."></textarea>
            <div style="display:flex;gap:8px;">
                <button style="flex:1;" onclick="sendText()">Send Text</button>
                <input type="file" id="file-input" style="display:none;" onchange="sendFile()">
                <button style="background:#475569;" onclick="document.getElementById('file-input').click()">Send File</button>
            </div>

            <div style="font-weight:600;font-size:13px;color:#94a3b8;margin-top:10px;">INBOX / MESSAGES</div>
            <div id="messages" style="display:flex;flex-direction:column;gap:8px;overflow-y:auto;max-height:500px;"></div>
        </div>
    </div>

<script>
var DEVICE_ID = localStorage.getItem("pairme_device_id");
if (!DEVICE_ID) {
    DEVICE_ID = 'dev_' + crypto.randomUUID().replaceAll('-', '').slice(0, 12);
    localStorage.setItem("pairme_device_id", DEVICE_ID);
}
document.getElementById("my-dev-id").textContent = DEVICE_ID;

var myName = localStorage.getItem("pairme_device_name") || ("Device " + DEVICE_ID.slice(-4));
document.getElementById("dev-name").value = myName;

var selectedTarget = "";
var activeDevices = [];

async function getKey() {
    var secret = document.getElementById("secret-key").value || "default_key";
    var enc = new TextEncoder();
    var keyMaterial = await crypto.subtle.importKey("raw", enc.encode(secret), { name: "PBKDF2" }, false, ["deriveKey"]);
    return crypto.subtle.deriveKey(
        { name: "PBKDF2", salt: enc.encode("pairme_salt"), iterations: 100000, hash: "SHA-256" },
        keyMaterial,
        { name: "AES-GCM", length: 256 },
        false,
        ["encrypt", "decrypt"]
    );
}

async function encryptData(dataString) {
    var key = await getKey();
    var iv = crypto.getRandomValues(new Uint8Array(12));
    var enc = new TextEncoder();
    var ciphertext = await crypto.subtle.encrypt({ name: "AES-GCM", iv: iv }, key, enc.encode(dataString));
    return {
        ct: btoa(String.fromCharCode(...new Uint8Array(ciphertext))),
        iv: btoa(String.fromCharCode(...iv))
    };
}

async function decryptData(encryptedObj) {
    try {
        var key = await getKey();
        var iv = new Uint8Array(atob(encryptedObj.iv).split("").map(c => c.charCodeAt(0)));
        var ct = new Uint8Array(atob(encryptedObj.ct).split("").map(c => c.charCodeAt(0)));
        var decrypted = await crypto.subtle.decrypt({ name: "AES-GCM", iv: iv }, key, ct);
        return new TextDecoder().decode(decrypted);
    } catch (e) {
        return "[Decryption Failed - Check Secret Key]";
    }
}

async function registerDevice() {
    await fetch("/api/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ device_id: DEVICE_ID, device_name: myName })
    });
}

function updateName() {
    myName = document.getElementById("dev-name").value;
    localStorage.setItem("pairme_device_name", myName);
    registerDevice();
}

async function loadDevices() {
    var res = await fetch("/api/devices");
    var data = await res.json();
    activeDevices = data.devices || [];
    renderDevices();
}

function renderDevices() {
    var container = document.getElementById("device-list");
    container.innerHTML = "";
    activeDevices.forEach(dev => {
        if (dev.device_id === DEVICE_ID) return;
        var div = document.createElement("div");
        div.className = "device-item " + (selectedTarget === dev.device_id ? "active" : "");
        div.onclick = () => {
            selectedTarget = (selectedTarget === dev.device_id) ? "" : dev.device_id;
            document.getElementById("target-label").textContent = "Target: " + (selectedTarget ? dev.name : "ALL");
            renderDevices();
        };
        div.innerHTML = `<div><div style="font-weight:600;">${escapeHtml(dev.name)}</div><div style="font-size:10px;color:#94a3b8;">${dev.device_id}</div></div>
                         <div class="status-dot ${dev.online ? 'online' : ''}"></div>`;
        container.appendChild(div);
    });
}

async function sendPayload(targetId, payload) {
    await fetch("/api/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            sender_id: DEVICE_ID,
            target_id: targetId,
            payload: payload
        })
    });
}

async function sendText() {
    var text = document.getElementById("msg-input").value.trim();
    if (!text) return;
    var encrypted = await encryptData(JSON.stringify({ type: "text", content: text }));
    var targets = selectedTarget ? [selectedTarget] : activeDevices.map(d => d.device_id).filter(id => id !== DEVICE_ID);
    
    for (var t of targets) {
        await sendPayload(t, encrypted);
    }
    addMessageToUI(myName, text, "text", true);
    document.getElementById("msg-input").value = "";
}

async function sendFile() {
    var fileInput = document.getElementById("file-input");
    if (!fileInput.files.length) return;
    var file = fileInput.files[0];
    var reader = new FileReader();
    
    reader.onload = async function(e) {
        var base64 = e.target.result;
        var encrypted = await encryptData(JSON.stringify({
            type: "file",
            name: file.name,
            size: file.size,
            data: base64
        }));
        var targets = selectedTarget ? [selectedTarget] : activeDevices.map(d => d.device_id).filter(id => id !== DEVICE_ID);
        for (var t of targets) {
            await sendPayload(t, encrypted);
        }
        addMessageToUI(myName, file.name + " (" + Math.round(file.size/1024) + " KB)", "file", true);
        fileInput.value = "";
    };
    reader.readAsDataURL(file);
}

async function pollMessages() {
    try {
        var res = await fetch("/api/poll", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ device_id: DEVICE_ID })
        });
        var data = await res.json();
        if (data.messages && data.messages.length) {
            for (var msg of data.messages) {
                var decryptedStr = await decryptData(msg.payload);
                try {
                    var obj = JSON.parse(decryptedStr);
                    if (obj.type === "text") {
                        addMessageToUI(msg.sender_name, obj.content, "text", false);
                    } else if (obj.type === "file") {
                        addFileToUI(msg.sender_name, obj.name, obj.data);
                    }
                } catch(e) {
                    addMessageToUI(msg.sender_name, decryptedStr, "text", false);
                }
            }
        }
    } catch(e) {}
}

function addMessageToUI(sender, content, type, isSelf) {
    var box = document.getElementById("messages");
    var div = document.createElement("div");
    div.className = "msg-box";
    div.innerHTML = `<div class="msg-header"><span>${escapeHtml(sender)} ${isSelf ? '(You)' : ''}</span><span>${new Date().toLocaleTimeString()}</span></div>
                     <div>${escapeHtml(content)}</div>`;
    box.insertBefore(div, box.firstChild);
}

function addFileToUI(sender, fileName, fileData) {
    var box = document.getElementById("messages");
    var div = document.createElement("div");
    div.className = "msg-box";
    div.innerHTML = `<div class="msg-header"><span>${escapeHtml(sender)}</span><span>${new Date().toLocaleTimeString()}</span></div>
                     <div><strong>File:</strong> ${escapeHtml(fileName)}</div>
                     <a href="${fileData}" download="${escapeHtml(fileName)}" style="color:#60a5fa;font-size:12px;margin-top:4px;display:inline-block;">Download File</a>`;
    box.insertBefore(div, box.firstChild);
}

function escapeHtml(str) {
    var div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

registerDevice();
setInterval(registerDevice, 10000);
setInterval(loadDevices, 3000);
setInterval(pollMessages, 2000);
loadDevices();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
