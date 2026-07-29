var socket = null;
var mySid = "";
var myPeerId = "";
var serverRelayBuffer = {};
var serverRelayMeta = {};
var peerList = [];
var connections = {};
var pendingRequest = null;
var pendingFileQueue = {};
var CHUNK_SIZE = 16384;
var STUN_SERVERS = [
    {urls: "stun:stun.l.google.com:19302"},
    {urls: "stun:stun1.l.google.com:19302"},
    {urls: "turn:openrelay.metered.ca:80", username: "openrelayproject", credential: "openrelayproject"},
    {urls: "turn:openrelay.metered.ca:443", username: "openrelayproject", credential: "openrelayproject"}
];

function log(msg) {
    var el = document.getElementById("log");
    var time = new Date().toLocaleTimeString();
    el.textContent = "[" + time + "] " + msg + "\n" + el.textContent;
}

function checkWebRTC() {
    if (!window.RTCPeerConnection) {
        log("ERROR: WebRTC not supported.");
        alert("WebRTC not supported. Use a modern browser.");
        return false;
    }
    return true;
}

function loadName() {
    var saved = localStorage.getItem("pairme_name");
    if (saved) {
        document.getElementById("my-name").value = saved;
    }
}

function saveName(name) {
    localStorage.setItem("pairme_name", name);
}

function initSocket() {
    socket = io({transports: ["websocket", "polling"]});

    socket.on("connect", function() {
        log("Connected to server");
        var saved = localStorage.getItem("pairme_name");
        if (saved) {
            socket.emit("set_name", {name: saved});
        }
    });

    socket.on("init", function(data) {
        mySid = data.sid;
        myPeerId = data.peer_id;
        document.getElementById("my-id").textContent = "ID: " + myPeerId;
        log("Your ID: " + myPeerId);
    });

    socket.on("peers", function(data) {
        peerList = [];
        for (var i = 0; i < data.length; i++) {
            if (data[i].sid !== mySid) {
                peerList.push(data[i]);
            }
        }
        renderPeers();
    });

    socket.on("signal", function(data) {
        handleSignal(data);
    });

    socket.on("transfer_request", function(data) {
        pendingRequest = data;
        document.getElementById("request-details").textContent =
            data.from_name + " wants to send: " + data.file_name +
            " (" + formatBytes(data.file_size) + ")";
        document.getElementById("request-modal").style.display = "block";
    });

    socket.on("transfer_response", function(data) {
        if (data.accepted) {
            log("Transfer accepted");
            startDataTransfer(data.from);
        } else {
            log("Transfer declined");
            document.getElementById("send-status").textContent = "Declined";
        }
    });

    socket.on("room_joined", function(data) {
        document.getElementById("room-status").textContent = "Room: " + data.code;
        log("Joined room " + data.code);
    });

    socket.on("room_left", function() {
        document.getElementById("room-status").textContent = "No room joined";
        log("Left room");
    });

    socket.on("room_error", function(data) {
        log("Room error: " + data.msg);
        alert(data.msg);
    });

    socket.on("disconnect", function() {
        log("Disconnected from server");
    });
    socket.on("relay_text", function(data) {
        addReceived("text", data.text, data.from);
        log("Text relayed from " + data.from.slice(0,6));
    });

    socket.on("relay_file_start", function(data) {
        serverRelayBuffer[data.from] = [];
        serverRelayMeta[data.from] = {
            name: data.file_name,
            size: data.file_size,
            type: data.file_type
        };
        log("File relay start: " + data.file_name);
    });

    socket.on("relay_file_chunk", function(data) {
        if (!serverRelayBuffer[data.from]) serverRelayBuffer[data.from] = [];
        var binary = atob(data.chunk);
        var bytes = new Uint8Array(binary.length);
        for (var i = 0; i < binary.length; i++) {
            bytes[i] = binary.charCodeAt(i);
        }
        serverRelayBuffer[data.from].push(bytes.buffer);
    });

    socket.on("relay_file_done", function(data) {
        var meta = serverRelayMeta[data.from];
        var buffers = serverRelayBuffer[data.from];
        if (meta && buffers) {
            var blob = new Blob(buffers, {type: meta.type});
            var url = URL.createObjectURL(blob);
            addReceived("file", {name: meta.name, size: meta.size, type: meta.type, url: url}, data.from);
            log("File relay done: " + meta.name);
            delete serverRelayBuffer[data.from];
            delete serverRelayMeta[data.from];
        }
    });
}

function formatBytes(bytes) {
    if (bytes === 0) return "0 B";
    var k = 1024;
    var sizes = ["B", "KB", "MB", "GB"];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

function renderPeers() {
    log("Peers updated: " + peerList.length + " found");
    var list = document.getElementById("peer-list");
    var selText = document.getElementById("text-peer-select");
    var selFile = document.getElementById("file-peer-select");

    list.innerHTML = "";
    selText.innerHTML = '<option value="">-- Select peer --</option>';
    selFile.innerHTML = '<option value="">-- Select peer --</option>';

    if (peerList.length === 0) {
        list.innerHTML = '<li style="color:#666;font-style:italic;">No devices found. Join a room or wait.</li>';
        return;
    }

    for (var i = 0; i < peerList.length; i++) {
        var p = peerList[i];
        var li = document.createElement("li");
        li.innerHTML = '<div class="peer-name">' + escapeHtml(p.name) +
            '</div><div class="peer-id">' + p.id + '</div>';
        list.appendChild(li);

        var opt1 = document.createElement("option");
        opt1.value = p.sid;
        opt1.textContent = p.name + " (" + p.id + ")";
        selText.appendChild(opt1);

        var opt2 = document.createElement("option");
        opt2.value = p.sid;
        opt2.textContent = p.name + " (" + p.id + ")";
        selFile.appendChild(opt2);
    }
}

function escapeHtml(text) {
    var div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
}

function updateName() {
    var name = document.getElementById("my-name").value.trim();
    if (name) {
        socket.emit("set_name", {name: name});
        saveName(name);
        log("Name set: " + name);
    }
}

function joinRoom() {
    var code = document.getElementById("room-code-input").value.trim();
    if (!code || code.length !== 6) {
        alert("Enter a 6-digit code");
        return;
    }
    socket.emit("join_room_code", {code: code});
}

function createRoom() {
    socket.emit("create_room_code");
}

function leaveRoom() {
    socket.emit("leave_room_code");
}

function getOrCreateConnection(targetSid, isInitiator) {
    if (connections[targetSid]) {
        return connections[targetSid];
    }

    var pc = new RTCPeerConnection({iceServers: STUN_SERVERS});
    connections[targetSid] = pc;

    pc.targetSid = targetSid;
    pc.isInitiator = isInitiator;
    pc.dataChannel = null;
    pc.receiveBuffer = [];
    pc.receivedSize = 0;
    pc.expectedSize = 0;
    pc.receiveFileName = "";
    pc.receiveFileType = "";

    pc.onicecandidate = function(event) {
        if (event.candidate) {
            socket.emit("signal", {
                to: targetSid,
                signal: {type: "ice", candidate: event.candidate}
            });
        }
    };

    pc.ondatachannel = function(event) {
        setupDataChannel(event.channel, targetSid);
    };

    pc.onconnectionstatechange = function() {
        log("Conn state [" + targetSid.slice(0,6) + "]: " + pc.connectionState);
    };

    if (isInitiator) {
        var channel = pc.createDataChannel("pairme", {ordered: true});
        setupDataChannel(channel, targetSid);
        pc.dataChannel = channel;
    }

    return pc;
}

function setupDataChannel(channel, targetSid) {
    channel.onopen = function() {
        log("Channel open: " + targetSid.slice(0,6));
    };
    channel.onmessage = function(event) {
        log("Data from [" + targetSid.slice(0,6) + "], type: " + (typeof event.data));
        handleDataMessage(event.data, targetSid);
    };
    channel.onerror = function(err) {
        log("Channel error: " + err);
    };
    channel.onclose = function() {
        log("Channel closed: " + targetSid.slice(0,6));
    };
}

function handleSignal(data) {
    var fromSid = data.from;
    var signal = data.signal;
    var pc = getOrCreateConnection(fromSid, false);

    if (signal.type === "offer") {
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .then(function() { return pc.createAnswer(); })
            .then(function(answer) { return pc.setLocalDescription(answer); })
            .then(function() {
                socket.emit("signal", {
                    to: fromSid,
                    signal: {type: "answer", sdp: pc.localDescription}
                });
            })
            .catch(function(err) { log("Signal err [" + fromSid.slice(0,6) + "]: " + err.message); });
    } else if (signal.type === "answer") {
        pc.setRemoteDescription(new RTCSessionDescription(signal.sdp))
            .catch(function(err) { log("Answer err [" + fromSid.slice(0,6) + "]: " + err.message); });
    } else if (signal.type === "ice") {
        pc.addIceCandidate(new RTCIceCandidate(signal.candidate))
            .catch(function(err) { log("ICE err [" + fromSid.slice(0,6) + "]: " + err.message); });
    }
}

function connectPeer(targetSid) {
    if (!checkWebRTC()) return;
    var pc = getOrCreateConnection(targetSid, true);
    pc.createOffer()
        .then(function(offer) { return pc.setLocalDescription(offer); })
        .then(function() {
            socket.emit("signal", {
                to: targetSid,
                signal: {type: "offer", sdp: pc.localDescription}
            });
            log("Offer -> " + targetSid.slice(0,6));
        })
        .catch(function(err) { log("Offer fail [" + targetSid.slice(0,6) + "]: " + err.message); });
}

function sendText() {
    var targetSid = document.getElementById("text-peer-select").value;
    var text = document.getElementById("text-input").value.trim();
    if (!targetSid || !text) { alert("Select peer and enter text"); return; }
    sendTextTo(targetSid, text);
}

function sendTextAll() {
    var text = document.getElementById("text-input").value.trim();
    if (!text) { alert("Enter text"); return; }
    if (peerList.length === 0) { alert("No peers"); return; }
    for (var i = 0; i < peerList.length; i++) {
        sendTextTo(peerList[i].sid, text);
    }
}

function sendTextTo(targetSid, text) {
    var pc = connections[targetSid];
    if (pc && pc.dataChannel && pc.dataChannel.readyState === "open") {
        doSendText(targetSid, text);
        return;
    }
    log("P2P unavailable, relay text -> " + targetSid.slice(0,6));
    socket.emit("relay_text", {to: targetSid, text: text});
    document.getElementById("text-input").value = "";
}

function doSendText(targetSid, text) {
    var pc = connections[targetSid];
    if (!pc || !pc.dataChannel) {
        log("No PC/channel for text [" + targetSid.slice(0,6) + "]");
        return;
    }
    if (pc.dataChannel.readyState !== "open") {
        log("Channel state: " + pc.dataChannel.readyState + " [" + targetSid.slice(0,6) + "]");
        return;
    }
    pc.dataChannel.send(JSON.stringify({t: "txt", c: text}));
    log("Text sent -> " + targetSid.slice(0,6));
    document.getElementById("text-input").value = "";
}

function sendFile() {
    var targetSid = document.getElementById("file-peer-select").value;
    var input = document.getElementById("file-input");
    if (!targetSid || !input.files || input.files.length === 0) {
        alert("Select peer and file"); return;
    }
    for (var i = 0; i < input.files.length; i++) {
        sendFileTo(targetSid, input.files[i]);
    }
}

function sendFileAll() {
    var input = document.getElementById("file-input");
    if (!input.files || input.files.length === 0) { alert("Select file"); return; }
    if (peerList.length === 0) { alert("No peers"); return; }
    for (var p = 0; p < peerList.length; p++) {
        for (var i = 0; i < input.files.length; i++) {
            sendFileTo(peerList[p].sid, input.files[i]);
        }
    }
}

function sendFileTo(targetSid, file) {
    var pc = connections[targetSid];
    if (pc && pc.dataChannel && pc.dataChannel.readyState === "open") {
        requestFileSend(targetSid, file);
        return;
    }
    log("P2P unavailable, relay file -> " + targetSid.slice(0,6));
    serverRelaySendFile(targetSid, file);
}

function requestFileSend(targetSid, file) {
    if (!pendingFileQueue[targetSid]) pendingFileQueue[targetSid] = [];
    pendingFileQueue[targetSid].push({file: file});
    if (pendingFileQueue[targetSid].length > 1) {
        log("Queued file [" + targetSid.slice(0,6) + "], remaining: " + pendingFileQueue[targetSid].length);
        return;
    }
    doRequestFileSend(targetSid);
}

function doRequestFileSend(targetSid) {
    var item = pendingFileQueue[targetSid][0];
    if (!item) return;
    socket.emit("broadcast_request", {
        to: targetSid,
        file_name: item.file.name,
        file_size: item.file.size,
        file_type: item.file.type
    });
    document.getElementById("send-status").textContent = "Waiting...";
}

function startDataTransfer(targetSid) {
    var queue = pendingFileQueue[targetSid];
    if (!queue || queue.length === 0) return;
    var item = queue[0];
    var file = item.file;
    var pc = connections[targetSid];
    if (!pc || !pc.dataChannel) { log("No channel"); nextFile(targetSid); return; }
    if (pc.dataChannel.readyState !== "open") {
        log("Channel not open: " + pc.dataChannel.readyState);
        setTimeout(function() { startDataTransfer(targetSid); }, 1000);
        return;
    }
    var channel = pc.dataChannel;
    var reader = new FileReader();
    reader.onload = function(e) {
        var buffer = e.target.result;
        var totalChunks = Math.ceil(buffer.byteLength / CHUNK_SIZE);
        var chunkIndex = 0;
        channel.send(JSON.stringify({t: "fs", n: file.name, s: file.size, m: file.type}));
        document.getElementById("send-progress-wrap").classList.remove("hidden");
        function sendNextChunk() {
            if (chunkIndex >= totalChunks) {
                channel.send(JSON.stringify({t: "fe"}));
                document.getElementById("send-status").textContent = "Sent: " + file.name;
                document.getElementById("send-progress").style.width = "100%";
                setTimeout(function() {
                    document.getElementById("send-progress-wrap").classList.add("hidden");
                    document.getElementById("send-progress").style.width = "0%";
                    nextFile(targetSid);
                }, 500);
                return;
            }
            if (channel.bufferedAmount > CHUNK_SIZE * 8) {
                setTimeout(sendNextChunk, 50); return;
            }
            var start = chunkIndex * CHUNK_SIZE;
            var end = Math.min(start + CHUNK_SIZE, buffer.byteLength);
            channel.send(buffer.slice(start, end));
            chunkIndex++;
            var pct = Math.round((chunkIndex / totalChunks) * 100);
            document.getElementById("send-progress").style.width = pct + "%";
            document.getElementById("send-status").textContent = "Sending... " + pct + "%";
            setTimeout(sendNextChunk, 0);
        }
        sendNextChunk();
    };
    reader.readAsArrayBuffer(file);
}

function handleDataMessage(data, fromSid) {
    if (typeof data === "string") {
        try {
            var msg = JSON.parse(data);
            if (msg.t === "txt") {
                addReceived("text", msg.c, fromSid);
                log("Text from " + fromSid.slice(0,6));
            } else if (msg.t === "fs") {
                var pc = connections[fromSid];
                if (pc) {
                    pc.receiveBuffer = [];
                    pc.receivedSize = 0;
                    pc.expectedSize = msg.s;
                    pc.receiveFileName = msg.n;
                    pc.receiveFileType = msg.m;
                }
            } else if (msg.t === "fe") {
                var pc2 = connections[fromSid];
                if (pc2) {
                    var blob = new Blob(pc2.receiveBuffer);
                    var url = URL.createObjectURL(blob);
                    addReceived("file", {name: pc2.receiveFileName, size: pc2.expectedSize, type: pc2.receiveFileType, url: url}, fromSid);
                    pc2.receiveBuffer = [];
                    pc2.receivedSize = 0;
                    log("File done: " + pc2.receiveFileName);
                }
            }
        } catch (e) { log("Bad msg"); }
    } else if (data instanceof ArrayBuffer) {
        var pc3 = connections[fromSid];
        if (pc3) {
            pc3.receiveBuffer.push(data);
            pc3.receivedSize += data.byteLength;
        }
    }
}

function serverRelaySendFile(targetSid, file) {
    if (!pendingFileQueue[targetSid]) pendingFileQueue[targetSid] = [];
    pendingFileQueue[targetSid].push({file: file, relay: true});
    if (pendingFileQueue[targetSid].length > 1) {
        log("Queued relay [" + targetSid.slice(0,6) + "], remaining: " + pendingFileQueue[targetSid].length);
        return;
    }
    doServerRelaySendFile(targetSid);
}

function doServerRelaySendFile(targetSid) {
    var queue = pendingFileQueue[targetSid];
    if (!queue || queue.length === 0) return;
    var item = queue[0];
    var file = item.file;
    socket.emit("relay_file_start", {
        to: targetSid,
        file_name: file.name,
        file_size: file.size,
        file_type: file.type
    });
    var reader = new FileReader();
    reader.onload = function(e) {
        var buffer = e.target.result;
        var totalChunks = Math.ceil(buffer.byteLength / CHUNK_SIZE);
        var chunkIndex = 0;
        document.getElementById("send-progress-wrap").classList.remove("hidden");
        function sendNextChunk() {
            if (chunkIndex >= totalChunks) {
                socket.emit("relay_file_done", {to: targetSid});
                document.getElementById("send-status").textContent = "Sent (relay): " + file.name;
                document.getElementById("send-progress").style.width = "100%";
                setTimeout(function() {
                    document.getElementById("send-progress-wrap").classList.add("hidden");
                    document.getElementById("send-progress").style.width = "0%";
                    nextFile(targetSid);
                }, 500);
                return;
            }
            var start = chunkIndex * CHUNK_SIZE;
            var end = Math.min(start + CHUNK_SIZE, buffer.byteLength);
            var chunk = buffer.slice(start, end);
            var bytes = new Uint8Array(chunk);
            var binary = "";
            for (var i = 0; i < bytes.length; i++) {
                binary += String.fromCharCode(bytes[i]);
            }
            socket.emit("relay_file_chunk", {to: targetSid, chunk: btoa(binary)});
            chunkIndex++;
            var pct = Math.round((chunkIndex / totalChunks) * 100);
            document.getElementById("send-progress").style.width = pct + "%";
            document.getElementById("send-status").textContent = "Relaying... " + pct + "%";
            setTimeout(sendNextChunk, 10);
        }
        sendNextChunk();
    };
    reader.readAsArrayBuffer(file);
}

function nextFile(targetSid) {
    if (!pendingFileQueue[targetSid]) return;
    pendingFileQueue[targetSid].shift();
    if (pendingFileQueue[targetSid].length > 0) {
        log("Next file: " + pendingFileQueue[targetSid].length + " remaining");
        var item = pendingFileQueue[targetSid][0];
        if (item.relay) {
            doServerRelaySendFile(targetSid);
        } else {
            doRequestFileSend(targetSid);
        }
    } else {
        delete pendingFileQueue[targetSid];
    }
}

function addReceived(type, content, fromSid) {
    var list = document.getElementById("received-files");
    var li = document.createElement("li");
    var peerName = "Unknown";
    for (var i = 0; i < peerList.length; i++) {
        if (peerList[i].sid === fromSid) { peerName = peerList[i].name; break; }
    }
    var time = new Date().toLocaleTimeString();
    
    var header = document.createElement("div");
    header.innerHTML = '<b>' + escapeHtml(peerName) + '</b> @ ' + time;
    li.appendChild(header);
    
    if (type === "text") {
        var textWrap = document.createElement("div");
        textWrap.className = "text-msg";
        
        var trimmed = content.trim();
        var isSingleUrl = /^(https?:\/\/|www\.)[^\s]+$/i.test(trimmed);
        
        if (isSingleUrl) {
            var a = document.createElement("a");
            a.href = trimmed.indexOf("http") === 0 ? trimmed : "https://" + trimmed;
            a.target = "_blank";
            a.className = "msg-link";
            a.textContent = content;
            textWrap.appendChild(a);
        } else if (trimmed.indexOf("\n") !== -1 || /^[ \t]/.test(content) || /[{};<>]/.test(content)) {
            var pre = document.createElement("pre");
            pre.className = "msg-code";
            var code = document.createElement("code");
            code.textContent = content;
            pre.appendChild(code);
            textWrap.appendChild(pre);
        } else {
            var span = document.createElement("span");
            span.className = "msg-plain";
            span.innerHTML = escapeHtml(content).replace(/(https?:\/\/[^\s]+)/g, '<a href="$1" target="_blank" class="msg-link">$1</a>');
            textWrap.appendChild(span);
        }
        
        li.appendChild(textWrap);
        
        var copyBtn = document.createElement("button");
        copyBtn.className = "copy-btn small secondary";
        copyBtn.textContent = "Copy";
        copyBtn.onclick = function() {
            var ta = document.createElement("textarea");
            ta.value = content;
            document.body.appendChild(ta);
            ta.select();
            document.execCommand("copy");
            document.body.removeChild(ta);
            copyBtn.textContent = "Copied";
            setTimeout(function() { copyBtn.textContent = "Copy"; }, 1500);
        };
        li.appendChild(copyBtn);
    } else {
        var meta = document.createElement("div");
        meta.className = "file-meta";
        meta.textContent = content.name + " (" + formatBytes(content.size) + ")";
        li.appendChild(meta);
        
        var wrap = document.createElement("div");
        wrap.className = "preview-wrap";
        var mime = content.type || "";
        
        if (mime.indexOf("image/") === 0) {
            var img = document.createElement("img");
            img.src = content.url;
            img.className = "preview-img";
            img.alt = content.name;
            wrap.appendChild(img);
        } else if (mime.indexOf("video/") === 0) {
            var vid = document.createElement("video");
            vid.src = content.url;
            vid.controls = true;
            vid.className = "preview-video";
            wrap.appendChild(vid);
        } else if (mime.indexOf("audio/") === 0) {
            var aud = document.createElement("audio");
            aud.src = content.url;
            aud.controls = true;
            aud.className = "preview-audio";
            wrap.appendChild(aud);
        } else if (mime === "application/pdf" || mime.indexOf("pdf") !== -1) {
            var iframe = document.createElement("iframe");
            iframe.src = content.url;
            iframe.className = "preview-pdf";
            wrap.appendChild(iframe);
        } else if (mime.indexOf("text/") === 0) {
            var pre = document.createElement("pre");
            pre.className = "preview-text";
            pre.textContent = "Loading...";
            wrap.appendChild(pre);
            fetch(content.url).then(function(r) { return r.text(); }).then(function(t) {
                pre.textContent = t.substring(0, 5000);
            }).catch(function() { pre.textContent = "Cannot load text"; });
        }
        
        li.appendChild(wrap);
        
        var link = document.createElement("a");
        link.href = content.url;
        link.download = content.name;
        link.className = "btn small secondary";
        link.textContent = "Download";
        link.style.marginTop = "6px";
        li.appendChild(link);
    }
    list.insertBefore(li, list.firstChild);
}

function respondRequest(accepted) {
    document.getElementById("request-modal").style.display = "none";
    if (pendingRequest) {
        socket.emit("broadcast_response", {
            to: pendingRequest.from,
            accepted: accepted
        });
        log(accepted ? "Accepted" : "Declined");
        pendingRequest = null;
    }
}
function copyAllReceived() {
    var items = document.getElementById("received-files").querySelectorAll("li");
    var allText = "";
    for (var i = items.length - 1; i >= 0; i--) {
        var txt = items[i].querySelector(".text-msg");
        if (txt) {
            var code = txt.querySelector("code");
            var span = txt.querySelector(".msg-plain");
            var link = txt.querySelector(".msg-link");
            if (code) allText += code.textContent + "\n\n";
            else if (span) allText += span.textContent + "\n\n";
            else if (link) allText += link.textContent + "\n\n";
        }
    }
    if (!allText) { alert("No text to copy"); return; }
    var ta = document.createElement("textarea");
    ta.value = allText.trim();
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    document.body.removeChild(ta);
    var btn = document.getElementById("copy-all-btn");
    if (btn) {
        btn.textContent = "Copied All";
        setTimeout(function() { btn.textContent = "Copy All"; }, 1500);
    }
}

function injectCopyAll() {
    var cards = document.querySelectorAll(".card");
    for (var i = 0; i < cards.length; i++) {
        var h2 = cards[i].querySelector("h2");
        if (h2 && h2.textContent.trim() === "Received") {
            var btn = document.createElement("button");
            btn.id = "copy-all-btn";
            btn.className = "copy-all-btn secondary small";
            btn.textContent = "Copy All";
            btn.onclick = copyAllReceived;
            h2.appendChild(btn);
            h2.style.display = "flex";
            h2.style.justifyContent = "space-between";
            h2.style.alignItems = "center";
            break;
        }
    }
}

window.onload = function() {
    loadName();
    injectCopyAll();
    if (checkWebRTC()) {
        initSocket();
    }
};