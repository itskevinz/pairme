# PairMe

Fast, zero-config cross-device file transfer and real-time text sharing over WebRTC and Socket.IO.

PairMe enables instant peer-to-peer file transfers and direct message/code sharing between devices across local networks or over the internet using a lightweight browser UI and Python backend.

---

## Features

- **P2P Direct Transfer**: High-speed, local data transfer powered by WebRTC `RTCDataChannel`.
- **Relay Fallback**: Automatic WebSocket fallback via Socket.IO when direct P2P connections are restricted by complex network topographies or restrictive firewalls.
- **Room Isolation**: Isolate devices using custom 6-digit room codes or collaborate inside a public local lobby.
- **Rich Text & Code Sharing**: Formatted view for URLs, inline code snippets, and full code blocks with standard copy buttons and expand toggles.
- **File Previews**: Embedded image previews for instant inspection upon receipt.
- **Responsive Web UI**: Native mobile tab navigation and responsive sidebars for single-hand use on phones, tablets, or desktops.

---

## Tech Stack

* **Backend**: Python 3, Flask, Flask-SocketIO, Eventlet/Gevent
* **Frontend**: Vanilla JavaScript (ES6+), HTML5, CSS3
* **Protocols**: WebSockets, WebRTC (STUN via Google)

---

## Quick Start

### Prerequisites

- Python 3.8+
- `pip` package manager

### Installation

1. Clone the repository:
 ```bash
 git clone https://github.com/itskevinz/pairme.git
 cd pairme
 ```


2. Install dependencies:
```bash
pip install flask flask-socketio gevent gevent-websocket
```




3. Run the application:
```bash
python app.py
```




4. Open your browser and navigate to:

```text
http://localhost:5000
```





---

## Usage

1. **Connect Devices**: Open the app URL on two or more devices on the same local network (or enter the same custom room code on both devices).
2. **Select Target**: Choose a specific device from the **Nearby** list or leave it on **All Devices** to broadcast.
3. **Send Text or Files**:
* Paste text, URLs, or code blocks into the input area and hit **Send**.
* Drag and drop files onto the drop zone (or click to upload).


4. **Accept & Receive**: The target peer receives a prompt to accept incoming transfers. Files render directly with instant download links or embedded previews.

---

## License

Distributed under the [MIT License](https://www.google.com/search?q=LICENSE).
