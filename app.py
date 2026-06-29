"""
RabbitMQ Stream Queue Visualizer
Read-only viewer — never acknowledges, deletes, or modifies messages.

- Classic / quorum queues: RabbitMQ HTTP Management API with requeue=true.
- Stream queues: direct AMQP 0-9-1 connection, consumer with
  x-stream-offset=first and no-ack=true (messages never deleted).
"""

import base64
import json
import socket
import struct
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer


# ── Management API helpers ────────────────────────────────────────────────────

def _basic_auth(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def _api_get(host, mgmt_port, path, user, password):
    url = f"http://{host}:{mgmt_port}/api/{path}"
    req = urllib.request.Request(url, headers={"Authorization": _basic_auth(user, password)})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def get_queue_type(host, mgmt_port, vhost, queue, user, password) -> str:
    """Return the queue type string ('classic', 'quorum', 'stream') or 'classic' on error."""
    try:
        path = f"queues/{urllib.parse.quote(vhost, safe='')}/{urllib.parse.quote(queue, safe='')}"
        info = _api_get(host, mgmt_port, path, user, password)
        return info.get("type", "classic")
    except Exception:
        return "classic"


def peek_messages(host, mgmt_port, vhost, queue, user, password, count=500):
    """
    POST /api/queues/{vhost}/{queue}/get with ackmode=ack_requeue_true.
    Messages are immediately requeued — nothing is ever removed.
    Only works for classic and quorum queues.
    """
    url = (f"http://{host}:{mgmt_port}/api/queues"
           f"/{urllib.parse.quote(vhost, safe='')}"
           f"/{urllib.parse.quote(queue, safe='')}/get")
    payload = json.dumps({
        "count": count,
        "ackmode": "ack_requeue_true",  # peek only — messages stay in the queue
        "encoding": "auto",
        "truncate": 50000,
    }).encode()
    req = urllib.request.Request(
        url, data=payload,
        headers={
            "Authorization": _basic_auth(user, password),
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def list_queues(host, mgmt_port, user, password):
    return _api_get(host, mgmt_port, "queues", user, password)


# ── Minimal AMQP 0-9-1 client (stream queues) ────────────────────────────────

def _enc_short_str(s: str) -> bytes:
    b = s.encode()
    return bytes([len(b)]) + b


def _enc_long_str(s) -> bytes:
    b = s.encode() if isinstance(s, str) else bytes(s)
    return struct.pack(">I", len(b)) + b


def _enc_table(d: dict) -> bytes:
    buf = b""
    for k, v in d.items():
        buf += _enc_short_str(k)
        if isinstance(v, bool):
            buf += b"t" + bytes([int(v)])
        elif isinstance(v, int):
            buf += b"I" + struct.pack(">i", v)
        elif isinstance(v, str):
            buf += b"S" + _enc_long_str(v)
        elif v is None:
            buf += b"V"
    return struct.pack(">I", len(buf)) + buf


def _method_frame(channel: int, class_id: int, method_id: int, payload: bytes) -> bytes:
    body = struct.pack(">HH", class_id, method_id) + payload
    return struct.pack(">BHI", 1, channel, len(body)) + body + b"\xCE"


class _R:
    """Cursor reader over raw bytes."""
    def __init__(self, data: bytes, pos: int = 0):
        self.d = data
        self.p = pos

    def u8(self):
        v = self.d[self.p]; self.p += 1; return v
    def u16(self):
        v = struct.unpack_from(">H", self.d, self.p)[0]; self.p += 2; return v
    def u32(self):
        v = struct.unpack_from(">I", self.d, self.p)[0]; self.p += 4; return v
    def u64(self):
        v = struct.unpack_from(">Q", self.d, self.p)[0]; self.p += 8; return v
    def i16(self):
        v = struct.unpack_from(">h", self.d, self.p)[0]; self.p += 2; return v
    def i32(self):
        v = struct.unpack_from(">i", self.d, self.p)[0]; self.p += 4; return v
    def f32(self):
        v = struct.unpack_from(">f", self.d, self.p)[0]; self.p += 4; return v
    def f64(self):
        v = struct.unpack_from(">d", self.d, self.p)[0]; self.p += 8; return v

    def short_str(self) -> str:
        n = self.u8()
        s = self.d[self.p:self.p + n].decode(errors="replace"); self.p += n; return s

    def long_str(self) -> bytes:
        n = self.u32()
        s = self.d[self.p:self.p + n]; self.p += n; return s

    def table(self) -> dict:
        n = self.u32()
        end = self.p + n
        result = {}
        while self.p < end:
            try:
                k = self.short_str()
                t = chr(self.u8())
                if   t == "t": v = bool(self.u8())
                elif t == "b": v = self.u8()
                elif t == "B": v = self.u8()
                elif t == "U": v = self.i16()
                elif t == "u": v = self.u16()
                elif t == "I": v = self.i32()
                elif t == "i": v = self.u32()
                elif t == "L": v = struct.unpack_from(">q", self.d, self.p)[0]; self.p += 8
                elif t == "l": v = self.u64()
                elif t == "f": v = self.f32()
                elif t == "d": v = self.f64()
                elif t == "S": v = self.long_str().decode(errors="replace")
                elif t == "s": v = self.short_str()
                elif t == "F": v = self.table()
                elif t == "A": n2 = self.u32(); v = repr(self.d[self.p:self.p + n2]); self.p += n2
                elif t == "T": v = self.u64()
                elif t == "x": n2 = self.u32(); v = self.d[self.p:self.p + n2].hex(); self.p += n2
                elif t == "V": v = None
                else: break
                result[k] = v
            except Exception:
                break
        self.p = end
        return result


class AmqpStreamReader:
    """
    Minimal read-only AMQP 0-9-1 client for RabbitMQ stream queues.
    Consumes with x-stream-offset=first and no-ack=true:
      - messages are never acknowledged
      - stream queue messages are immutable — nothing is deleted or modified
    """

    F_METHOD = 1
    F_HEADER = 2
    F_BODY   = 3
    F_HEART  = 8

    def __init__(self, host: str, port: int, vhost: str, user: str, password: str):
        self._sock = socket.create_connection((host, port), timeout=15)
        self._sock.settimeout(15)

        # Protocol header
        self._sock.sendall(b"AMQP\x00\x00\x09\x01")

        # ← Connection.Start (10.10)
        self._read_frame()

        # → Connection.StartOk (10.11)
        client_props = _enc_table({"product": "RabbitMQ Viewer", "version": "1.0"})
        mechanism    = _enc_short_str("PLAIN")
        response     = _enc_long_str(b"\x00" + user.encode() + b"\x00" + password.encode())
        locale       = _enc_short_str("en_US")
        self._send(0, 10, 11, client_props + mechanism + response + locale)

        # ← Connection.Tune (10.30)
        _, _, payload = self._read_frame()
        r = _R(payload, 4)          # skip class-id + method-id
        r.u16()                     # channel-max (ignored)
        frame_max = r.u32()
        r.u16()                     # heartbeat (ignored)
        self._frame_max = frame_max if frame_max else 131072

        # → Connection.TuneOk (10.31)  — heartbeat disabled
        self._send(0, 10, 31, struct.pack(">HIH", 0, self._frame_max, 0))

        # → Connection.Open (10.40)
        self._send(0, 10, 40, _enc_short_str(vhost) + b"\x00\x00")
        # ← Connection.OpenOk (10.41)
        self._read_frame()

        # → Channel.Open (20.10)
        self._send(1, 20, 10, b"\x00")
        # ← Channel.OpenOk (20.11)
        self._read_frame()

    def read_messages(self, queue: str, count: int) -> list:
        tag = "viewer"

        # → Basic.Consume (60.20)
        #   flags byte: bit0=no-local=0, bit1=no-ack=1, bit2=exclusive=0, bit3=nowait=0
        self._send(1, 60, 20,
            struct.pack(">H", 0)            # reserved
            + _enc_short_str(queue)
            + _enc_short_str(tag)
            + bytes([0b00000010])           # no-ack = True
            + _enc_table({"x-stream-offset": "first"})
        )
        # ← Basic.ConsumeOk (60.21)
        self._read_frame()

        messages = []
        first_msg = True
        try:
            while len(messages) < count:
                if not first_msg:
                    self._sock.settimeout(3)   # short wait once messages start flowing
                ftype, _, payload = self._read_frame()
                if ftype == self.F_HEART:
                    self._pong(); continue
                if ftype != self.F_METHOD:
                    continue
                cls = struct.unpack_from(">H", payload, 0)[0]
                mth = struct.unpack_from(">H", payload, 2)[0]
                if cls == 60 and mth == 60:    # Basic.Deliver
                    first_msg = False
                    messages.append(self._recv_message(payload[4:]))
        except (socket.timeout, OSError):
            pass

        # → Basic.Cancel (60.30)
        try:
            self._sock.settimeout(5)
            self._send(1, 60, 30, _enc_short_str(tag) + b"\x00")
            self._read_frame()  # Basic.CancelOk
        except Exception:
            pass

        return messages

    def close(self):
        try:
            self._sock.settimeout(3)
            # Channel.Close (20.40)
            self._send(1, 20, 40,
                struct.pack(">H", 200) + _enc_short_str("bye") + struct.pack(">HH", 0, 0))
            self._read_frame()
            # Connection.Close (10.50)
            self._send(0, 10, 50,
                struct.pack(">H", 200) + _enc_short_str("bye") + struct.pack(">HH", 0, 0))
            self._read_frame()
        except Exception:
            pass
        finally:
            try: self._sock.close()
            except Exception: pass

    # ── internals ────────────────────────────────────────────────────────────

    def _recv_message(self, deliver_data: bytes) -> dict:
        r = _R(deliver_data)
        r.short_str()           # consumer-tag
        r.u64()                 # delivery-tag
        redelivered = bool(r.u8() & 1)
        exchange    = r.short_str()
        routing_key = r.short_str()

        # Content Header frame
        _, _, hpay = self._read_frame()
        r2 = _R(hpay)
        r2.u16()                # class-id
        r2.u16()                # weight (always 0)
        body_size = r2.u64()
        flags = r2.u16()

        props, headers = {}, {}
        try:
            if flags & 0x8000: props["content_type"]     = r2.short_str()
            if flags & 0x4000: props["content_encoding"] = r2.short_str()
            if flags & 0x2000: headers                   = r2.table()
            if flags & 0x1000: props["delivery_mode"]    = r2.u8()
            if flags & 0x0800: props["priority"]         = r2.u8()
            if flags & 0x0400: props["correlation_id"]   = r2.short_str()
            if flags & 0x0200: props["reply_to"]         = r2.short_str()
            if flags & 0x0100: props["expiration"]       = r2.short_str()
            if flags & 0x0080: props["message_id"]       = r2.short_str()
            if flags & 0x0040: props["timestamp"]        = r2.u64()
            if flags & 0x0020: props["type"]             = r2.short_str()
            if flags & 0x0010: props["user_id"]          = r2.short_str()
            if flags & 0x0008: props["app_id"]           = r2.short_str()
        except Exception:
            pass

        # Body frames
        body = b""
        while len(body) < body_size:
            ftype, _, bpay = self._read_frame()
            if ftype == self.F_BODY:  body += bpay
            elif ftype == self.F_HEART: self._pong()

        return {
            "exchange": exchange,
            "routing_key": routing_key,
            "redelivered": redelivered,
            "payload": body.decode(errors="replace"),
            "payload_encoding": "string",
            "properties": {**props, "headers": headers},
        }

    def _pong(self):
        self._sock.sendall(struct.pack(">BHI", 8, 0, 0) + b"\xCE")

    def _send(self, ch, cls, mth, payload):
        self._sock.sendall(_method_frame(ch, cls, mth, payload))

    def _read_frame(self):
        raw = self._recv_n(7)
        ftype, channel, size = struct.unpack(">BHI", raw)
        payload = self._recv_n(size)
        end = self._recv_n(1)
        if end[0] != 0xCE:
            raise ValueError(f"bad frame-end byte: {end!r}")
        return ftype, channel, payload

    def _recv_n(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise EOFError("connection closed")
            buf += chunk
        return buf


# ── HTML page ─────────────────────────────────────────────────────────────────

HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>RabbitMQ Stream Viewer</title>
<style>
  :root{--bg:#0f1117;--card:#1a1d27;--border:#2d3148;--accent:#7c6af7;--accent2:#5ee7d0;--text:#e2e8f0;--muted:#8892a4;--error:#f87171;--ok:#4ade80}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh;padding:2rem}
  h1{font-size:1.6rem;font-weight:700;background:linear-gradient(90deg,var(--accent),var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:1.5rem}
  .card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:1.5rem;margin-bottom:1.5rem}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:.75rem}
  label{font-size:.8rem;color:var(--muted);display:block;margin-bottom:.3rem}
  input,select{width:100%;padding:.5rem .75rem;border-radius:8px;border:1px solid var(--border);background:#0f1117;color:var(--text);font-size:.9rem;outline:none;transition:border-color .2s}
  input:focus,select:focus{border-color:var(--accent)}
  .btn{padding:.55rem 1.4rem;border-radius:8px;border:none;cursor:pointer;font-size:.9rem;font-weight:600;transition:opacity .2s}
  .btn-primary{background:var(--accent);color:#fff}
  .btn-secondary{background:var(--border);color:var(--text)}
  .btn:hover{opacity:.85}
  .row{display:flex;gap:.75rem;align-items:flex-end;flex-wrap:wrap}
  .badge{display:inline-block;padding:.15rem .55rem;border-radius:99px;font-size:.72rem;font-weight:600}
  .badge-ok{background:#14532d;color:var(--ok)}
  .badge-err{background:#450a0a;color:var(--error)}
  .badge-stream{background:#1e1b4b;color:var(--accent)}
  #status{font-size:.85rem;margin-top:.75rem;min-height:1.2rem;color:var(--muted)}
  #status.err{color:var(--error)}
  table{width:100%;border-collapse:collapse;font-size:.82rem}
  thead tr{background:#12152080}
  th{text-align:left;padding:.6rem .75rem;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border)}
  td{padding:.55rem .75rem;border-bottom:1px solid #1e2235;vertical-align:top;word-break:break-all}
  tr:hover td{background:#1f2337}
  .payload{max-height:120px;overflow:auto;white-space:pre-wrap;font-family:monospace;font-size:.78rem;background:#0d1020;padding:.4rem .6rem;border-radius:6px;color:var(--accent2)}
  .toolbar{display:flex;gap:.75rem;align-items:center;flex-wrap:wrap;margin-bottom:1rem}
  #search{flex:1;min-width:200px}
  #count-badge{color:var(--muted);font-size:.82rem}
  .empty{text-align:center;padding:3rem;color:var(--muted)}
  .spinner{display:inline-block;width:14px;height:14px;border:2px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;margin-right:.4rem}
  @keyframes spin{to{transform:rotate(360deg)}}
  .prop-key{color:var(--muted);font-size:.75rem}
  .prop-val{font-size:.8rem}
  details summary{cursor:pointer;color:var(--accent);font-size:.78rem;user-select:none}
  .readonly-notice{background:#1e1b4b;border:1px solid #3730a3;border-radius:8px;padding:.6rem 1rem;font-size:.8rem;color:#a5b4fc;margin-bottom:1rem}
</style>
</head>
<body>
<h1>RabbitMQ Stream Viewer</h1>

<div class="readonly-notice">
  🔒 <strong>Read-only mode</strong> — classic/quorum queues use <code>ackmode=ack_requeue_true</code>;
  stream queues use a direct AMQP consumer with <code>no-ack=true</code> and <code>x-stream-offset=first</code>.
  Nothing is ever deleted or modified.
</div>

<div class="card">
  <div class="grid">
    <div><label>RabbitMQ Host</label><input id="host" value="localhost" placeholder="e.g. 192.168.1.10"/></div>
    <div><label>AMQP Port</label><input id="port" value="5672" placeholder="5672"/></div>
    <div><label>Management Port</label><input id="mgmt" value="15672" placeholder="15672"/></div>
    <div><label>Virtual Host</label><input id="vhost" value="/" placeholder="/"/></div>
    <div><label>Username</label><input id="user" value="guest"/></div>
    <div><label>Password</label><input id="pass" type="password" value="guest"/></div>
    <div><label>Queue Name</label><input id="queue" placeholder="my-stream-queue"/></div>
    <div><label>Max Messages</label><input id="maxmsg" type="number" value="200" min="1" max="5000"/></div>
  </div>
  <div class="row" style="margin-top:1rem">
    <button class="btn btn-primary" onclick="loadMessages()">Load Messages</button>
    <button class="btn btn-secondary" onclick="loadQueues()">Browse Queues</button>
    <label style="display:flex;align-items:center;gap:.4rem;cursor:pointer;color:var(--muted);font-size:.85rem;margin:0">
      <input type="checkbox" id="autorefresh" onchange="toggleAuto()"/> Auto-refresh
    </label>
    <select id="interval" style="width:auto;padding:.4rem">
      <option value="5000">5 s</option>
      <option value="10000" selected>10 s</option>
      <option value="30000">30 s</option>
      <option value="60000">60 s</option>
    </select>
  </div>
  <div id="status"></div>
</div>

<div id="queue-list" style="display:none" class="card">
  <h2 style="font-size:1rem;margin-bottom:1rem">Available Queues</h2>
  <div id="queue-table"></div>
</div>

<div id="msg-section" style="display:none" class="card">
  <div class="toolbar">
    <span id="count-badge"></span>
    <input id="search" placeholder="Filter messages…" oninput="filterTable()"/>
    <button class="btn btn-secondary" onclick="exportJSON()">Export JSON</button>
  </div>
  <div id="msg-table"></div>
</div>

<script>
let _allMessages = [];
let _autoTimer = null;

function setStatus(html, isErr=false){
  const el = document.getElementById('status');
  el.innerHTML = html;
  el.className = isErr ? 'err' : '';
}

function params(){
  return {
    host: document.getElementById('host').value.trim(),
    port: document.getElementById('port').value.trim(),
    mgmt: document.getElementById('mgmt').value.trim(),
    vhost: document.getElementById('vhost').value.trim() || '/',
    user: document.getElementById('user').value.trim(),
    pass: document.getElementById('pass').value,
    queue: document.getElementById('queue').value.trim(),
    maxmsg: parseInt(document.getElementById('maxmsg').value)||200,
  };
}

async function loadMessages(){
  const p = params();
  if(!p.queue){setStatus('Enter a queue name.',true);return;}
  setStatus('<span class="spinner"></span> Loading…');
  try{
    const qs = new URLSearchParams({host:p.host,port:p.port,mgmt:p.mgmt,vhost:p.vhost,user:p.user,pass:p.pass,queue:p.queue,count:p.maxmsg});
    const r = await fetch('/api/messages?'+qs);
    const d = await r.json();
    if(d.error){setStatus(d.error,true);return;}
    _allMessages = d.messages||[];
    renderMessages(_allMessages);
    const via = d.via ? ` <span style="color:var(--muted)">(via ${d.via})</span>` : '';
    setStatus(`Loaded ${_allMessages.length} message(s)${via} — ${new Date().toLocaleTimeString()}`);
  }catch(e){setStatus('Request failed: '+e.message,true);}
}

async function loadQueues(){
  const p = params();
  setStatus('<span class="spinner"></span> Fetching queue list…');
  try{
    const qs = new URLSearchParams({host:p.host,mgmt:p.mgmt,user:p.user,pass:p.pass});
    const r = await fetch('/api/queues?'+qs);
    const d = await r.json();
    if(d.error){setStatus(d.error,true);return;}
    renderQueueList(d.queues||[]);
    setStatus('');
  }catch(e){setStatus('Request failed: '+e.message,true);}
}

function renderQueueList(queues){
  const sec = document.getElementById('queue-list');
  sec.style.display='block';
  const rows = queues.map(q=>{
    const type = q.type||'classic';
    const badge = type==='stream'?'<span class="badge badge-stream">stream</span>':'<span class="badge">'+type+'</span>';
    return `<tr>
      <td><a href="#" onclick="selectQueue('${escHtml(q.name)}');return false" style="color:var(--accent)">${escHtml(q.name)}</a></td>
      <td>${badge}</td>
      <td>${(q.messages||0).toLocaleString()}</td>
      <td>${escHtml(q.vhost)}</td>
      <td><span class="badge ${q.state==='running'?'badge-ok':'badge-err'}">${q.state||'?'}</span></td>
    </tr>`;
  }).join('');
  document.getElementById('queue-table').innerHTML=`<table>
    <thead><tr><th>Queue</th><th>Type</th><th>Messages</th><th>VHost</th><th>State</th></tr></thead>
    <tbody>${rows||'<tr><td colspan=5 class="empty">No queues found</td></tr>'}</tbody>
  </table>`;
}

function selectQueue(name){
  document.getElementById('queue').value=name;
  document.getElementById('queue-list').style.display='none';
  loadMessages();
}

function renderMessages(msgs){
  const sec = document.getElementById('msg-section');
  sec.style.display='block';
  document.getElementById('count-badge').textContent = msgs.length+' message(s)';
  if(!msgs.length){
    document.getElementById('msg-table').innerHTML='<div class="empty">Queue is empty or no messages returned.</div>';
    return;
  }
  const rows = msgs.map((m,i)=>{
    const payload = formatPayload(m.payload, m.payload_encoding);
    const props = m.properties||{};
    const headers = props.headers ? JSON.stringify(props.headers,null,2) : '';
    const propsList = Object.entries(props).filter(([k])=>k!=='headers').map(([k,v])=>`<div><span class="prop-key">${escHtml(k)}: </span><span class="prop-val">${escHtml(String(v))}</span></div>`).join('');
    return `<tr>
      <td style="color:var(--muted);width:3rem">${i+1}</td>
      <td style="width:7rem">${escHtml(m.routing_key||'')}</td>
      <td style="width:6rem">${escHtml(m.exchange||'(default)')}</td>
      <td><div class="payload">${escHtml(payload)}</div></td>
      <td>
        ${propsList}
        ${headers?`<details><summary>Headers</summary><div class="payload">${escHtml(headers)}</div></details>`:''}
      </td>
    </tr>`;
  }).join('');
  document.getElementById('msg-table').innerHTML=`<table>
    <thead><tr><th>#</th><th>Routing Key</th><th>Exchange</th><th>Payload</th><th>Properties</th></tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

function formatPayload(payload, encoding){
  if(!payload) return '(empty)';
  if(encoding==='base64'){try{return atob(payload);}catch{return payload;}}
  try{return JSON.stringify(JSON.parse(payload),null,2);}catch{return payload;}
}

function filterTable(){
  const q = document.getElementById('search').value.toLowerCase();
  renderMessages(q ? _allMessages.filter(m=>JSON.stringify(m).toLowerCase().includes(q)) : _allMessages);
}

function exportJSON(){
  const blob = new Blob([JSON.stringify(_allMessages,null,2)],{type:'application/json'});
  const a = document.createElement('a');
  a.href=URL.createObjectURL(blob);
  a.download='rabbitmq-messages.json';
  a.click();
}

function toggleAuto(){
  if(_autoTimer){clearInterval(_autoTimer);_autoTimer=null;}
  if(document.getElementById('autorefresh').checked){
    const ms = parseInt(document.getElementById('interval').value)||10000;
    _autoTimer = setInterval(loadMessages, ms);
  }
}

function escHtml(s){
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
</script>
</body>
</html>
"""

# ── HTTP handler ──────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def send_json(self, data, code=200):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html: str):
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)

        def q(k, default=""):
            return qs.get(k, [default])[0]

        if parsed.path == "/":
            self.send_html(HTML_PAGE)

        elif parsed.path == "/api/messages":
            host     = q("host", "localhost")
            amqp_port = int(q("port", "5672"))
            mgmt_port = int(q("mgmt", "15672"))
            vhost    = q("vhost", "/")
            queue    = q("queue")
            user     = q("user", "guest")
            password = q("pass", "guest")
            count    = min(int(q("count", "200")), 5000)

            try:
                # Detect queue type so we choose the right reader
                qtype = get_queue_type(host, mgmt_port, vhost, queue, user, password)

                if qtype == "stream":
                    # Use direct AMQP connection for stream queues
                    try:
                        reader = AmqpStreamReader(host, amqp_port, vhost, user, password)
                    except ConnectionRefusedError:
                        self.send_json({"error": (
                            f"Cannot connect to AMQP port {amqp_port} on {host}. "
                            "Stream queues require a direct AMQP connection. "
                            "Check that port 5672 is open and reachable "
                            "(firewall, VPN, or Docker port mapping may be blocking it)."
                        )})
                        return
                    except OSError as e:
                        self.send_json({"error": f"AMQP connection failed: {e}"})
                        return
                    try:
                        msgs = reader.read_messages(queue, count)
                    finally:
                        reader.close()
                    self.send_json({"messages": msgs, "via": "AMQP (stream)"})
                else:
                    # Use management API for classic/quorum queues
                    msgs = peek_messages(host, mgmt_port, vhost, queue, user, password, count)
                    self.send_json({"messages": msgs, "via": "management API"})

            except urllib.error.HTTPError as e:
                body = e.read().decode(errors="replace")
                self.send_json({"error": f"HTTP {e.code}: {body}"})
            except Exception as e:
                self.send_json({"error": str(e)})

        elif parsed.path == "/api/queues":
            host      = q("host", "localhost")
            mgmt_port = int(q("mgmt", "15672"))
            user      = q("user", "guest")
            password  = q("pass", "guest")
            try:
                queues = list_queues(host, mgmt_port, user, password)
                self.send_json({"queues": queues})
            except urllib.error.HTTPError as e:
                body = e.read().decode(errors="replace")
                self.send_json({"error": f"HTTP {e.code}: {body}"})
            except Exception as e:
                self.send_json({"error": str(e)})

        else:
            self.send_response(404)
            self.end_headers()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    port = 8765
    server = HTTPServer(("127.0.0.1", port), Handler)
    url = f"http://127.0.0.1:{port}"
    print(f"RabbitMQ Stream Viewer  →  {url}")
    print("Press Ctrl-C to stop.")
    try:
        import webbrowser
        webbrowser.open(url)
    except Exception:
        pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
