"""
RabbitMQ Stream Queue Visualizer
Read-only viewer — never acknowledges, deletes, or modifies messages.
Uses the RabbitMQ HTTP Management API (port 15672 by default).
"""

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

# ── Management API helpers ────────────────────────────────────────────────────

def _basic_auth(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def _api_get(host: str, mgmt_port: int, path: str, user: str, password: str):
    url = f"http://{host}:{mgmt_port}/api/{path}"
    req = urllib.request.Request(url, headers={"Authorization": _basic_auth(user, password)})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def peek_messages(host: str, mgmt_port: int, vhost: str, queue: str,
                  user: str, password: str, count: int = 500):
    """
    POST /api/queues/{vhost}/{queue}/get  with  requeue=true
    This is the ONLY way to read messages via the management API.
    ackmode=ack_requeue_true guarantees messages are never removed from the queue.
    """
    url = f"http://{host}:{mgmt_port}/api/queues/{urllib.parse.quote(vhost, safe='')}/{urllib.parse.quote(queue, safe='')}/get"
    payload = json.dumps({
        "count": count,
        "ackmode": "ack_requeue_true",  # peek only — messages stay in the queue
        "encoding": "auto",
        "truncate": 50000,
    }).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": _basic_auth(user, password),
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def list_queues(host: str, mgmt_port: int, user: str, password: str):
    return _api_get(host, mgmt_port, "queues", user, password)


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
  🔒 <strong>Read-only mode</strong> — messages are peeked with <code>requeue=true</code>. Nothing is ever deleted or modified.
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

function setStatus(msg, isErr=false){
  const el = document.getElementById('status');
  el.textContent = msg;
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
  document.getElementById('status').innerHTML = '<span class="spinner"></span> Loading…';
  try{
    const qs = new URLSearchParams({host:p.host,mgmt:p.mgmt,vhost:p.vhost,user:p.user,pass:p.pass,queue:p.queue,count:p.maxmsg});
    const r = await fetch('/api/messages?'+qs);
    const d = await r.json();
    if(d.error){setStatus(d.error,true);return;}
    _allMessages = d.messages||[];
    renderMessages(_allMessages);
    setStatus(`Loaded ${_allMessages.length} message(s) — ${new Date().toLocaleTimeString()}`);
  }catch(e){setStatus('Request failed: '+e.message,true);}
}

async function loadQueues(){
  const p = params();
  setStatus('');
  document.getElementById('status').innerHTML = '<span class="spinner"></span> Fetching queue list…';
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
  if(encoding==='base64'){
    try{return atob(payload);}catch{return payload;}
  }
  try{return JSON.stringify(JSON.parse(payload),null,2);}catch{return payload;}
}

function filterTable(){
  const q = document.getElementById('search').value.toLowerCase();
  if(!q){renderMessages(_allMessages);return;}
  const filtered = _allMessages.filter(m=>JSON.stringify(m).toLowerCase().includes(q));
  renderMessages(filtered);
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

# ── HTTP request handler ──────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # silence default access log
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
            host = q("host", "localhost")
            mgmt = int(q("mgmt", "15672"))
            vhost = q("vhost", "/")
            queue = q("queue")
            user = q("user", "guest")
            password = q("pass", "guest")
            count = min(int(q("count", "200")), 5000)
            try:
                msgs = peek_messages(host, mgmt, vhost, queue, user, password, count)
                self.send_json({"messages": msgs})
            except urllib.error.HTTPError as e:
                body = e.read().decode(errors="replace")
                self.send_json({"error": f"HTTP {e.code}: {body}"})
            except Exception as e:
                self.send_json({"error": str(e)})

        elif parsed.path == "/api/queues":
            host = q("host", "localhost")
            mgmt = int(q("mgmt", "15672"))
            user = q("user", "guest")
            password = q("pass", "guest")
            try:
                queues = list_queues(host, mgmt, user, password)
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

    # Try to open the browser automatically
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
