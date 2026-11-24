// GameOn сервер: статика + WebSocket-лобби (Node.js, без внешних зависимостей)
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  const file = path.join(__dirname, 'index.html');
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(500); res.end('error'); return; }
    res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});
    res.end(data);
  });
});

// ---- минимальный WebSocket (RFC6455) ----
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function wsAccept(key){ return crypto.createHash('sha1').update(key+GUID).digest('base64'); }
function encodeFrame(str){
  const payload = Buffer.from(str), len = payload.length;
  let header;
  if (len < 126) { header = Buffer.from([0x81, len]); }
  else if (len < 65536) { header = Buffer.alloc(4); header[0]=0x81; header[1]=126; header.writeUInt16BE(len,2); }
  else { header = Buffer.alloc(10); header[0]=0x81; header[1]=127; header.writeBigUInt64BE(BigInt(len),2); }
  return Buffer.concat([header, payload]);
}
function decodeFrames(buf){
  const frames=[]; let off=0;
  while (off+2 <= buf.length) {
    const opcode = buf[off] & 0x0f;
    let len = buf[off+1] & 0x7f, hlen=2;
    if (len===126){ if(off+4>buf.length) break; len=buf.readUInt16BE(off+2); hlen=4; }
    else if (len===127){ if(off+10>buf.length) break; len=Number(buf.readBigUInt64BE(off+2)); hlen=10; }
    if (off+hlen+len > buf.length) break;
    frames.push({opcode, payload: buf.slice(off+hlen, off+hlen+len)});
    off += hlen+len;
  }
  return frames;
}

const rooms = new Map(); // code -> {players:[sockA,sockB], meta:{}, started:bool}
let waiting = null; // очередь быстрого матча

function send(sock, obj){ try{ sock.write(encodeFrame(JSON.stringify(obj))); }catch(e){} }
function broadcast(room, obj, except){ room.players.forEach(p=>{ if(p && p!==except) send(p, obj); }); }
function roomOf(sock){ return sock._room ? rooms.get(sock._room) : null; }

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { sock.destroy(); return; }
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'+
             'Sec-WebSocket-Accept: '+wsAccept(key)+'\r\n\r\n');
  sock.setNoDelay(true);
  send(sock, {t:'connected'});

  let buf = Buffer.alloc(0);
  sock.on('data', d => {
    buf = Buffer.concat([buf, d]);
    decodeFrames(buf).forEach(fr => {
      if (fr.opcode === 8) { cleanup(sock); sock.end(); return; }
      if (fr.opcode !== 1) return;
      buf = Buffer.alloc(0);
      let m; try { m = JSON.parse(fr.payload.toString()); } catch(e){ return; }
      handle(sock, m);
    });
  });
  sock.on('close', ()=>cleanup(sock));
  sock.on('error', ()=>cleanup(sock));
});

function leaveRoom(sock){
  const code = sock._room; if (!code) return;
  const room = rooms.get(code);
  if (room){
    room.players = room.players.filter(p=>p!==sock);
    broadcast(room, {t:'peer_left'});
    if (room.players.length===0) rooms.delete(code);
  }
  sock._room = null;
}
function cleanup(sock){ if (sock._room) leaveRoom(sock); if (waiting===sock) waiting=null; }

function handle(sock, m){
  const me = () => {
    const r = roomOf(sock);
    return r ? {room:r, idx:r.players.indexOf(sock)} : null;
  };
  switch(m.t){
    case 'create': {
      leaveRoom(sock);
      const code = crypto.randomBytes(3).toString('hex').toUpperCase().slice(0,5);
      rooms.set(code, {players:[sock], started:false});
      sock._room = code;
      send(sock, {t:'created', code});
      break;
    }
    case 'join': {
      leaveRoom(sock);
      const code = String(m.code||'').toUpperCase();
      const room = rooms.get(code);
      if (!room) { send(sock, {t:'error', msg:'Комната не найдена'}); break; }
      if (room.players.length >= 2) { send(sock, {t:'error', msg:'Комната заполнена'}); break; }
      room.p


layers.push(sock); sock._room = code;
      send(sock, {t:'joined', code});
      broadcast(room, {t:'peer_joined'});
      break;
    }
    case 'leave': { leaveRoom(sock); send(sock, {t:'left'}); break; }
    case 'quick': { // быстрый матч
      leaveRoom(sock);
      if (waiting && waiting !== sock && waiting._room==null) {
        const code = crypto.randomBytes(3).toString('hex').toUpperCase().slice(0,5);
        rooms.set(code, {players:[waiting, sock], started:false});
        waiting._room = code; sock._room = code; waiting = null;
        send(sock, {t:'joined', code}); broadcast(rooms.get(code), {t:'peer_joined'});
      } else { waiting = sock; send(sock, {t:'waiting'}); }
      break;
    }
    case 'start': { // хост жмёт «начать», обоим уходит сигнал старта
      const ctx = me(); if (!ctx) break;
      ctx.room.started = true;
      broadcast(ctx.room, {t:'start_game', host: ctx.idx===0?1:0});
      break;
    }
    default: { // всё остальное — релей врагу: msg, state, score, result...
      const ctx = me(); if (!ctx) break;
      m._from = ctx.idx;
      broadcast(ctx.room, m, sock);
    }
  }
}

server.listen(PORT, ()=>console.log('GameOn на порту '+PORT));