#!/usr/bin/env node
/* Death Arrows — lobby-based multiplayer server, zero dependencies.
   Serves index.html and speaks WebSocket (RFC 6455) by hand.
   The physics core is extracted from index.html so server and clients
   run identical game logic. PORT env is respected (Render injects it).
   Run: node server.js */
'use strict';
const http = require('http'), crypto = require('crypto'), fs = require('fs'), path = require('path');
const PORT = process.env.PORT || 3000;

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
const marker = html.match(/\/\*CORE-BEGIN\*\/([\s\S]*?)\/\*CORE-END\*\//);
if(!marker){ console.error('CORE markers not found in index.html'); process.exit(1); }
const CORE = new Function(marker[1] + '\nreturn CORE;')();
const MAX_CONNS = 64, MAX_ROOMS = 16, ROOM_CAP = CORE.PLAYER_CAP;

// ---------- minimal RFC 6455 websocket ----------
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
class Conn {
  constructor(sock){
    this.sock=sock; this.buf=Buffer.alloc(0); this.frag=null; this.alive=true;
    this.pdata=null; this.room=null;
    sock.setNoDelay(true);
    sock.on('data', d=>this.onData(d));
    sock.on('error', ()=>{ this.alive=false; });
    sock.on('close', ()=>{ this.alive=false; onLeave(this); });
  }
  onData(d){
    this.buf = Buffer.concat([this.buf, d]);
    let guard=0;
    while(this.parse() && ++guard<100){}
  }
  parse(){
    const b=this.buf; if(b.length<2) return false;
    const fin=b[0]&0x80, op=b[0]&0x0f, masked=b[1]&0x80;
    let len=b[1]&0x7f, off=2;
    if(len===126){ if(b.length<4) return false; len=b.readUInt16BE(2); off=4; }
    else if(len===127){ if(b.length<10) return false; len=Number(b.readBigUInt64BE(2)); off=10; }
    if(len>32768){ this.close(); return false; }
    let mask=null;
    if(masked){ if(b.length<off+4) return false; mask=b.subarray(off,off+4); off+=4; }
    if(b.length<off+len) return false;
    let pl=Buffer.from(b.subarray(off, off+len));
    if(mask) for(let i=0;i<pl.length;i++) pl[i]^=mask[i&3];
    this.buf=b.subarray(off+len);
    switch(op){
      case 0:
        if(this.frag) this.frag=Buffer.concat([this.frag,pl]);
        if(fin && this.frag){ this.onText(this.frag.toString('utf8')); this.frag=null; }
        break;
      case 1:
        if(fin) this.onText(pl.toString('utf8'));
        else this.frag=pl;
        break;
      case 2: break;
      case 8: try{ this.sendFrame(0x8, pl.subarray(0,2)); this.sock.end(); }catch(e){} this.alive=false; break;
      case 9: this.sendFrame(0xA, pl); break;
      case 10: break;
      default: this.close();
    }
    return this.alive;
  }
  sendText(s){ this.sendFrame(0x1, Buffer.from(s,'utf8')); }
  sendFrame(op, payload){
    const len=payload.length; let header;
    if(len<126){ header=Buffer.alloc(2); header[1]=len; }
    else if(len<65536){ header=Buffer.alloc(4); header[1]=126; header.writeUInt16BE(len,2); }
    else { header=Buffer.alloc(10); header[1]=127; header.writeBigUInt64BE(BigInt(len),2); }
    header[0]=0x80|op;
    this.sock.write(Buffer.concat([header, payload]));
  }
  close(){ try{ this.sendFrame(0x8, Buffer.alloc(0)); this.sock.end(); }catch(e){} this.alive=false; }
  onText(txt){ let m; try{ m=JSON.parse(txt); }catch(e){ return; } onMessage(this, m); }
}

// ---------- rooms ----------
const conns = new Set();
const rooms = new Map();
let nextPid = 1, nextRoomId = 1;
const r1 = v=>Math.round(v*10)/10;
function clean(s, n){ return String(s==null?'':s).replace(/[^\x20-\x7E]/g,'').slice(0, n).trim(); }

function makeRoom(name, password){
  const room = { id:nextRoomId++, name:clean(name,24)||'Lobby', password:String(password==null?'':password),
    host:null, phase:'lobby', target:3, matchEnded:false, state:CORE.createState(), tickN:0 };
  rooms.set(room.id, room);
  return room;
}
function roomConns(room){ return [...conns].filter(c=>c.room===room && c.alive); }
function sendRoom(room, obj){ const s=JSON.stringify(obj); for(const c of roomConns(room)) c.sendText(s); }
function broadcastRooms(target){
  const list = [...rooms.values()].map(r=>[r.id, r.name, r.state.players.filter(p=>!p.bot).length, ROOM_CAP, r.password?1:0, r.phase==='playing'?1:0]);
  const s = JSON.stringify({t:'rooms', rooms:list});
  if(target){ target.sendText(s); return; }
  for(const c of conns) if(c.alive && !c.room) c.sendText(s);
}
function sendRoomMeta(room){
  sendRoom(room, {t:'room', id:room.id, name:room.name, host:room.host, phase:room.phase, target:room.target});
}
function broadcastRoster(room){
  const rs={};
  for(const p of room.state.players) rs[p.id]=[p.name, p.color, p.bot?1:0];
  sendRoom(room, {t:'r', rs, host:room.host});
}
function joinRoom(c, room){
  c.room = room;
  CORE.addPlayer(room.state, {id:c.pdata.id, name:c.pdata.name, color:c.pdata.color});
  if(room.host===null) room.host = c.pdata.id;
  sendRoomMeta(room); broadcastRoster(room); broadcastRooms();
}
function leaveRoom(c){
  const room = c.room; if(!room) return;
  c.room = null;
  CORE.removePlayer(room.state, c.pdata.id);
  if(room.host===c.pdata.id){
    const next = roomConns(room)[0];
    room.host = next ? next.pdata.id : null;
  }
  if(roomConns(room).length===0){
    rooms.delete(room.id);
    console.log(`> lobby closed: "${room.name}"`);
  } else {
    sendRoomMeta(room); broadcastRoster(room);
  }
  if(c.alive) c.sendText(JSON.stringify({t:'lobby'}));
  broadcastRooms();
}
function startMatch(room){
  for(const p of room.state.players) p.score = 0;
  room.state.roundNum = -1;   // resetRound bumps it: first countdown reads "GET READY"
  room.matchEnded = false;
  CORE.resetRound(room.state);   // also picks a fresh map
  room.phase = 'playing';
  room.tickN = 0;
  sendRoomMeta(room); broadcastRoster(room); broadcastRooms();
  console.log(`> match started in "${room.name}" (first to ${room.target})`);
}
function finalizeMatch(room){
  room.phase = 'lobby'; room.matchEnded = false;
  const bots = room.state.players.filter(p=>p.bot).length;
  const members = roomConns(room).map(c=>c.pdata);
  room.state = CORE.createState();
  for(const pd of members) CORE.addPlayer(room.state, {id:pd.id, name:pd.name, color:pd.color});
  CORE.setBots(room.state, bots);
  sendRoomMeta(room); broadcastRoster(room); broadcastRooms();
}
function checkMatchEnd(room){
  if(room.matchEnded){
    if(room.state.phase==='count') finalizeMatch(room);
    return;
  }
  if(room.state.phase==='over'){
    const w = room.state.players.find(p=>p.alive);
    if(w && w.score>=room.target){
      room.matchEnded = true;
      sendRoom(room, {t:'match', name:w.name, color:w.color});
      console.log(`> "${w.name}" wins the match in "${room.name}"`);
    }
  }
}
function broadcastState(room){
  // client-predicted events (puffs, arrow clacks, arrow bumps) are not forwarded;
  // player bump events carry an id so clients skip the echo of their own hit
  const ev = room.state.events.filter(e=>e.type!=='puff' && e.type!=='abounce' && !(e.type==='bump' && !e.id));
  room.state.events = [];
  const st = room.state;
  const pl = st.players.map(p=>[p.id, r1(p.x), r1(p.y), r1(p.vx), r1(p.vy), p.heavy?1:0, p.alive?1:0,
    p.score, p.active?1:0, p.charging?1:0, +p.aim.toFixed(2),
    +Math.min(1,p.chargeT/CORE.CHARGE_MAX).toFixed(2), +p.fireCd.toFixed(2)]);
  const ar = st.arrows.map(a=>[a.id, r1(a.x), r1(a.y), r1(a.vx), r1(a.vy), a.owner]);
  sendRoom(room, {t:'s', tk:st.tick, ph:st.phase, pt:+st.phaseT.toFixed(2), rn:st.roundNum, map:st.map, pl, ar, ev});
}

function onMessage(c, m){
  if(m.t==='join'){
    if(c.pdata) return;
    if(conns.size>=MAX_CONNS){ c.sendText(JSON.stringify({t:'full'})); return; }
    const name = clean(m.name,12) || 'Ball';
    const color = CORE.PALETTE.includes(m.color) ? m.color : CORE.PALETTE[nextPid % CORE.PALETTE.length];
    c.pdata = { id:nextPid++, name, color };
    conns.add(c);
    c.sendText(JSON.stringify({t:'w', id:c.pdata.id}));
    console.log(`+ ${name} (${conns.size} online)`);
    broadcastRooms(c);
    return;
  }
  if(!c.pdata) return;
  if(m.t==='list'){ broadcastRooms(c); }
  else if(m.t==='create'){
    if(c.room) return;
    if(rooms.size>=MAX_ROOMS){ c.sendText(JSON.stringify({t:'denied', msg:'Server lobby limit reached'})); return; }
    const room = makeRoom(m.name, m.password);
    joinRoom(c, room);
    console.log(`> lobby created: "${room.name}" by ${c.pdata.name}`);
  }
  else if(m.t==='joinroom'){
    if(c.room) return;
    const room = rooms.get(m.id|0);
    if(!room){ c.sendText(JSON.stringify({t:'denied', msg:'That lobby no longer exists'})); return; }
    if(room.password && room.password !== String(m.pw==null?'':m.pw)){
      c.sendText(JSON.stringify({t:'denied', msg:'Wrong password'})); return;
    }
    if(room.state.players.length >= ROOM_CAP){ c.sendText(JSON.stringify({t:'denied', msg:'Lobby is full'})); return; }
    joinRoom(c, room);
  }
  else if(m.t==='leaveroom'){ leaveRoom(c); }
  else if(m.t==='chat'){
    if(!c.room) return;
    const msg = clean(m.msg,120);
    if(!msg) return;
    sendRoom(c.room, {t:'chat', name:c.pdata.name, color:c.pdata.color, msg});
  }
  else if(m.t==='rounds'){
    if(!c.room || c.room.host!==c.pdata.id) return;
    c.room.target = Math.max(1, Math.min(15, m.n|0));
    sendRoomMeta(c.room);
  }
  else if(m.t==='start'){
    if(!c.room || c.room.host!==c.pdata.id || c.room.phase==='playing') return;
    startMatch(c.room);
  }
  else if(m.t==='bots'){
    if(!c.room || c.room.host!==c.pdata.id) return;
    CORE.setBots(c.room.state, Math.max(0, Math.min(5, m.n|0)));
    broadcastRoster(c.room);
  }
  else if(m.t==='i'){
    if(!c.room || c.room.phase!=='playing') return;
    const p = c.room.state.players.find(q=>q.id===c.pdata.id);
    if(p && Array.isArray(m.k)){
      p.input.l=!!m.k[0]; p.input.r=!!m.k[1]; p.input.u=!!m.k[2];
      p.input.d=!!m.k[3]; p.input.h=!!m.k[4]; p.input.f=!!m.k[5];
    }
  }
  else if(m.t==='p'){ c.sendText(JSON.stringify({t:'po', ts:m.ts})); }
}
function onLeave(c){
  const had = conns.delete(c);
  if(had){
    if(c.pdata) console.log(`- ${c.pdata.name}`);
    if(c.room) leaveRoom(c);
    broadcastRooms();
  }
}

// fixed-timestep: 60 Hz sim per active room, 30 Hz snapshots
let last=Date.now(), acc=0;
const STEP=1000/60;
setInterval(()=>{
  const now=Date.now();
  acc += Math.min(250, now-last); last=now;
  while(acc>=STEP){
    for(const room of [...rooms.values()]){
      if(room.phase!=='playing') continue;
      CORE.step(room.state);
      room.tickN++;
      if(room.tickN%2===0) broadcastState(room);
      checkMatchEnd(room);
    }
    acc -= STEP;
  }
}, 5);
setInterval(()=>broadcastRooms(), 1000);
setInterval(()=>{ for(const c of conns) if(c.alive) c.sendFrame(0x9, Buffer.alloc(0)); }, 20000);

// ---------- http + upgrade ----------
const srv = http.createServer((req,res)=>{
  const u=(req.url||'/').split('?')[0];
  if(u==='/' || u==='/index.html' || u==='/game.html'){
    res.writeHead(200, {'content-type':'text/html; charset=utf-8', 'cache-control':'no-store'});
    res.end(html);
  } else if(u==='/health'){ res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end(); }
});
srv.on('upgrade', (req, sock)=>{
  const key=req.headers['sec-websocket-key'];
  const ver=req.headers['sec-websocket-version'];
  if(!key || ver!=='13'){ sock.write('HTTP/1.1 400 Bad Request\r\n\r\n'); sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key+GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
  new Conn(sock);
});
srv.listen(PORT, ()=>console.log('Death Arrows server -> http://localhost:'+PORT));