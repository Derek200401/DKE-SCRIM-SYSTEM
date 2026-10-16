import express from 'express';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import admin from 'firebase-admin';
import { GoogleGenAI, Type } from '@google/genai';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3000;
const USER = process.env.ADMIN_USERNAME || 'admin';
const PASS = process.env.ADMIN_PASSWORD || 'admin123';
const SECRET = process.env.SECRET || PASS + '-dke-secret';

/* ---------- Firebase Realtime Database (permanent storage) ---------- */
function loadCred() {
  let raw = (process.env.FIREBASE_SERVICE_ACCOUNT || '').trim();
  if (!raw) throw new Error('Kulang ang FIREBASE_SERVICE_ACCOUNT sa Railway Variables.');
  if (!raw.startsWith('{')) raw = Buffer.from(raw, 'base64').toString('utf8');
  return JSON.parse(raw);
}
admin.initializeApp({
  credential: admin.credential.cert(loadCred()),
  databaseURL: process.env.FIREBASE_DATABASE_URL || 'https://dke-points-default-rtdb.asia-southeast1.firebasedatabase.app'
});
const ref = admin.database().ref('players');

let players = [];
const clients = new Set();
const toList = v => Object.entries(v || {}).map(([id, p]) => ({ id, ign: String(p.ign || ''), points: Number(p.points) || 0 }));

// Keeps a live copy of the database and pushes every change to all open websites.
await new Promise(ok => {
  ref.on('value', snap => {
    players = toList(snap.val());
    const msg = 'data: ' + JSON.stringify(players) + '\n\n';
    clients.forEach(c => c.write(msg));
    ok();
  }, err => { console.error('Firebase error:', err.message); process.exit(1); });
});

/* ---------- helpers ---------- */
const sign = s => crypto.createHmac('sha256', SECRET).update(s).digest('hex');
const safe = (a, b) => {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
};
function auth(req, res, next) {
  const [exp, sig] = (req.headers.authorization || '').replace('Bearer ', '').split('.');
  if (exp && sig && Date.now() < +exp && safe(sig, sign(exp))) return next();
  res.status(401).json({ error: 'Session expired. Mag-login ulit.' });
}
const clean = v => String(v || '').trim().slice(0, 40);
const num = v => (Number.isFinite(+v) ? Math.round(+v) : null);
const find = ign => players.find(p => p.ign.toLowerCase() === ign.toLowerCase());
const wrap = fn => (req, res) => fn(req, res).catch(e => res.status(500).json({ error: 'Server error: ' + e.message }));

/* ---------- app ---------- */
const app = express();
app.use('/api/scan', express.json({ limit: '10mb' }));
app.use(express.json({ limit: '100kb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/players', (req, res) => res.json(players));

app.get('/api/stream', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
  res.flushHeaders();
  res.write('data: ' + JSON.stringify(players) + '\n\n');
  clients.add(res);
  const ping = setInterval(() => res.write(': ping\n\n'), 20000);
  req.on('close', () => { clients.delete(res); clearInterval(ping); });
});

app.post('/api/login', (req, res) => {
  const { username, password } = req.body || {};
  if (safe(username || '', USER) && safe(password || '', PASS)) {
    const exp = String(Date.now() + 7 * 24 * 3600 * 1000);
    return res.json({ token: exp + '.' + sign(exp) });
  }
  res.status(401).json({ error: 'Mali ang username o password.' });
});

app.post('/api/players', auth, wrap(async (req, res) => {
  const ign = clean(req.body.ign), points = num(req.body.points);
  if (!ign || points === null) return res.status(400).json({ error: 'Kailangan ng IGN at points.' });
  if (find(ign)) return res.status(409).json({ error: 'Existing na ang IGN na ito.' });
  await ref.push({ ign, points });
  res.json({ ok: true });
}));

app.put('/api/players/:id', auth, wrap(async (req, res) => {
  const p = players.find(x => x.id === req.params.id);
  const ign = clean(req.body.ign), points = num(req.body.points);
  if (!p) return res.status(404).json({ error: 'Hindi mahanap ang player.' });
  if (!ign || points === null) return res.status(400).json({ error: 'Kailangan ng IGN at points.' });
  const dup = find(ign);
  if (dup && dup.id !== p.id) return res.status(409).json({ error: 'Existing na ang IGN na ito.' });
  await ref.child(p.id).update({ ign, points });
  res.json({ ok: true });
}));

app.delete('/api/players/:id', auth, wrap(async (req, res) => {
  await ref.child(req.params.id).remove();
  res.json({ ok: true });
}));

// Add points to many players at once (from the scan confirmation)
app.post('/api/players/bulk', auth, wrap(async (req, res) => {
  const rows = Array.isArray(req.body.rows) ? req.body.rows.slice(0, 20) : [];
  const seen = new Map(), upd = {};
  for (const r of rows) {
    const ign = clean(r.ign), pts = num(r.points);
    if (!ign || pts === null) continue;
    const k = ign.toLowerCase();
    let q = seen.get(k);
    if (!q) {
      const e = find(ign);
      q = e ? { ...e, isNew: false } : { id: ref.push().key, ign, points: 0, isNew: true };
      seen.set(k, q);
    }
    q.points += pts;
  }
  for (const q of seen.values()) {
    if (q.isNew) upd[q.id] = { ign: q.ign, points: q.points };
    else upd[q.id + '/points'] = q.points;
  }
  if (Object.keys(upd).length) await ref.update(upd);
  res.json({ ok: true });
}));

/* ---------- Gemini scoreboard scan ---------- */
const schema = {
  type: Type.OBJECT,
  properties: {
    matchResult: { type: Type.STRING, description: 'VICTORY or DEFEAT (banner at the top, applies to the LEFT team)' },
    leftTeamScore: { type: Type.NUMBER },
    rightTeamScore: { type: Type.NUMBER },
    players: {
      type: Type.ARRAY,
      description: 'Exactly 10 players: 5 Left then 5 Right, top to bottom',
      minItems: 10,
      maxItems: 10,
      items: {
        type: Type.OBJECT,
        properties: {
          side: { type: Type.STRING, description: "Must be 'Left' or 'Right'" },
          ign: { type: Type.STRING, description: 'Exact in-game name, preserving custom fonts/symbols, excluding flags, gender icons, level numbers' },
          rating: { type: Type.NUMBER, description: 'Exact decimal rating printed under the medal icon (e.g. 12.3, 8.2, 3.0)' },
          medal: { type: Type.STRING, description: "Medal icon shown: 'Victory MVP', 'Supreme Gold', 'Gold', 'Silver', 'Bronze', or 'Defeat MVP (MVP Loss)'" }
        },
        required: ['side', 'ign', 'rating', 'medal']
      }
    }
  },
  required: ['matchResult', 'leftTeamScore', 'rightTeamScore', 'players']
};

const PROMPT = `Analyze this Mobile Legends end-game scoreboard carefully.
1. Read the top banner (VICTORY or DEFEAT). It applies to the LEFT (blue) team. Also read both team kill scores.
2. Extract all 10 players row by row: 5 on the Left side (names on the left) and 5 on the Right side (names on the right), top to bottom.
3. For each player capture:
   - The exact IGN with every letter, digit, symbol and special character (for example ø ë Ω π . ~ " ²) and spacing. Exclude the gender icon, country flag, level number and any small icon beside the name.
   - The exact rating: the decimal number printed at the bottom of the medal / MVP icon near the centre divider. Do NOT confuse it with kills/deaths/assists, gold or level numbers.
   - The medal icon beside the rating. Rules: Winning team: Victory MVP = highest score of the winning team; Supreme Gold 10.1-15.0; Gold 7.1-10.0; Silver 4.1-7.0; Bronze 3.0-4.0. Losing team: Defeat MVP (MVP Loss) = highest score of the losing team (minimum 6.6, sometimes none); Gold 7.1 and up; Silver 4.1-7.0; Bronze 3.0-4.0.`;

app.post('/api/scan', auth, wrap(async (req, res) => {
  if (!process.env.GEMINI_API_KEY) return res.status(500).json({ error: 'Wala pang GEMINI_API_KEY sa Railway Variables.' });
  const m = /^data:(image\/(?:jpeg|png|webp));base64,(.+)$/.exec(req.body.image || '');
  if (!m) return res.status(400).json({ error: 'Invalid na image.' });
  try {
    const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    let d;
    for (let i = 0; i < 2; i++) { // retry once if it did not return all 10 players
      const response = await ai.models.generateContent({
        model: process.env.GEMINI_MODEL || 'gemini-2.5-flash',
        contents: [{ inlineData: { data: m[2], mimeType: m[1] } }, PROMPT],
        config: { responseMimeType: 'application/json', responseSchema: schema, temperature: 0 }
      });
      d = JSON.parse(response.text);
      if ((d.players || []).length === 10) break;
    }
    const medal = x => { const t = String(x || '').toLowerCase(); return t.includes('mvp') ? 'MVP' : t.includes('supreme') ? 'Supreme Gold' : t.includes('gold') ? 'Gold' : t.includes('silver') ? 'Silver' : t.includes('bronze') ? 'Bronze' : ''; };
    const fix = side => (d.players || []).filter(p => String(p.side).toLowerCase() === side).slice(0, 5)
      .map(p => ({ ign: clean(p.ign), rating: Math.min(15, Math.max(0, Math.round(parseFloat(p.rating) * 10) / 10 || 0)), medal: medal(p.medal) }));
    res.json({ banner: String(d.matchResult || '').toUpperCase(), left: fix('left'), right: fix('right') });
  } catch (e) {
    const msg = String(e.message || e);
    const busy = /429|quota|RESOURCE_EXHAUSTED/i.test(msg);
    res.status(502).json({ error: busy ? 'Naubos muna ang free limit ng Gemini. Maghintay ng ilang minuto at subukan ulit.' : 'Hindi na-scan: ' + msg.slice(0, 200) });
  }
}));

app.listen(PORT, () => console.log('DKE Scrim Points running on ' + PORT));
