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

await new Promise(ok => {
  ref.on('value', snap => {
    players = toList(snap.val());
    const msg = 'data: ' + JSON.stringify(players) + '\n\n';
    clients.forEach(c => c.write(msg));
    ok();
  }, err => { console.error('Firebase error:', err.message); process.exit(1); });
});

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
const normalizeIgn = v => {
  let s = String(v || '').trim().replace(/\u00A0/g, ' ').replace(/\s+/g, ' ');
  const match = s.match(/^([A-Z0-9][A-Z0-9._-]{0,7})\s+(.+)$/);
  if (match) {
    const tag = match[1];
    const name = match[2].trim();
    if (name.length >= 2 && /^[A-Z0-9._-]{2,8}$/i.test(tag) && !/^\d+$/.test(tag)) {
      const upperTag = tag.toUpperCase();
      const ignored = ['BP', 'IS', 'TDB', 'TB', 'TEAM', 'CLAN'];
      if (ignored.includes(upperTag) || /^[A-Z0-9]{2,5}$/.test(tag)) return name;
    }
  }
  return s.slice(0, 40);
};
const clean = v => normalizeIgn(v);
const num = v => {
  if (v === null || v === undefined || String(v).trim() === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.round(n) : null;
};
const find = ign => players.find(p => p.ign.toLowerCase() === ign.toLowerCase());
const canonicalIgn = ign => clean(ign).replace(/[\u02B0-\u02FF]/g, '').normalize('NFKD')
  .replace(/\p{M}/gu, '')
  .replace(/[\u200B-\u200D\uFEFF]/g, '')
  .toLowerCase()
  .replace(/[^\p{L}\p{N}]/gu, '');
function similarity(a, b) {
  if (!a || !b) return 0;
  const left = Array.from(a), right = Array.from(b);
  let previous = Array.from({ length: right.length + 1 }, (_, i) => i);
  for (let i = 1; i <= left.length; i++) {
    const current = [i];
    for (let j = 1; j <= right.length; j++) {
      current[j] = Math.min(
        current[j - 1] + 1,
        previous[j] + 1,
        previous[j - 1] + (left[i - 1] === right[j - 1] ? 0 : 1)
      );
    }
    previous = current;
  }
  return 1 - previous[right.length] / Math.max(left.length, right.length);
}
function matchIgn(ign, records, excludeId = null) {
  const target = canonicalIgn(ign);
  if (!target) return [];
  const available = records.filter(p => p.id !== excludeId);
  const exactMatches = available.filter(p => canonicalIgn(p.ign) === target);
  if (exactMatches.length) return exactMatches;
  const matches = available
    .map(player => ({ player, score: similarity(target, canonicalIgn(player.ign)) }))
    .filter(x => x.score >= 0.8)
    .sort((a, b) => b.score - a.score);
  if (!matches.length) return [];
  if (matches.length > 1 && matches[0].score === matches[1].score) return null;
  return [matches[0].player];
}
const wrap = fn => (req, res) => fn(req, res).catch(e => res.status(500).json({ error: 'Server error: ' + e.message }));

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
  const rows = Array.isArray(req.body.rows) ? req.body.rows : [];
  if (!rows.length || rows.length > 20) {
    return res.status(400).json({ error: 'Walang rows o sobra sa 20 players ang scanned result.' });
  }
  if (rows.some(r => !r || !clean(r.ign) || num(r.points) === null)) {
    return res.status(400).json({ error: 'May IGN o points na kulang o invalid sa scanned result.' });
  }
  const prepared = rows.map(r => ({ ign: clean(r.ign), points: num(r.points), id: ref.push().key }));
  let ambiguousIgn = '';
  const result = await ref.transaction(current => {
    ambiguousIgn = '';
    const data = current && typeof current === 'object' ? { ...current } : {};
    const records = Object.entries(data).map(([id, p]) => ({
      id,
      ign: String(p?.ign || '')
    }));
    for (const row of prepared) {
      const matches = matchIgn(row.ign, records);
      if (matches === null) {
        ambiguousIgn = row.ign;
        return;
      }
      if (matches.length) {
        const keeper = matches[0];
        const mergedPoints = matches.reduce((total, player) => total + (Number(data[player.id]?.points) || 0), row.points);
        const matchedIds = new Set(matches.map(player => player.id));
        for (const player of matches.slice(1)) delete data[player.id];
        data[keeper.id] = { ...data[keeper.id], points: mergedPoints };
        for (let i = records.length - 1; i >= 0; i--) {
          if (matchedIds.has(records[i].id) && records[i].id !== keeper.id) records.splice(i, 1);
        }
      } else {
        data[row.id] = { ign: row.ign, points: row.points };
        records.push({ id: row.id, ign: row.ign });
      }
    }
    return data;
  });
  if (!result.committed) {
    if (ambiguousIgn) {
      return res.status(409).json({ error: `Hindi naidagdag ang points: may dalawang magkapantay na posibleng IGN para sa "${ambiguousIgn}". I-check ang leaderboard at itama muna ang IGN.` });
    }
    return res.status(500).json({ error: 'Hindi na-save ang points sa database. Subukan ulit.' });
  }
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
   - IMPORTANT: Ignore any squad/clan tag prefix attached to the IGN. A tag such as BP, TDB, IS, or any short uppercase label before the actual name is NOT part of the IGN. Output only the real IGN.
   - Examples of tag stripping: 'BP RAVEN' -> 'RAVEN'; 'TDB bℓευ' -> 'bℓευ'; 'IS CARL' -> 'CARL'; 'IS R.Shen²' -> 'R.Shen²'.
   - The exact rating: the decimal number printed at the bottom of the medal / MVP icon near the centre divider. Do NOT confuse it with kills/deaths/assists, gold or level numbers.
   - The medal icon beside the rating. Rules: Winning team: Victory MVP = highest score of the winning team; Supreme Gold 10.1-15.0; Gold 7.1-10.0; Silver 4.1-7.0; Bronze 3.0-4.0. Losing team: Defeat MVP (MVP Loss) = highest score of the losing team (minimum 6.6, sometimes none); Gold 7.1 and up; Silver 4.1-7.0; Bronze 3.0-4.0.`;

const MODELS = [process.env.GEMINI_MODEL, 'gemini-3.8-flash', 'gemini-flash-latest', 'gemini-flash-lite-latest'].filter((x, i, a) => x && a.indexOf(x) === i);
async function generate(ai, m) {
  let last;
  for (const model of MODELS) {
    try {
      return await ai.models.generateContent({
        model,
        contents: [{ inlineData: { data: m[2], mimeType: m[1] } }, PROMPT],
        config: { responseMimeType: 'application/json', responseSchema: schema, temperature: 0 }
      });
    } catch (e) {
      last = e;
      const msg = String(e.message || e);
      if (/503|UNAVAILABLE|high demand|overloaded|404|NOT_FOUND|no longer available|not found/i.test(msg)) continue;
      throw e;
    }
  }
  throw last;
}

function validateScan(d) {
  if (!d || !['VICTORY', 'DEFEAT'].includes(String(d.matchResult || '').trim().toUpperCase()) || !Array.isArray(d.players)) return false;
  const left = d.players.filter(p => p && String(p.side || '').toLowerCase() === 'left');
  const right = d.players.filter(p => p && String(p.side || '').toLowerCase() === 'right');
  if (left.length !== 5 || right.length !== 5 || d.players.length !== 10) return false;
  return d.players.every(p => {
    if (!p || typeof p.ign !== 'string' || p.rating === null || p.rating === undefined || String(p.rating).trim() === '') return false;
    const rating = Number(p.rating);
    return Boolean(clean(p.ign)) && Number.isFinite(rating) && rating >= 0 && rating <= 15;
  });
}

app.post('/api/scan', auth, wrap(async (req, res) => {
  if (!process.env.GEMINI_API_KEY) return res.status(500).json({ error: 'Wala pang GEMINI_API_KEY sa Railway Variables.' });
  const m = /^data:(image\/(?:jpeg|png|webp));base64,(.+)$/.exec(req.body.image || '');
  if (!m) return res.status(400).json({ error: 'Invalid na image.' });
  try {
    const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    let d;
    for (let i = 0; i < 2; i++) {
      const response = await generate(ai, m);
      try { d = JSON.parse(response.text); }
      catch { d = null; }
      if (validateScan(d)) break;
    }
    if (!validateScan(d)) {
      return res.status(502).json({ error: 'Hindi kumpleto o hindi malinaw ang scan (kailangan 5 players bawat team). Subukan ulit gamit ang malinaw at buong scoreboard image.' });
    }
    const medal = x => { const t = String(x || '').toLowerCase(); return t.includes('mvp') ? 'MVP' : t.includes('supreme') ? 'Supreme Gold' : t.includes('gold') ? 'Gold' : t.includes('silver') ? 'Silver' : t.includes('bronze') ? 'Bronze' : ''; };
    const fix = side => (d.players || []).filter(p => String(p.side).toLowerCase() === side).slice(0, 5)
      .map(p => ({ ign: clean(p.ign), rating: Math.min(15, Math.max(0, Math.round(parseFloat(p.rating) * 10) / 10 || 0)), medal: medal(p.medal) }));
    res.json({ banner: String(d.matchResult || '').toUpperCase(), left: fix('left'), right: fix('right') });
  } catch (e) {
    const msg = String(e.message || e);
    const busy = /429|quota|RESOURCE_EXHAUSTED/i.test(msg);
    const down = /503|UNAVAILABLE|high demand|overloaded/i.test(msg);
    res.status(502).json({ error: down ? 'Busy ang Gemini ngayon (pansamantala lang). Subukan ulit sa loob ng ilang segundo.' : busy ? 'Naubos muna ang free limit ng Gemini. Maghintay ng ilang minuto at subukan ulit.' : 'Hindi na-scan: ' + msg.slice(0, 200) });
  }
}));

app.listen(PORT, () => console.log('DKE Scrim Points running on ' + PORT));
