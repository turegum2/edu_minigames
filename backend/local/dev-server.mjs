// =========================
// file: backend/local/dev-server.mjs
// =========================
import http from "node:http";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import jwt from "jsonwebtoken";
import { fileURLToPath } from "node:url";

const PORT = Number(process.env.PORT || 8787);
const JWT_SECRET = process.env.JWT_SECRET || "dev_secret_change_me";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Stable paths regardless of where you run node from
const ROOT_BACKEND = path.resolve(__dirname, "..");              // backend/
const ROOT_PUBLIC = path.resolve(ROOT_BACKEND, "..", "public");  // public/
const DATA_DIR = path.resolve(ROOT_BACKEND, "local", "data");
const DB_PATH = path.resolve(DATA_DIR, "db.json");
const RAW_DIR = path.resolve(DATA_DIR, "raw");

const GAMES = [
  { game_id: "parabola", title: "Parabola" },
  { game_id: "balancer", title: "Balancer" },
  { game_id: "graph_master", title: "Graph Master" },
  { game_id: "chemical_detective", title: "Chemical Detective" },
  { game_id: "constructor", title: "Constructor" },
];

function normalizePathname(p) {
  if (!p) return "/";
  // Remove trailing slash (except root)
  if (p.length > 1 && p.endsWith("/")) return p.slice(0, -1);
  return p;
}

function json(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    "Cache-Control": "no-store",
  });
  res.end(body);
}

function notFound(res) {
  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Not found");
}

async function readBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf-8");
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function ensureDirs() {
  await fsp.mkdir(DATA_DIR, { recursive: true });
  await fsp.mkdir(RAW_DIR, { recursive: true });
}

async function loadDb() {
  await ensureDirs();
  try {
    const txt = await fsp.readFile(DB_PATH, "utf-8");
    return JSON.parse(txt);
  } catch {
    return {
      users: {}, // user_id -> {user_id, phone, name, created_at}
      phoneToUser: {}, // phone -> user_id
      otps: {}, // phone -> {code, expires_at}
      gameStats: {}, // user_id -> { game_id -> {last_stars,best_stars,updated_at} }
      saves: {}, // user_id -> { game_id -> {payload,updated_at} }
      sessions: {}, // session_id -> { ... }
    };
  }
}

async function saveDb(db) {
  await ensureDirs();
  await fsp.writeFile(DB_PATH, JSON.stringify(db, null, 2), "utf-8");
}

function normalizePhone(p) {
  return String(p || "").trim();
}

function authFromReq(req) {
  const h = req.headers.authorization || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  if (!m) return null;
  try {
    const payload = jwt.verify(m[1], JWT_SECRET);
    return payload;
  } catch {
    return null;
  }
}

function makeToken(user) {
  return jwt.sign(
    { user_id: user.user_id, phone: user.phone, name: user.name || "" },
    JWT_SECRET,
    { expiresIn: "30d" }
  );
}

function calcStarsTotalFromSummary(summary) {
  if (!summary || typeof summary !== "object") return 0;
  const v = summary.stars_total;
  if (Number.isFinite(v)) return Math.max(0, Math.floor(v));
  if (Array.isArray(summary.stars_by_level)) {
    return summary.stars_by_level.reduce((a, b) => a + (Number(b) || 0), 0);
  }
  return 0;
}

async function writeRawEvents(gameId, userId, sessionId, events, summary) {
  const dt = new Date().toISOString().slice(0, 10);
  const dir = path.join(RAW_DIR, `game=${gameId}`, `dt=${dt}`, `user=${userId}`);
  await fsp.mkdir(dir, { recursive: true });
  const file = path.join(dir, `session=${sessionId}.jsonl`);

  const lines = [];
  lines.push(
    JSON.stringify({
      type: "meta",
      t: Date.now(),
      game_id: gameId,
      user_id: userId,
      session_id: sessionId,
    })
  );
  for (const e of Array.isArray(events) ? events : []) {
    lines.push(JSON.stringify(e));
  }
  lines.push(JSON.stringify({ type: "summary", t: Date.now(), summary: summary || {} }));

  await fsp.writeFile(file, lines.join("\n") + "\n", "utf-8");
  return `local://${path.relative(DATA_DIR, file).replace(/\\/g, "/")}`;
}

function contentTypeByExt(ext) {
  switch (ext) {
    case ".html":
      return "text/html; charset=utf-8";
    case ".js":
      return "application/javascript; charset=utf-8";
    case ".css":
      return "text/css; charset=utf-8";
    case ".woff2":
      return "font/woff2";
    case ".woff":
      return "font/woff";
    case ".ttf":
      return "font/ttf";
    case ".png":
      return "image/png";
    case ".jpg":
    case ".jpeg":
      return "image/jpeg";
    case ".svg":
      return "image/svg+xml";
    case ".json":
      return "application/json; charset=utf-8";
    default:
      return "application/octet-stream";
  }
}

function serveStatic(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  let p = decodeURIComponent(url.pathname);
  p = normalizePathname(p);

  if (p === "/") p = "/index.html";

  const filePath = path.resolve(ROOT_PUBLIC, "." + p);
  if (!filePath.startsWith(ROOT_PUBLIC)) {
    return notFound(res);
  }

  fs.stat(filePath, (err, st) => {
    if (err || !st.isFile()) return notFound(res);
    const ext = path.extname(filePath).toLowerCase();
    const ct = contentTypeByExt(ext);
    res.writeHead(200, { "Content-Type": ct, "Cache-Control": "no-store" });
    fs.createReadStream(filePath).pipe(res);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = normalizePathname(url.pathname);

  // CORS preflight
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
      "Access-Control-Max-Age": "600",
    });
    return res.end();
  }

  // API routes
  if (pathname.startsWith("/api/")) {
    const db = await loadDb();

    // /api/auth/start
    if (req.method === "POST" && pathname === "/api/auth/start") {
      const body = await readBody(req);
      const phone = normalizePhone(body?.phone);
      if (!phone) return json(res, 400, { ok: false, error: "phone_required" });

      const code = String(Math.floor(1000 + Math.random() * 9000)); // 4 digits
      const expiresAt = Date.now() + 10 * 60 * 1000;

      db.otps[phone] = { code, expires_at: expiresAt };
      await saveDb(db);

      // local debug code for testing
      return json(res, 200, { ok: true, debug_code: code });
    }

    // /api/auth/verify
    if (req.method === "POST" && pathname === "/api/auth/verify") {
      const body = await readBody(req);
      const phone = normalizePhone(body?.phone);
      const code = String(body?.code || "").trim();
      if (!phone || !code)
        return json(res, 400, { ok: false, error: "phone_and_code_required" });

      const otp = db.otps[phone];
      if (!otp || otp.expires_at < Date.now())
        return json(res, 401, { ok: false, error: "code_expired" });
      if (String(otp.code) !== code)
        return json(res, 401, { ok: false, error: "code_invalid" });

      delete db.otps[phone];

      let userId = db.phoneToUser[phone];
      if (!userId) {
        userId = crypto.randomUUID();
        db.phoneToUser[phone] = userId;
        db.users[userId] = {
          user_id: userId,
          phone,
          name: "",
          created_at: new Date().toISOString(),
        };
      }

      await saveDb(db);

      const token = makeToken(db.users[userId]);
      return json(res, 200, { ok: true, token });
    }

    // Auth required below
    const auth = authFromReq(req);
    if (!auth) return json(res, 401, { ok: false, error: "unauthorized" });

    const user = db.users[auth.user_id];
    if (!user) return json(res, 401, { ok: false, error: "unauthorized" });

    // /api/me (GET)
    if (req.method === "GET" && pathname === "/api/me") {
      const stats = db.gameStats[user.user_id] || {};
      const saves = db.saves[user.user_id] || {};
      const games = GAMES.map((g) => {
        const st = stats[g.game_id] || {};
        return {
          game_id: g.game_id,
          title: g.title,
          last_stars: st.last_stars || 0,
          best_stars: st.best_stars || 0,
          has_save: !!saves[g.game_id],
        };
      });
      return json(res, 200, {
        ok: true,
        user: { user_id: user.user_id, phone: user.phone, name: user.name || "", games },
      });
    }

    // /api/me (POST) set name
    if (req.method === "POST" && pathname === "/api/me") {
      const body = await readBody(req);
      const name = String(body?.name || "").trim();
      if (!name) return json(res, 400, { ok: false, error: "name_required" });
      user.name = name;
      db.users[user.user_id] = user;
      await saveDb(db);
      return json(res, 200, { ok: true });
    }

    // saves: /api/games/{gameId}/save
    const saveMatch = pathname.match(/^\/api\/games\/([^\/]+)\/save$/);
    if (saveMatch) {
      const gameId = decodeURIComponent(saveMatch[1]);
      if (!GAMES.some((g) => g.game_id === gameId))
        return json(res, 404, { ok: false, error: "unknown_game" });

      db.saves[user.user_id] ||= {};

      if (req.method === "GET") {
        const s = db.saves[user.user_id][gameId];
        if (!s) return json(res, 200, { ok: true, save: null });
        return json(res, 200, { ok: true, save: { updated_at: s.updated_at, payload: s.payload } });
      }

      if (req.method === "PUT") {
        const body = await readBody(req);
        const payload = body?.save ?? body?.payload;
        if (!payload || typeof payload !== "object")
          return json(res, 400, { ok: false, error: "save_required" });
        db.saves[user.user_id][gameId] = { payload, updated_at: new Date().toISOString() };
        await saveDb(db);
        return json(res, 200, { ok: true });
      }

      if (req.method === "DELETE") {
        delete db.saves[user.user_id][gameId];
        await saveDb(db);
        return json(res, 200, { ok: true });
      }
    }

    // session start: /api/games/{gameId}/session/start
    const startMatch = pathname.match(/^\/api\/games\/([^\/]+)\/session\/start$/);
    if (req.method === "POST" && startMatch) {
      const gameId = decodeURIComponent(startMatch[1]);
      if (!GAMES.some((g) => g.game_id === gameId))
        return json(res, 404, { ok: false, error: "unknown_game" });

      const sessionId = crypto.randomUUID();
      db.sessions[sessionId] = {
        session_id: sessionId,
        user_id: user.user_id,
        game_id: gameId,
        started_at: new Date().toISOString(),
      };
      await saveDb(db);
      return json(res, 200, { ok: true, session_id: sessionId });
    }

    // session finish: /api/games/{gameId}/session/{sessionId}/finish
    const finMatch = pathname.match(/^\/api\/games\/([^\/]+)\/session\/([^\/]+)\/finish$/);
    if (req.method === "POST" && finMatch) {
      const gameId = decodeURIComponent(finMatch[1]);
      const sessionId = decodeURIComponent(finMatch[2]);

      const sess = db.sessions[sessionId];
      if (!sess || sess.user_id !== user.user_id || sess.game_id !== gameId) {
        return json(res, 404, { ok: false, error: "session_not_found" });
      }

      const body = await readBody(req);
      const reason = String(body?.reason || "exit");
      const summary = body?.summary || {};
      const events = Array.isArray(body?.events) ? body.events : [];

      const rawKey = await writeRawEvents(gameId, user.user_id, sessionId, events, summary);
      const starsTotal = calcStarsTotalFromSummary(summary);

      db.gameStats[user.user_id] ||= {};
      const st = db.gameStats[user.user_id][gameId] || { last_stars: 0, best_stars: 0 };
      st.last_stars = starsTotal;
      st.best_stars = Math.max(st.best_stars || 0, starsTotal);
      st.updated_at = new Date().toISOString();
      db.gameStats[user.user_id][gameId] = st;

      sess.finished_at = new Date().toISOString();
      sess.reason = reason;
      sess.stars_total = starsTotal;
      sess.raw_key = rawKey;
      sess.summary = summary;

      await saveDb(db);
      return json(res, 200, { ok: true, raw_key: rawKey, stars_total: starsTotal });
    }

    return json(res, 404, { ok: false, error: "not_found" });
  }

  // Static
  return serveStatic(req, res);
});

server.listen(PORT, () => {
  console.log(`Local dev server: http://localhost:${PORT}`);
  console.log(`Serving public/: ${ROOT_PUBLIC}`);
  console.log(`DB: ${DB_PATH}`);
});