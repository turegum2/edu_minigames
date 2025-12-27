// =========================
// file: backend/function/app.mjs
// =========================
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);

const { v4: uuidv4 } = require("uuid");
const jwt = require("jsonwebtoken");
const crypto = require("node:crypto");

const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { SNSClient, PublishCommand } = require("@aws-sdk/client-sns");
const { Ydb } = require("ydb-sdk-lite");

/**
 * Cloud Function handler for Yandex API Gateway.
 * Assumes OpenAPI routes all /api/* to this function (payload_format_version: "1.0").
 */

// ---- config ----
const JWT_SECRET = process.env.JWT_SECRET || "";
const YDB_DB_NAME = process.env.YDB_DB_NAME || "";
const TP = (process.env.YDB_TABLE_PREFIX || "").trim();

// Accept both names: LOGS_BUCKET (old) and RAW_BUCKET (new, used in GitHub vars)
const LOGS_BUCKET = process.env.LOGS_BUCKET || process.env.RAW_BUCKET || "";
const LOGS_PREFIX = (process.env.LOGS_PREFIX || "raw").replace(/\/+$/, "");

const S3_ENDPOINT = process.env.S3_ENDPOINT || "https://storage.yandexcloud.net";
const AWS_REGION = process.env.AWS_REGION || "ru-central1";

const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || "";
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || "";

const SMS_MODE = (process.env.SMS_MODE || "cns").toLowerCase(); // cns | mock
const CNS_ENDPOINT = process.env.CNS_ENDPOINT || "https://notifications.yandexcloud.net";
const DEBUG_OTP = (process.env.DEBUG_OTP || "0") === "1";

// games catalog
const GAMES = [
  { game_id: "parabola", title: "Parabola" },
  { game_id: "balancer", title: "Balancer" },
  { game_id: "graph_master", title: "Graph Master" },
  { game_id: "chemical_detective", title: "Chemical Detective" },
  { game_id: "constructor", title: "Constructor" },
];

// ---- clients (lazy singletons) ----
let ydb = null;
let s3 = null;
let sns = null;
let schemaEnsured = false;

function normalizePathname(p) {
  if (!p) return "/";
  if (p.length > 1 && p.endsWith("/")) return p.slice(0, -1);
  return p;
}

function resp(statusCode, obj, extraHeaders) {
  return {
    statusCode,
    isBase64Encoded: false,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
      "Cache-Control": "no-store",
      ...(extraHeaders || {}),
    },
    body: JSON.stringify(obj),
  };
}

function normalizePhone(p) {
  return String(p || "").trim();
}

function sha256(s) {
  return crypto.createHash("sha256").update(String(s)).digest("hex");
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

function parseEvent(event) {
  const method = event.httpMethod || event?.requestContext?.httpMethod || "GET";
  const path = normalizePathname(event.path || event.url || "/");
  const headers = event.headers || {};
  const qs = event.queryStringParameters || {};
  const pathParams =
    event.pathParameters || event.pathParams || event.params || event.parameters || {};

  const rawBody = event.body || "";
  const decodedBody = rawBody
    ? event.isBase64Encoded
      ? Buffer.from(rawBody, "base64").toString("utf-8")
      : rawBody
    : "";

  const body = decodedBody
    ? (() => {
        try {
          return JSON.parse(decodedBody);
        } catch {
          return null;
        }
      })()
    : null;

  return { method, path, headers, qs, pathParams, body };
}

function getBearer(headers) {
  const h = headers?.Authorization || headers?.authorization || "";
  const m = String(h).match(/^Bearer\s+(.+)$/i);
  return m ? m[1] : "";
}

function verifyToken(token) {
  if (!token || !JWT_SECRET) return null;
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch {
    return null;
  }
}

function signToken(user) {
  if (!JWT_SECRET) throw new Error("JWT_SECRET is required");
  return jwt.sign(
    { user_id: user.user_id, phone: user.phone, name: user.name || "" },
    JWT_SECRET,
    { expiresIn: "30d" }
  );
}

async function getYdb(context) {
  if (ydb) return ydb;
  if (!YDB_DB_NAME) throw new Error("YDB_DB_NAME is required");

  // Cloud Functions: context.token is an object: { access_token, expires_in, token_type }
  const iamToken =
    (context?.token && typeof context.token === "object" ? context.token.access_token : "") ||
    (typeof context?.access_token === "string" ? context.access_token : "") ||
    (typeof context?.token === "string" ? context.token : "") ||
    process.env.YDB_IAM_TOKEN ||
    "";

  if (!iamToken) {
    console.log("No IAM token in context. context.token type =", typeof context?.token);
    throw new Error("No IAM token for YDB");
  }

  // ydb-sdk-lite expects iamToken (no manual 'Bearer ')
  ydb = new Ydb({ dbName: YDB_DB_NAME, iamToken });
  return ydb;
}

function getS3() {
  if (s3) return s3;
  s3 = new S3Client({
    region: AWS_REGION,
    endpoint: S3_ENDPOINT,
    credentials:
      AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY
        ? { accessKeyId: AWS_ACCESS_KEY_ID, secretAccessKey: AWS_SECRET_ACCESS_KEY }
        : undefined,
  });
  return s3;
}

function getSns() {
  if (sns) return sns;
  sns = new SNSClient({
    region: AWS_REGION,
    endpoint: CNS_ENDPOINT,
    credentials:
      AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY
        ? { accessKeyId: AWS_ACCESS_KEY_ID, secretAccessKey: AWS_SECRET_ACCESS_KEY }
        : undefined,
  });
  return sns;
}

async function ensureSchema(context) {
  if (schemaEnsured) return;
  const y = await getYdb(context);

  const yql = `
    CREATE TABLE IF NOT EXISTS ${TP}users (
      user_id Utf8,
      phone Utf8,
      name Utf8,
      created_at Timestamp,
      PRIMARY KEY (user_id)
    );

    CREATE TABLE IF NOT EXISTS ${TP}auth_codes (
      phone Utf8,
      code_hash Utf8,
      expires_at Timestamp,
      created_at Timestamp,
      PRIMARY KEY (phone)
    );

    CREATE TABLE IF NOT EXISTS ${TP}game_stats (
      user_id Utf8,
      game_id Utf8,
      last_stars Int32,
      best_stars Int32,
      last_updated_at Timestamp,
      PRIMARY KEY (user_id, game_id)
    );

    CREATE TABLE IF NOT EXISTS ${TP}saves (
      user_id Utf8,
      game_id Utf8,
      payload_json Utf8,
      updated_at Timestamp,
      PRIMARY KEY (user_id, game_id)
    );

    CREATE TABLE IF NOT EXISTS ${TP}sessions (
      session_id Utf8,
      user_id Utf8,
      game_id Utf8,
      started_at Timestamp,
      finished_at Optional<Timestamp>,
      reason Utf8,
      summary_json Utf8,
      stars_total Int32,
      raw_key Utf8,
      PRIMARY KEY (session_id)
    );
  `;
  try {
    await y.executeYql(yql);
  } catch (e) {
    // If schema already exists or SA cannot create tables, we allow continuing.
    console.log("ensureSchema warning:", String(e?.message || e));
  }
  schemaEnsured = true;
}

// ---- DB helpers (ALL PARAM QUERIES USE DECLARE) ----

async function dbGetUserByPhone(context, phone) {
  const y = await getYdb(context);
  const q = `
    DECLARE $phone AS Utf8;
    SELECT user_id, phone, name, created_at
    FROM ${TP}users
    WHERE phone = $phone
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $phone: phone });
  return rows?.[0] || null;
}

async function dbGetUserById(context, userId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    SELECT user_id, phone, name, created_at
    FROM ${TP}users
    WHERE user_id = $uid
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $uid: userId });
  return rows?.[0] || null;
}

async function dbCreateUser(context, phone) {
  const y = await getYdb(context);
  const user = { user_id: uuidv4(), phone, name: "" };
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $phone AS Utf8;
    DECLARE $name AS Utf8;

    UPSERT INTO ${TP}users (user_id, phone, name, created_at)
    VALUES ($uid, $phone, $name, CurrentUtcTimestamp());
  `;
  await y.executeDataQuery(q, { $uid: user.user_id, $phone: phone, $name: "" });
  return user;
}

async function dbUpdateUserName(context, userId, name) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $name AS Utf8;

    UPDATE ${TP}users
    SET name = $name
    WHERE user_id = $uid;
  `;
  await y.executeDataQuery(q, { $uid: userId, $name: name });
}

async function dbPutAuthCode(context, phone, codeHash, expiresAtIso) {
  const y = await getYdb(context);
  const q = `
    DECLARE $phone AS Utf8;
    DECLARE $hash AS Utf8;
    DECLARE $exp AS Utf8;

    UPSERT INTO ${TP}auth_codes (phone, code_hash, expires_at, created_at)
    VALUES ($phone, $hash, CAST($exp AS Timestamp), CurrentUtcTimestamp());
  `;
  await y.executeDataQuery(q, { $phone: phone, $hash: codeHash, $exp: expiresAtIso });
}

async function dbGetAuthCode(context, phone) {
  const y = await getYdb(context);
  const q = `
    DECLARE $phone AS Utf8;
    SELECT phone, code_hash, expires_at
    FROM ${TP}auth_codes
    WHERE phone = $phone
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $phone: phone });
  return rows?.[0] || null;
}

async function dbDeleteAuthCode(context, phone) {
  const y = await getYdb(context);
  const q = `
    DECLARE $phone AS Utf8;
    DELETE FROM ${TP}auth_codes
    WHERE phone = $phone;
  `;
  await y.executeDataQuery(q, { $phone: phone });
}

async function dbGetStatsAndSaves(context, userId) {
  const y = await getYdb(context);

  const q1 = `
    DECLARE $uid AS Utf8;
    SELECT game_id, last_stars, best_stars
    FROM ${TP}game_stats
    WHERE user_id = $uid;
  `;
  const [statsRows] = await y.executeDataQuery(q1, { $uid: userId });

  const q2 = `
    DECLARE $uid AS Utf8;
    SELECT game_id
    FROM ${TP}saves
    WHERE user_id = $uid;
  `;
  const [saveRows] = await y.executeDataQuery(q2, { $uid: userId });

  const stats = {};
  for (const r of statsRows || []) {
    stats[r.game_id] = {
      last_stars: Number(r.last_stars || 0),
      best_stars: Number(r.best_stars || 0),
    };
  }
  const saves = new Set((saveRows || []).map((r) => r.game_id));
  return { stats, saves };
}

async function dbSaveGet(context, userId, gameId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;

    SELECT payload_json, updated_at
    FROM ${TP}saves
    WHERE user_id = $uid AND game_id = $gid
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $uid: userId, $gid: gameId });
  const r = rows?.[0];
  if (!r) return null;

  let payload = null;
  try {
    payload = JSON.parse(r.payload_json);
  } catch {
    payload = null;
  }
  return { payload, updated_at: r.updated_at };
}

async function dbSavePut(context, userId, gameId, payload) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;
    DECLARE $p AS Utf8;

    UPSERT INTO ${TP}saves (user_id, game_id, payload_json, updated_at)
    VALUES ($uid, $gid, $p, CurrentUtcTimestamp());
  `;
  await y.executeDataQuery(q, {
    $uid: userId,
    $gid: gameId,
    $p: JSON.stringify(payload),
  });
}

async function dbSaveDelete(context, userId, gameId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;

    DELETE FROM ${TP}saves
    WHERE user_id = $uid AND game_id = $gid;
  `;
  await y.executeDataQuery(q, { $uid: userId, $gid: gameId });
}

async function dbSessionStart(context, sessionId, userId, gameId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $sid AS Utf8;
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;

    UPSERT INTO ${TP}sessions (session_id, user_id, game_id, started_at, reason, summary_json, stars_total, raw_key)
    VALUES ($sid, $uid, $gid, CurrentUtcTimestamp(), "", "{}", 0, "");
  `;
  await y.executeDataQuery(q, { $sid: sessionId, $uid: userId, $gid: gameId });
}

async function dbSessionGet(context, sessionId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $sid AS Utf8;
    SELECT session_id, user_id, game_id
    FROM ${TP}sessions
    WHERE session_id = $sid
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $sid: sessionId });
  return rows?.[0] || null;
}

async function dbSessionFinish(context, sessionId, reason, summary, starsTotal, rawKey) {
  const y = await getYdb(context);
  const q = `
    DECLARE $sid AS Utf8;
    DECLARE $reason AS Utf8;
    DECLARE $summary AS Utf8;
    DECLARE $stars AS Int32;
    DECLARE $raw AS Utf8;

    UPDATE ${TP}sessions
    SET finished_at = CurrentUtcTimestamp(),
        reason = $reason,
        summary_json = $summary,
        stars_total = $stars,
        raw_key = $raw
    WHERE session_id = $sid;
  `;
  await y.executeDataQuery(q, {
    $sid: sessionId,
    $reason: reason,
    $summary: JSON.stringify(summary || {}),
    $stars: Math.floor(starsTotal || 0),
    $raw: rawKey || "",
  });
}

async function dbUpsertGameStats(context, userId, gameId, starsTotal) {
  const y = await getYdb(context);

  const q1 = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;

    SELECT last_stars, best_stars
    FROM ${TP}game_stats
    WHERE user_id = $uid AND game_id = $gid
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q1, { $uid: userId, $gid: gameId });
  const cur = rows?.[0];
  const best = Math.max(Number(cur?.best_stars || 0), Math.floor(starsTotal || 0));

  const q2 = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;
    DECLARE $last AS Int32;
    DECLARE $best AS Int32;

    UPSERT INTO ${TP}game_stats (user_id, game_id, last_stars, best_stars, last_updated_at)
    VALUES ($uid, $gid, $last, $best, CurrentUtcTimestamp());
  `;
  await y.executeDataQuery(q2, {
    $uid: userId,
    $gid: gameId,
    $last: Math.floor(starsTotal || 0),
    $best: best,
  });

  return { last_stars: Math.floor(starsTotal || 0), best_stars: best };
}

// ---- integrations ----
async function sendSms(phone, message) {
  if (SMS_MODE === "mock") {
    console.log("[SMS MOCK]", phone, message);
    return { ok: true };
  }
  const client = getSns();
  await client.send(
    new PublishCommand({
      PhoneNumber: phone,
      Message: message,
    })
  );
  return { ok: true };
}

async function uploadRaw(gameId, userId, sessionId, events, summary) {
  if (!LOGS_BUCKET) return { ok: false, error: "LOGS_BUCKET not set" };

  const dt = new Date().toISOString().slice(0, 10);
  const key = `${LOGS_PREFIX}/game=${gameId}/dt=${dt}/user=${userId}/session=${sessionId}.jsonl`;

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
  for (const e of Array.isArray(events) ? events : []) lines.push(JSON.stringify(e));
  lines.push(JSON.stringify({ type: "summary", t: Date.now(), summary: summary || {} }));

  const client = getS3();
  await client.send(
    new PutObjectCommand({
      Bucket: LOGS_BUCKET,
      Key: key,
      Body: Buffer.from(lines.join("\n") + "\n", "utf-8"),
      ContentType: "application/x-ndjson",
    })
  );

  return { ok: true, key };
}

// ---- handler ----
export async function handler(event, context) {
  try {
    const { method, path, headers, body } = parseEvent(event);

    // CORS preflight
    if (method === "OPTIONS") return resp(204, { ok: true });

    await ensureSchema(context);

    // /api/auth/start
    if (method === "POST" && path === "/api/auth/start") {
      const phone = normalizePhone(body?.phone);
      if (!phone) return resp(400, { ok: false, error: "phone_required" });

      const code = SMS_MODE === "mock" ? "0000" : String(Math.floor(1000 + Math.random() * 9000));
      const expiresAt = new Date(Date.now() + 10 * 60 * 1000).toISOString();

      await dbPutAuthCode(context, phone, sha256(code), expiresAt);

      const msg = `Код подтверждения: ${code}`;
      try {
        await sendSms(phone, msg);
      } catch (e) {
        console.log("sendSms error:", String(e?.message || e));
        return resp(500, {
          ok: false,
          error: "sms_failed",
          ...(DEBUG_OTP ? { debug_code: code } : {}),
        });
      }

      return resp(200, { ok: true, ...(DEBUG_OTP ? { debug_code: code } : {}) });
    }

    // /api/auth/verify
    if (method === "POST" && path === "/api/auth/verify") {
      const phone = normalizePhone(body?.phone);
      const code = String(body?.code || "").trim();
      if (!phone || !code) return resp(400, { ok: false, error: "phone_and_code_required" });

      const rec = await dbGetAuthCode(context, phone);
      if (!rec) return resp(401, { ok: false, error: "code_invalid" });

      // expires_at may come as Date, string, or sdk-specific type; String() is acceptable here
      const expMs = Date.parse(String(rec.expires_at));
      if (Number.isFinite(expMs) && expMs < Date.now()) {
        await dbDeleteAuthCode(context, phone);
        return resp(401, { ok: false, error: "code_expired" });
      }

      if (rec.code_hash !== sha256(code)) return resp(401, { ok: false, error: "code_invalid" });

      await dbDeleteAuthCode(context, phone);

      let user = await dbGetUserByPhone(context, phone);
      if (!user) user = await dbCreateUser(context, phone);

      const token = signToken(user);
      return resp(200, { ok: true, token });
    }

    // Auth required below
    const token = getBearer(headers);
    const claims = verifyToken(token);
    if (!claims) return resp(401, { ok: false, error: "unauthorized" });

    const user = await dbGetUserById(context, claims.user_id);
    if (!user) return resp(401, { ok: false, error: "unauthorized" });

    // /api/me GET
    if (method === "GET" && path === "/api/me") {
      const { stats, saves } = await dbGetStatsAndSaves(context, user.user_id);
      const games = GAMES.map((g) => ({
        game_id: g.game_id,
        title: g.title,
        last_stars: stats[g.game_id]?.last_stars || 0,
        best_stars: stats[g.game_id]?.best_stars || 0,
        has_save: saves.has(g.game_id),
      }));
      return resp(200, {
        ok: true,
        user: { user_id: user.user_id, phone: user.phone, name: user.name || "", games },
      });
    }

    // /api/me POST (set name)
    if (method === "POST" && path === "/api/me") {
      const name = String(body?.name || "").trim();
      if (!name) return resp(400, { ok: false, error: "name_required" });
      await dbUpdateUserName(context, user.user_id, name);
      return resp(200, { ok: true });
    }

    // saves: /api/games/{gameId}/save
    {
      const m = path.match(/^\/api\/games\/([^\/]+)\/save$/);
      if (m) {
        const gameId = decodeURIComponent(m[1]);
        if (!GAMES.some((g) => g.game_id === gameId))
          return resp(404, { ok: false, error: "unknown_game" });

        if (method === "GET") {
          const s = await dbSaveGet(context, user.user_id, gameId);
          return resp(200, { ok: true, save: s ? { updated_at: s.updated_at, payload: s.payload } : null });
        }
        if (method === "PUT") {
          const payload = body?.save ?? body?.payload;
          if (!payload || typeof payload !== "object")
            return resp(400, { ok: false, error: "save_required" });
          await dbSavePut(context, user.user_id, gameId, payload);
          return resp(200, { ok: true });
        }
        if (method === "DELETE") {
          await dbSaveDelete(context, user.user_id, gameId);
          return resp(200, { ok: true });
        }
      }
    }

    // session start: /api/games/{gameId}/session/start
    {
      const m = path.match(/^\/api\/games\/([^\/]+)\/session\/start$/);
      if (method === "POST" && m) {
        const gameId = decodeURIComponent(m[1]);
        if (!GAMES.some((g) => g.game_id === gameId))
          return resp(404, { ok: false, error: "unknown_game" });

        const sid = uuidv4();
        await dbSessionStart(context, sid, user.user_id, gameId);
        return resp(200, { ok: true, session_id: sid });
      }
    }

    // session finish: /api/games/{gameId}/session/{sessionId}/finish
    {
      const m = path.match(/^\/api\/games\/([^\/]+)\/session\/([^\/]+)\/finish$/);
      if (method === "POST" && m) {
        const gameId = decodeURIComponent(m[1]);
        const sid = decodeURIComponent(m[2]);
        if (!GAMES.some((g) => g.game_id === gameId))
          return resp(404, { ok: false, error: "unknown_game" });

        // IMPORTANT: verify the session belongs to this user and this game
        const sess = await dbSessionGet(context, sid);
        if (!sess || sess.user_id !== user.user_id || sess.game_id !== gameId) {
          return resp(404, { ok: false, error: "session_not_found" });
        }

        const reason = String(body?.reason || "exit");
        const summary = body?.summary || {};
        const events = Array.isArray(body?.events) ? body.events : [];

        const starsTotal = calcStarsTotalFromSummary(summary);

        const up = await uploadRaw(gameId, user.user_id, sid, events, summary);
        const rawKey = up.ok ? up.key : "";

        await dbSessionFinish(context, sid, reason, summary, starsTotal, rawKey);
        await dbUpsertGameStats(context, user.user_id, gameId, starsTotal);

        return resp(200, { ok: true, raw_key: rawKey, stars_total: starsTotal });
      }
    }

    return resp(404, { ok: false, error: "not_found" });
  } catch (e) {
    console.log("handler error:", e?.stack || e);
    return resp(500, { ok: false, error: "internal_error" });
  }
}