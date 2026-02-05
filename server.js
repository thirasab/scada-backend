"use strict";

/**
 * server.js — SCADA MQTT -> Neon(Postgres)
 * - Subscribes:
 *    cp/test/dht22/telemetry  (JSON)
 *    cp/test/dht22/status     (text or JSON)
 * - Inserts telemetry into Postgres
 * - Exposes:
 *    GET/HEAD /
 *    GET /health
 *    GET /telemetry/latest?device_id=...
 *    GET /telemetry?device_id=...&limit=200
 */

// ===== BOOT MARK (use to confirm Render runs THIS file) =====
console.log("BOOT MARK: server.js v2026-02-05-telemetry-fix");

const fastify = require("fastify")({
  logger: { level: process.env.LOG_LEVEL || "info" },
});

const mqtt = require("mqtt");
const { Pool } = require("pg");

/* ================== CONFIG ================== */
const PORT = Number(process.env.PORT || 10000);

// DB
const DATABASE_URL = process.env.DATABASE_URL;

// MQTT
const MQTT_HOST = process.env.MQTT_HOST; // e.g. 9b6e...hivemq.cloud
const MQTT_PORT = Number(process.env.MQTT_PORT || 8883);
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;

const TOPIC_TELE = process.env.TOPIC_TELE || "cp/test/dht22/telemetry";
const TOPIC_STAT = process.env.TOPIC_STAT || "cp/test/dht22/status";

// Retention / cleanup
const RETENTION_DAYS = Number(process.env.RETENTION_DAYS || 30);
const CLEANUP_EVERY_MINUTES = Number(process.env.CLEANUP_EVERY_MINUTES || 360);

/* ================== STATE ================== */
let mqttClient = null;
let lastMessageAt = null;
let lastError = null;
let inserted = 0;

const lastStatusByDevice = new Map(); // device_id -> { status, at }

/* ================== HELPERS ================== */
function nowIso() {
  return new Date().toISOString();
}

function parseJsonSafe(text) {
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (e) {
    return { ok: false, error: e };
  }
}

function isFiniteNumber(x) {
  return typeof x === "number" && Number.isFinite(x);
}

function normalizeTelemetry(data, fallbackDeviceId = "unknown") {
  // Accept schemas:
  // ESP32: { device_id, ts, dht_ok, temp_c, hum_pct }
  // Generic: { device_id, temperature, humidity, timestamp, ok }

  const device_id = data.device_id || fallbackDeviceId;

  const temperature = data.temp_c ?? data.temperature ?? data.temp ?? data.t;
  const humidity = data.hum_pct ?? data.humidity ?? data.hum ?? data.h;

  const dht_ok = data.dht_ok ?? data.ok ?? null;

  // ts can be epoch seconds (10 digits) or epoch ms (13 digits) or ISO string
  let at = new Date();
  const tsNum =
    typeof data.ts === "number"
      ? data.ts
      : typeof data.timestamp === "number"
        ? data.timestamp
        : null;

  if (typeof tsNum === "number") {
    at = tsNum < 1e12 ? new Date(tsNum * 1000) : new Date(tsNum);
  } else if (typeof data.ts === "string") {
    const d = new Date(data.ts);
    if (!Number.isNaN(d.getTime())) at = d;
  } else if (typeof data.at === "string") {
    const d = new Date(data.at);
    if (!Number.isNaN(d.getTime())) at = d;
  }

  return {
    device_id,
    at,
    temperature,
    humidity,
    dht_ok,
    raw: data,
  };
}

/* ================== DB ================== */
if (!DATABASE_URL) {
  // keep this early and obvious
  console.error("Missing DATABASE_URL");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  // Neon requires SSL; rejectUnauthorized false is common for managed PG.
  // If you already include sslmode=verify-full in DATABASE_URL, this is fine too.
  ssl: { rejectUnauthorized: false },
});

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS telemetry (
      id BIGSERIAL PRIMARY KEY,
      device_id TEXT NOT NULL,
      at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      temp_c DOUBLE PRECISION NOT NULL,
      hum_pct DOUBLE PRECISION NOT NULL,
      dht_ok BOOLEAN,
      raw JSONB
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS telemetry_device_at_idx
    ON telemetry (device_id, at DESC);
  `);
}

async function insertTelemetry(row) {
  const q = `
    INSERT INTO telemetry (device_id, at, temp_c, hum_pct, dht_ok, raw)
    VALUES ($1, $2, $3, $4, $5, $6)
  `;
  const params = [
    row.device_id,
    row.at,
    row.temperature,
    row.humidity,
    row.dht_ok,
    row.raw,
  ];
  await pool.query(q, params);
  inserted += 1;
}

async function cleanupOldTelemetry() {
  const q = `
    DELETE FROM telemetry
    WHERE at < NOW() - ($1 || ' days')::interval
  `;
  const res = await pool.query(q, [String(RETENTION_DAYS)]);
  fastify.log.info(
    { deleted: res.rowCount, retention_days: RETENTION_DAYS },
    "Cleanup old telemetry done"
  );
}

/* ================== MQTT ================== */
function connectMqtt() {
  if (!MQTT_HOST) throw new Error("Missing MQTT_HOST");
  if (!MQTT_USER) throw new Error("Missing MQTT_USER");
  if (!MQTT_PASS) throw new Error("Missing MQTT_PASS");

  const url = `mqtts://${MQTT_HOST}:${MQTT_PORT}`;

  mqttClient = mqtt.connect(url, {
    username: MQTT_USER,
    password: MQTT_PASS,
    keepalive: 60,
    reconnectPeriod: 3000,
    connectTimeout: 30_000,
  });

  mqttClient.on("connect", () => {
    fastify.log.info({ msg: `MQTT connected: ${url}` });

    mqttClient.subscribe(TOPIC_TELE, { qos: 0 }, (err) => {
      if (err) {
        lastError = `Subscribe telemetry failed: ${err.message}`;
        fastify.log.error({ err }, "Subscribe telemetry failed");
      } else {
        fastify.log.info({ msg: `Subscribed telemetry: ${TOPIC_TELE}` });
      }
    });

    mqttClient.subscribe(TOPIC_STAT, { qos: 0 }, (err) => {
      if (err) {
        lastError = `Subscribe status failed: ${err.message}`;
        fastify.log.error({ err }, "Subscribe status failed");
      } else {
        fastify.log.info({ msg: `Subscribed status: ${TOPIC_STAT}` });
      }
    });
  });

  mqttClient.on("reconnect", () => {
    fastify.log.warn("MQTT reconnecting...");
  });

  mqttClient.on("error", (err) => {
    lastError = `MQTT error: ${err.message}`;
    fastify.log.error({ err }, "MQTT error");
  });

  mqttClient.on("message", async (topic, message) => {
    lastMessageAt = nowIso();

    // STATUS
    if (topic === TOPIC_STAT) {
      const text = message.toString("utf8").trim();

      // Accept status as either plain text "online" or JSON {"device_id":"...","status":"online"}
      const parsed = parseJsonSafe(text);
      if (parsed.ok && parsed.value && typeof parsed.value === "object") {
        const device_id = parsed.value.device_id || "unknown";
        const status = parsed.value.status || parsed.value.state || text;
        lastStatusByDevice.set(device_id, { status: String(status), at: new Date() });
        fastify.log.info({ topic, device_id, status }, "Status message (json)");
      } else {
        const device_id = "unknown";
        lastStatusByDevice.set(device_id, { status: text, at: new Date() });
        fastify.log.info({ topic, text }, "Status message");
      }
      return;
    }

    // TELEMETRY
    if (topic === TOPIC_TELE) {
      const text = message.toString("utf8").trim();

      const parsed = parseJsonSafe(text);
      if (!parsed.ok) {
        lastError = `Telemetry not JSON: ${parsed.error.message}`;
        fastify.log.warn({ topic, text }, "Telemetry not JSON");
        return;
      }

      const row = normalizeTelemetry(parsed.value, "unknown");

      if (!isFiniteNumber(row.temperature) || !isFiniteNumber(row.humidity)) {
        lastError = "Telemetry missing temp/hum (number)";
        fastify.log.warn({ topic, data: parsed.value }, "Telemetry missing temp/hum");
        return;
      }

      try {
        await insertTelemetry(row);
        fastify.log.info(
          {
            device_id: row.device_id,
            at: row.at.toISOString(),
            temp_c: row.temperature,
            hum_pct: row.humidity,
            inserted,
          },
          "Telemetry inserted"
        );
      } catch (err) {
        lastError = `DB insert failed: ${err.message}`;
        fastify.log.error({ err, topic, data: parsed.value }, "DB insert failed");
      }
    }
  });
}

/* ================== ROUTES ================== */

// Prevent Render health/HEAD probes from spamming 404
fastify.head("/", async (_req, reply) => {
  reply.code(200).send();
});

fastify.get("/", async () => {
  return {
    ok: true,
    service: "scada-backend",
    time: nowIso(),
    routes: ["/health", "/telemetry/latest", "/telemetry"],
  };
});

fastify.get("/health", async () => {
  let db = "unknown";
  try {
    await pool.query("SELECT 1");
    db = "ok";
  } catch (e) {
    db = "error";
    lastError = `DB health failed: ${e.message}`;
  }

  const mqttState = mqttClient && mqttClient.connected ? "connected" : "disconnected";

  return {
    ok: true,
    time: nowIso(),
    db,
    mqtt: mqttState,
    lastMessageAt,
    inserted,
    lastError,
  };
});

fastify.get("/telemetry/latest", async (req) => {
  const device_id = (req.query.device_id || "dht22-01").toString();

  const { rows } = await pool.query(
    `SELECT device_id, at, temp_c, hum_pct, dht_ok
     FROM telemetry
     WHERE device_id = $1
     ORDER BY at DESC
     LIMIT 1`,
    [device_id]
  );

  return { device_id, latest: rows[0] || null };
});

fastify.get("/telemetry", async (req) => {
  const device_id = (req.query.device_id || "dht22-01").toString();
  const limit = Math.max(1, Math.min(500, Number(req.query.limit || 200)));

  const { rows } = await pool.query(
    `SELECT device_id, at, temp_c, hum_pct, dht_ok
     FROM telemetry
     WHERE device_id = $1
     ORDER BY at DESC
     LIMIT $2`,
    [device_id, limit]
  );

  return { device_id, count: rows.length, rows };
});

/* ================== BOOT ================== */
async function main() {
  if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

  await ensureSchema();
  fastify.log.info("DB connected OK");

  // Cleanup scheduler
  setInterval(() => {
    cleanupOldTelemetry().catch((e) => {
      lastError = `Cleanup failed: ${e.message}`;
      fastify.log.error({ err: e }, "Cleanup failed");
    });
  }, CLEANUP_EVERY_MINUTES * 60 * 1000);

  fastify.log.info(
    { every_minutes: CLEANUP_EVERY_MINUTES, retention_days: RETENTION_DAYS },
    "Cleanup scheduler started"
  );

  // Run cleanup once on boot
  cleanupOldTelemetry().catch(() => {});

  connectMqtt();

  await fastify.listen({ port: PORT, host: "0.0.0.0" });
  fastify.log.info(`HTTP listening on :${PORT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
