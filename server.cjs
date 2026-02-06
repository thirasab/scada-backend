/**
 * server.js — SCADA MQTT -> Postgres (Neon) + HTTP API (Fastify)
 *
 * Features:
 * - Connects to HiveMQ Cloud over MQTT/TLS (mqtts)
 * - Subscribes to telemetry + status topics
 * - Normalizes payload fields (temperature/humidity OR temp_c/hum_pct)
 * - Inserts into Postgres (Neon)
 * - /health endpoint returns db/mqtt status + counters
 * - /api/telemetry/latest + /api/telemetry?device_id=... endpoints
 * - Retention cleanup scheduler
 * - Adds GET/HEAD "/" so Render healthcheck won’t 404
 */

"use strict";

require("dotenv").config();

const fastify = require("fastify")({
  logger: true,
});

const cors = require("@fastify/cors");
const mqtt = require("mqtt");
const { Pool } = require("pg");

// -------------------- Config --------------------
const PORT = Number(process.env.PORT || 10000);

// Postgres (Neon)
const DATABASE_URL = process.env.DATABASE_URL;
const PG_SSL =
  process.env.PG_SSL === "false"
    ? false
    : { rejectUnauthorized: true }; // Neon typically needs SSL

// MQTT
const MQTT_URL =
  process.env.MQTT_URL ||
  process.env.MQTT_HOST
    ? `mqtts://${process.env.MQTT_HOST}:${process.env.MQTT_PORT || 8883}`
    : undefined;

const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;

// Topics
const TOPIC_TELE = process.env.TOPIC_TELE || "cp/test/dht22/telemetry";
const TOPIC_STAT = process.env.TOPIC_STAT || "cp/test/dht22/status";

// Retention
const RETENTION_DAYS = Number(process.env.RETENTION_DAYS || 30);
const CLEANUP_EVERY_MINUTES = Number(process.env.CLEANUP_EVERY_MINUTES || 360);

// -------------------- Runtime status --------------------
let mqttClient = null;
let mqttConnected = false;

let inserted = 0;
let lastMessageAt = null; // ISO string
let lastError = null;

// -------------------- Helpers --------------------
function safeJsonParse(str) {
  try {
    return { ok: true, value: JSON.parse(str) };
  } catch (e) {
    return { ok: false, error: e };
  }
}

function deviceIdFromTopic(topic) {
  // If you ever want to derive device_id from topic segments,
  // do it here. For now, return null.
  return null;
}

/**
 * Normalize telemetry:
 * Supports:
 * - { temperature, humidity }
 * - { temp_c, hum_pct }
 * - { temp, hum }
 */
function normalizeTelemetry(payload, topic) {
  const device_id = payload.device_id || deviceIdFromTopic(topic) || "unknown";

  const temperature = Number(
    payload.temperature ??
      payload.temp_c ??
      payload.temp ??
      payload.t ??
      payload.tempC
  );

  const humidity = Number(
    payload.humidity ?? payload.hum_pct ?? payload.hum ?? payload.h ?? payload.rh
  );

  const dht_ok =
    typeof payload.dht_ok === "boolean"
      ? payload.dht_ok
      : payload.dht_ok == null
        ? null
        : Boolean(payload.dht_ok);

  // ts can be:
  // - epoch seconds (like 1770340028)
  // - epoch ms
  // - ISO string
  let ts = new Date();
  if (payload.ts != null) {
    if (typeof payload.ts === "number") {
      ts = new Date(payload.ts > 1e12 ? payload.ts : payload.ts * 1000);
    } else if (typeof payload.ts === "string") {
      const asNum = Number(payload.ts);
      if (Number.isFinite(asNum)) {
        ts = new Date(asNum > 1e12 ? asNum : asNum * 1000);
      } else {
        const d = new Date(payload.ts);
        if (!Number.isNaN(d.getTime())) ts = d;
      }
    }
  }

  // Optional extras
  const raw = payload;

  const ok =
    Number.isFinite(temperature) && Number.isFinite(humidity) && device_id;

  return {
    ok,
    device_id,
    ts,
    temperature,
    humidity,
    dht_ok,
    raw,
  };
}

// -------------------- DB --------------------
if (!DATABASE_URL) {
  // Hard fail with a clear message (Render logs)
  fastify.log.error("Missing DATABASE_URL env var");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: PG_SSL,
});

async function initDb() {
  await pool.query("SELECT 1;");
  fastify.log.info("DB connected OK");

  // Create table if not exists
  await pool.query(`
    CREATE TABLE IF NOT EXISTS telemetry (
      id BIGSERIAL PRIMARY KEY,
      device_id TEXT NOT NULL,
      ts TIMESTAMPTZ NOT NULL DEFAULT now(),
      temperature DOUBLE PRECISION,
      humidity DOUBLE PRECISION,
      dht_ok BOOLEAN,
      raw JSONB
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS telemetry_device_ts_idx
    ON telemetry (device_id, ts DESC);
  `);
}

async function insertTelemetry(row) {
  const q = `
    INSERT INTO telemetry (device_id, ts, temperature, humidity, dht_ok, raw)
    VALUES ($1, $2, $3, $4, $5, $6)
  `;
  const vals = [
    row.device_id,
    row.ts,
    row.temperature,
    row.humidity,
    row.dht_ok,
    row.raw,
  ];
  await pool.query(q, vals);
  inserted += 1;
}

async function cleanupOldTelemetry() {
  const q = `
    DELETE FROM telemetry
    WHERE ts < (now() - ($1 || ' days')::interval)
  `;
  const res = await pool.query(q, [RETENTION_DAYS]);
  fastify.log.info(
    { deleted: res.rowCount, retention_days: RETENTION_DAYS },
    "Cleanup old telemetry done"
  );
}

function startCleanupScheduler() {
  // Run once on boot
  cleanupOldTelemetry().catch((e) => {
    lastError = `cleanup: ${e.message}`;
    fastify.log.error(e, "Cleanup failed");
  });

  const ms = CLEANUP_EVERY_MINUTES * 60 * 1000;
  setInterval(() => {
    cleanupOldTelemetry().catch((e) => {
      lastError = `cleanup: ${e.message}`;
      fastify.log.error(e, "Cleanup failed");
    });
  }, ms);

  fastify.log.info(
    { every_minutes: CLEANUP_EVERY_MINUTES, retention_days: RETENTION_DAYS },
    "Cleanup scheduler started"
  );
}

// -------------------- MQTT --------------------
function initMqtt() {
  if (!MQTT_URL) {
    fastify.log.error(
      "Missing MQTT_URL (or MQTT_HOST/MQTT_PORT). MQTT will not start."
    );
    return;
  }

  mqttClient = mqtt.connect(MQTT_URL, {
    username: MQTT_USER,
    password: MQTT_PASS,
    reconnectPeriod: 2000,
    keepalive: 30,
    // For HiveMQ Cloud, mqtt.js uses Node TLS under the hood.
    // If you ever need custom CA, you can pass `ca: fs.readFileSync(...)`.
  });

  mqttClient.on("connect", () => {
    mqttConnected = true;
    fastify.log.info({ msg: `MQTT connected: ${MQTT_URL}` });

    mqttClient.subscribe(TOPIC_TELE, { qos: 0 }, (err) => {
      if (err) {
        lastError = `mqtt subscribe telemetry: ${err.message}`;
        fastify.log.error(err, "Subscribe telemetry failed");
      } else {
        fastify.log.info({ msg: `Subscribed telemetry: ${TOPIC_TELE}` });
      }
    });

    mqttClient.subscribe(TOPIC_STAT, { qos: 0 }, (err) => {
      if (err) {
        lastError = `mqtt subscribe status: ${err.message}`;
        fastify.log.error(err, "Subscribe status failed");
      } else {
        fastify.log.info({ msg: `Subscribed status: ${TOPIC_STAT}` });
      }
    });
  });

  mqttClient.on("reconnect", () => {
    mqttConnected = false;
    fastify.log.warn("MQTT reconnecting...");
  });

  mqttClient.on("close", () => {
    mqttConnected = false;
    fastify.log.warn("MQTT connection closed");
  });

  mqttClient.on("error", (err) => {
    mqttConnected = false;
    lastError = `mqtt: ${err.message}`;
    fastify.log.error(err, "MQTT error");
  });

  mqttClient.on("message", async (topic, messageBuf) => {
    const text = messageBuf.toString("utf8");
    lastMessageAt = new Date().toISOString();

    // STATUS
    if (topic === TOPIC_STAT) {
      fastify.log.info({ topic, text }, "Status message");
      return;
    }

    // TELEMETRY
    if (topic === TOPIC_TELE) {
      const parsed = safeJsonParse(text);
      if (!parsed.ok) {
        lastError = `telemetry not json: ${parsed.error.message}`;
        fastify.log.warn({ topic, text }, "Telemetry not JSON / parse failed");
        return;
      }

      const norm = normalizeTelemetry(parsed.value, topic);
      if (!norm.ok) {
        lastError = "telemetry missing temperature/humidity/device_id";
        fastify.log.warn(
          { topic, payload: parsed.value },
          "Telemetry missing required fields; skip insert"
        );
        return;
      }

      try {
        await insertTelemetry(norm);
        // Uncomment if you want very chatty logs:
        // fastify.log.info({ device_id: norm.device_id, temperature: norm.temperature, humidity: norm.humidity }, "Telemetry inserted");
      } catch (e) {
        lastError = `db insert: ${e.message}`;
        fastify.log.error(e, "DB insert failed");
      }
      return;
    }

    // Other topics (if any)
    fastify.log.debug({ topic, text }, "MQTT message ignored");
  });
}

// -------------------- HTTP API --------------------
async function initHttp() {
  await fastify.register(cors, { origin: true });

  // Root routes to avoid Render HEAD / 404
  fastify.get("/", async () => {
    return { ok: true, service: "scada-backend", ts: new Date().toISOString() };
  });
  fastify.head("/", async (_req, reply) => reply.code(200).send());

  fastify.get("/health", async () => {
    // quick db check without heavy load
    let db = "ok";
    try {
      await pool.query("SELECT 1;");
    } catch (e) {
      db = "down";
      lastError = `db health: ${e.message}`;
    }

    return {
      ok: true,
      time: new Date().toISOString(),
      db,
      mqtt: mqttConnected ? "connected" : "disconnected",
      lastMessageAt,
      inserted,
      lastError,
      topics: { telemetry: TOPIC_TELE, status: TOPIC_STAT },
      retention_days: RETENTION_DAYS,
    };
  });

  // Latest per device (or all devices)
  fastify.get("/api/telemetry/latest", async (req) => {
    const device_id = req.query.device_id;

    if (device_id) {
      const { rows } = await pool.query(
        `
        SELECT device_id, ts, temperature, humidity, dht_ok, raw
        FROM telemetry
        WHERE device_id = $1
        ORDER BY ts DESC
        LIMIT 1
      `,
        [device_id]
      );
      return { device_id, latest: rows[0] || null };
    }

    // Latest row per device
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (device_id)
        device_id, ts, temperature, humidity, dht_ok, raw
      FROM telemetry
      ORDER BY device_id, ts DESC
    `);
    return { latest: rows };
  });

  // Query time range
  fastify.get("/api/telemetry", async (req) => {
    const device_id = req.query.device_id;
    const limit = Math.min(Number(req.query.limit || 200), 2000);
    const from = req.query.from ? new Date(req.query.from) : null;
    const to = req.query.to ? new Date(req.query.to) : null;

    const where = [];
    const params = [];

    if (device_id) {
      params.push(device_id);
      where.push(`device_id = $${params.length}`);
    }
    if (from && !Number.isNaN(from.getTime())) {
      params.push(from);
      where.push(`ts >= $${params.length}`);
    }
    if (to && !Number.isNaN(to.getTime())) {
      params.push(to);
      where.push(`ts <= $${params.length}`);
    }

    params.push(limit);

    const sql = `
      SELECT device_id, ts, temperature, humidity, dht_ok
      FROM telemetry
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY ts DESC
      LIMIT $${params.length}
    `;

    const { rows } = await pool.query(sql, params);
    return { count: rows.length, rows };
  });
}

// -------------------- Boot --------------------
async function main() {
  try {
    await initDb();
    startCleanupScheduler();
    await initHttp();
    initMqtt();

    await fastify.listen({ port: PORT, host: "0.0.0.0" });
    fastify.log.info(`HTTP listening on :${PORT}`);
  } catch (e) {
    lastError = e.message;
    fastify.log.error(e, "Fatal startup error");
    process.exit(1);
  }
}

main();

// Graceful shutdown
process.on("SIGTERM", async () => {
  try {
    fastify.log.info("SIGTERM received: closing...");
    if (mqttClient) mqttClient.end(true);
    await fastify.close();
    await pool.end();
  } catch (e) {
    // ignore
  } finally {
    process.exit(0);
  }
});
