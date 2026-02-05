// server.js (ESM)
// ✅ Render + Neon (TimescaleDB) + HiveMQ Cloud (MQTTS) + Fastify
// ✅ MQTT optional (ไม่ล้มถ้า env ยังไม่ครบ) ด้วย MQTT_ENABLED=false
// ✅ ใช้ Render PORT (process.env.PORT) อัตโนมัติ
// ✅ Cleanup/Retention ตาม RETENTION_DAYS + CLEANUP_EVERY_MINUTES

import 'dotenv/config';
import Fastify from 'fastify';
import mqtt from 'mqtt';
import pg from 'pg';

/* =========================
   Environment
   ========================= */
const DATABASE_URL = process.env.DATABASE_URL;

// Render จะตั้ง PORT ให้เอง
const HTTP_PORT = Number(process.env.PORT || 3000);

const DEVICE_ID_DEFAULT = process.env.DEVICE_ID_DEFAULT || 'default';

// Retention / Cleanup
const RETENTION_DAYS = Number(process.env.RETENTION_DAYS || 30);
const CLEANUP_EVERY_MINUTES = Number(process.env.CLEANUP_EVERY_MINUTES || 360);

// MQTT (HiveMQ Cloud)
const MQTT_ENABLED = process.env.MQTT_ENABLED !== 'false'; // default: true
const MQTT_HOST = process.env.MQTT_HOST;
const MQTT_PORT = Number(process.env.MQTT_PORT || 8883);
const MQTT_USERNAME = process.env.MQTT_USERNAME;
const MQTT_PASSWORD = process.env.MQTT_PASSWORD;
const MQTT_TELE_TOPIC = process.env.MQTT_TELE_TOPIC; // e.g. cp/test/dht22/telemetry or cp/test/+/telemetry
const MQTT_STAT_TOPIC = process.env.MQTT_STAT_TOPIC; // optional

if (!DATABASE_URL) throw new Error('Missing DATABASE_URL');

/* =========================
   DB (Neon Postgres / TimescaleDB)
   ========================= */
const { Pool } = pg;

// NOTE: เพื่อไม่ให้ deploy ล้มเพราะ SSL verify ในบาง environment,
// ค่า default ใช้ rejectUnauthorized=false (เหมือนที่คุณใช้เดิม)
// ถ้าคุณอยาก “เข้มขึ้น” ให้ตั้ง env: PG_SSL_REJECT_UNAUTHORIZED=true
const PG_SSL_REJECT_UNAUTHORIZED = String(process.env.PG_SSL_REJECT_UNAUTHORIZED || 'false') === 'true';

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: PG_SSL_REJECT_UNAUTHORIZED },
  max: 5,
});

/* =========================
   Fastify
   ========================= */
const fastify = Fastify({ logger: true });

function nowIso() {
  return new Date().toISOString();
}

/* =========================
   Helpers
   ========================= */
function guessDeviceIdFromTopic(topic) {
  // รองรับรูปแบบทั่วไป:
  // cp/<site>/<device>/telemetry  => device
  // cp/test/dht22/telemetry       => dht22
  const parts = String(topic || '').split('/').filter(Boolean);
  if (parts.length >= 2) return parts[parts.length - 2];
  return DEVICE_ID_DEFAULT;
}

function parseTelemetry(payloadText) {
  try {
    const j = JSON.parse(payloadText);

    const tempRaw = j.temp_c ?? j.temp ?? null;
    const humRaw = j.hum_pct ?? j.hum ?? null;

    const temp_c = Number.isFinite(Number(tempRaw)) ? Number(tempRaw) : null;
    const hum_pct = Number.isFinite(Number(humRaw)) ? Number(humRaw) : null;

    // timestamp: รองรับ epoch sec/ms
    let ts = new Date();
    if (j.ts != null) {
      const n = Number(j.ts);
      if (Number.isFinite(n)) ts = new Date(n < 1e12 ? n * 1000 : n);
    }

    return { ok: true, ts, temp_c, hum_pct, raw: j };
  } catch {
    return { ok: false };
  }
}

async function insertTelemetry({ ts, device_id, temp_c, hum_pct, topic, raw_json }) {
  const q = `
    INSERT INTO telemetry_raw (ts, device_id, temp_c, hum_pct, topic, raw_json)
    VALUES ($1, $2, $3, $4, $5, $6)
  `;
  const values = [ts, device_id, temp_c, hum_pct, topic, raw_json];
  await pool.query(q, values);
}

/* =========================
   ✅ Cleanup / Retention
   ========================= */
async function cleanupOldData() {
  const days = Math.max(1, Math.min(3650, Number.isFinite(RETENTION_DAYS) ? RETENTION_DAYS : 30));
  const q = `
    DELETE FROM telemetry_raw
    WHERE ts < now() - ($1::text || ' days')::interval
  `;
  const r = await pool.query(q, [String(days)]);
  fastify.log.info({ deleted: r.rowCount, retention_days: days }, 'Cleanup old telemetry done');
}

function scheduleCleanup() {
  const everyMin = Math.max(
    5,
    Math.min(24 * 60, Number.isFinite(CLEANUP_EVERY_MINUTES) ? CLEANUP_EVERY_MINUTES : 360)
  );

  cleanupOldData().catch((e) => fastify.log.error(e, 'Cleanup failed'));
  setInterval(() => {
    cleanupOldData().catch((e) => fastify.log.error(e, 'Cleanup failed'));
  }, everyMin * 60 * 1000);

  fastify.log.info(
    { every_minutes: everyMin, retention_days: Number.isFinite(RETENTION_DAYS) ? RETENTION_DAYS : 30 },
    'Cleanup scheduler started'
  );
}

/* =========================
   MQTT Client (optional)
   ========================= */
let mqttClient = null;

const lastStatus = {
  mqtt: 'disabled',
  lastMessageAt: null,
  lastError: null,
  inserted: 0,
};

function startMqttIfConfigured() {
  if (!MQTT_ENABLED) {
    lastStatus.mqtt = 'disabled';
    fastify.log.warn('MQTT disabled by MQTT_ENABLED=false');
    return;
  }

  const missing = [];
  if (!MQTT_HOST) missing.push('MQTT_HOST');
  if (!MQTT_USERNAME) missing.push('MQTT_USERNAME');
  if (!MQTT_PASSWORD) missing.push('MQTT_PASSWORD');
  if (!MQTT_TELE_TOPIC) missing.push('MQTT_TELE_TOPIC');

  if (missing.length) {
    lastStatus.mqtt = 'disabled';
    fastify.log.warn({ missing }, 'MQTT disabled due to missing env');
    return;
  }

  const mqttUrl = `mqtts://${MQTT_HOST}:${MQTT_PORT}`;

  mqttClient = mqtt.connect(mqttUrl, {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
    clean: true,
    reconnectPeriod: 2000,
    connectTimeout: 10000,
    keepalive: 30,
    // ปล่อยให้ Node ตรวจ cert ตามปกติ (ปลอดภัยกว่า)
    rejectUnauthorized: true,
  });

  lastStatus.mqtt = 'connecting';

  mqttClient.on('connect', () => {
    lastStatus.mqtt = 'connected';
    lastStatus.lastError = null;
    fastify.log.info(`MQTT connected: ${mqttUrl}`);

    mqttClient.subscribe(MQTT_TELE_TOPIC, { qos: 1 }, (err) => {
      if (err) fastify.log.error(err, 'Subscribe telemetry failed');
      else fastify.log.info(`Subscribed telemetry: ${MQTT_TELE_TOPIC}`);
    });

    if (MQTT_STAT_TOPIC) {
      mqttClient.subscribe(MQTT_STAT_TOPIC, { qos: 1 }, (err) => {
        if (err) fastify.log.error(err, 'Subscribe status failed');
        else fastify.log.info(`Subscribed status: ${MQTT_STAT_TOPIC}`);
      });
    }
  });

  mqttClient.on('reconnect', () => {
    lastStatus.mqtt = 'reconnecting';
  });

  mqttClient.on('close', () => {
    lastStatus.mqtt = 'disconnected';
  });

  mqttClient.on('error', (err) => {
    lastStatus.mqtt = 'error';
    lastStatus.lastError = err?.message || String(err);
    fastify.log.error(err, 'MQTT error');
  });

  mqttClient.on('message', async (topic, payload) => {
    lastStatus.lastMessageAt = nowIso();
    const text = payload.toString('utf8');

    if (MQTT_STAT_TOPIC && topic === MQTT_STAT_TOPIC) {
      fastify.log.info({ topic, text }, 'Status message');
      return;
    }

    const parsed = parseTelemetry(text);
    if (!parsed.ok) {
      fastify.log.warn({ topic, text }, 'Telemetry not JSON / parse failed');
      return;
    }

    const device_id = guessDeviceIdFromTopic(topic);

    try {
      await insertTelemetry({
        ts: parsed.ts,
        device_id,
        temp_c: parsed.temp_c,
        hum_pct: parsed.hum_pct,
        topic,
        raw_json: parsed.raw,
      });
      lastStatus.inserted++;
    } catch (err) {
      lastStatus.lastError = err?.message || String(err);
      fastify.log.error(err, 'DB insert failed');
    }
  });
}

/* =========================
   HTTP API
   ========================= */
fastify.get('/health', async () => {
  const r = await pool.query('SELECT 1 as ok');
  return {
    ok: true,
    time: nowIso(),
    db: r.rows?.[0]?.ok === 1 ? 'ok' : 'unknown',
    mqtt: lastStatus.mqtt,
    lastMessageAt: lastStatus.lastMessageAt,
    inserted: lastStatus.inserted,
    lastError: lastStatus.lastError,
  };
});

fastify.get('/api/latest', async (req) => {
  const device_id = req.query?.device_id || DEVICE_ID_DEFAULT;

  const q = `
    SELECT ts, device_id, temp_c, hum_pct, topic
    FROM telemetry_raw
    WHERE device_id = $1
    ORDER BY ts DESC
    LIMIT 1
  `;
  const r = await pool.query(q, [device_id]);
  return r.rows[0] || null;
});

fastify.get('/api/history', async (req) => {
  const device_id = req.query?.device_id || DEVICE_ID_DEFAULT;
  const minutes = Math.max(1, Math.min(24 * 60, Number(req.query?.minutes || 60)));

  const q = `
    SELECT ts, temp_c, hum_pct
    FROM telemetry_raw
    WHERE device_id = $1
      AND ts >= now() - ($2::text || ' minutes')::interval
    ORDER BY ts ASC
  `;
  const r = await pool.query(q, [device_id, String(minutes)]);
  return r.rows;
});

/* =========================
   Start
   ========================= */
async function start() {
  await pool.query('SELECT now()');
  fastify.log.info('DB connected OK');

  scheduleCleanup();
  startMqttIfConfigured();

  await fastify.listen({ port: HTTP_PORT, host: '0.0.0.0' });
  fastify.log.info(`HTTP listening on :${HTTP_PORT}`);
}

start().catch((err) => {
  fastify.log.error(err);
  process.exit(1);
});

/* =========================
   Graceful shutdown
   ========================= */
async function shutdown() {
  try {
    if (mqttClient) mqttClient.end(true);
    await pool.end();
    await fastify.close();
  } finally {
    process.exit(0);
  }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
