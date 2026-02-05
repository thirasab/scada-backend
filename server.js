import 'dotenv/config';
import Fastify from 'fastify';
import mqtt from 'mqtt';
import pg from 'pg';

const {
  DATABASE_URL,
  MQTT_HOST,
  MQTT_PORT = '8883',
  MQTT_USERNAME,
  MQTT_PASSWORD,
  MQTT_TELE_TOPIC,
  MQTT_STAT_TOPIC,
  DEVICE_ID_DEFAULT = 'default',
  PORT = '3000',

  // ✅ retention / cleanup
  RETENTION_DAYS = '30',
  CLEANUP_EVERY_MINUTES = '360',
} = process.env;

if (!DATABASE_URL) throw new Error('Missing DATABASE_URL');
if (!MQTT_HOST) throw new Error('Missing MQTT_HOST');
if (!MQTT_USERNAME || !MQTT_PASSWORD) throw new Error('Missing MQTT_USERNAME/MQTT_PASSWORD');
if (!MQTT_TELE_TOPIC) throw new Error('Missing MQTT_TELE_TOPIC');

const { Pool } = pg;
const pool = new Pool({
  connectionString: DATABASE_URL,
  // Neon ต้องใช้ SSL
  ssl: { rejectUnauthorized: false },
  max: 5,
});

const fastify = Fastify({ logger: true });

function nowIso() {
  return new Date().toISOString();
}

// ดึง device_id จาก topic แบบง่าย ๆ (ปรับได้)
function guessDeviceIdFromTopic(topic) {
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

    let ts = new Date(); // default now
    if (j.ts != null) {
      const n = Number(j.ts);
      if (Number.isFinite(n)) {
        ts = new Date(n < 1e12 ? n * 1000 : n);
      }
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
  const days = Math.max(1, Math.min(3650, Number(RETENTION_DAYS || 30)));
  const q = `
    DELETE FROM telemetry_raw
    WHERE ts < now() - ($1::text || ' days')::interval
  `;
  const r = await pool.query(q, [String(days)]);
  fastify.log.info(
    { deleted: r.rowCount, retention_days: days },
    'Cleanup old telemetry done'
  );
}

function scheduleCleanup() {
  const everyMin = Math.max(5, Math.min(24 * 60, Number(CLEANUP_EVERY_MINUTES || 360)));

  // run once on startup
  cleanupOldData().catch((e) => fastify.log.error(e, 'Cleanup failed'));

  setInterval(() => {
    cleanupOldData().catch((e) => fastify.log.error(e, 'Cleanup failed'));
  }, everyMin * 60 * 1000);

  fastify.log.info(
    { every_minutes: everyMin, retention_days: Number(RETENTION_DAYS || 30) },
    'Cleanup scheduler started'
  );
}

/* =========================
   MQTT Client
   ========================= */
const mqttUrl = `mqtts://${MQTT_HOST}:${MQTT_PORT}`;
const mqttClient = mqtt.connect(mqttUrl, {
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,
  clean: true,
  reconnectPeriod: 2000,
  connectTimeout: 10000,
  keepalive: 30,
});

let lastStatus = {
  mqtt: 'disconnected',
  lastMessageAt: null,
  lastError: null,
  inserted: 0,
};

mqttClient.on('connect', async () => {
  lastStatus.mqtt = 'connected';
  lastStatus.lastError = null;
  fastify.log.info(`MQTT connected: ${mqttUrl}`);

  mqttClient.subscribe(MQTT_TELE_TOPIC, { qos: 0 }, (err) => {
    if (err) fastify.log.error(err, 'Subscribe telemetry failed');
    else fastify.log.info(`Subscribed telemetry: ${MQTT_TELE_TOPIC}`);
  });

  if (MQTT_STAT_TOPIC) {
    mqttClient.subscribe(MQTT_STAT_TOPIC, { qos: 0 }, (err) => {
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

  const text = payload.toString();

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

async function start() {
  await pool.query('SELECT now()');
fastify.log.info('DB connected OK');

  // ✅ เริ่ม job ลบข้อมูลเก่า
  scheduleCleanup();

  await fastify.listen({ port: Number(PORT), host: '0.0.0.0' });
  fastify.log.info(`HTTP listening on :${PORT}`);
}

start().catch((err) => {
  fastify.log.error(err);
  process.exit(1);
});

// graceful shutdown
process.on('SIGINT', async () => {
  try {
    mqttClient.end(true);
    await pool.end();
    await fastify.close();
  } finally {
    process.exit(0);
  }
});
