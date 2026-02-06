# \# SCADA MQTT → Neon (TimescaleDB) Backend

# 

# \## Features

# \- Subscribe MQTT (HiveMQ Cloud TLS) and insert telemetry into Postgres/TimescaleDB

# \- Cleanup/retention job

# \- HTTP API:

# &nbsp; - GET /health

# &nbsp; - GET /api/latest?device\_id=...

# &nbsp; - GET /api/history?device\_id=...\&minutes=...

# 

# \## Local run

# 1\) Copy env

# \- cp .env.example .env

# \- fill values

# 

# 2\) Install \& run

# \- npm install

# \- npm start

# 

# \## Render deploy

# \- Build: npm install

# \- Start: npm start

# \- Set env vars in Render (no quotes, no PORT needed)



