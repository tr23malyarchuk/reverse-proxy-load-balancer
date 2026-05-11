# Reverse Proxy Load Balancer

> A load balancing system for file conversion services with dynamic scaling between servers using a reverse proxy

## 📝 Description

This project demonstrates efficient request distribution across multiple backend servers in real time. The system includes a web interface for file conversion (WAV -> MP3, PDF -> PNG, WEBP -> PNG, RAR -> ZIP), automatic horizontal container scaling based on CPU usage, and an admin panel for monitoring. A fully functional load balancer with 4 dynamic containers and Docker API integration was implemented.

## ✨ Features

- Dynamic container autoscaling based on CPU metrics
- Horizontal scaling via Docker API
- Multiple load balancing strategies
- Real-time system monitoring
- Microservices-based file conversion ecosystem
- Docker orchestration
- SQLite analytics

## 🛠 Technologies

<p align="left">
  <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/>
  <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/react/react-original.svg" alt="react" width="40" height="40"/>
  <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/nodejs/nodejs-original.svg" alt="nodejs" width="40" height="40"/>
  <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/docker/docker-original.svg" alt="docker" width="40" height="40"/>
  <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/nginx/nginx-original.svg" alt="nginx" width="40" height="40"/>
  <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/sqlite/sqlite-original.svg" alt="sqlite" width="40" height="40"/>
</p>

| Technology | Purpose |
|------------|---------|
| **Python (FastAPI)** | Backend load balancer, API server |
| **Bash** | Custom orchestration scripts and load testing automation |
| **React (Node.js)** | Frontend (user interface + admin panel) |
| **Docker** | Containerization of conversion services |
| **NGINX** | Reverse proxy, API Gateway, static file serving |
| **k6** | Load testing and performance evaluation |
| **SQLite** | Storing service metadata and statistics |

---

## 🚀 Main Commands

### 🐍 Load Balancer (main.py)

```bash
# Run the balancer locally
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Check status
curl http://localhost:8000/servers | jq
curl http://localhost:8000/stats | jq
```

### 🐳 Docker Containers

```bash
# Build and run the whole system
docker compose up --build -d

# Rebuild containers ecologically
docker compose build --no-cache

# Check container status
docker compose ps
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# View logs of specific containers
docker compose logs backend --tail 30
docker compose logs frontend --tail 30

# Stop and remove everything
docker compose down -v  # -v removes volumes

# Clean up the system (be careful!)
docker system prune -a
```

### 🗄 Database (requests.db)

```bash
# Create/reset the database schema
cd backend
python3 db_schema.py          # create tables
LC_ALL=C.UTF-8 python3 db_schema.py --reset  # delete all data

# View data
sqlite3 backend/data/requests.db "SELECT * FROM Services;"
sqlite3 backend/data/requests.db "SELECT * FROM requests ORDER BY created_at DESC LIMIT 10;"

# Terminal admin panel
python3 admin.py
watch -n 2 python3 admin.py   # auto-refresh every 2 seconds
```

### 📊 Load Testing (k6)

```bash
# Run all scenarios
k6 run backend/load_test.js

# Run with a specific balancing algorithm
LB_ALG=least_connections k6 run backend/load_test.js
LB_ALG=random k6 run backend/load_test.js
LB_ALG=ip_hash k6 run backend/load_test.js

# Run only one scenario (using filter)
k6 run --include "wav2mp3" backend/load_test.js
```

### 🎛 Orchestrator (scriptA.sh)

```bash
# Run the orchestrator (scales containers under load)
bash backend/scriptA.sh

# Monitor container status (in another terminal)
watch -n 2 'docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"'

# Check CPU usage of containers
docker stats --no-stream

# Stop all containers (Ctrl+C in scriptA.sh)
# Or manually:
docker stop srv1 srv2 srv3 srv4 && docker rm srv1 srv2 srv3 srv4
```

---

## 🗂 Database Schema (Conceptual)

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Services     │     │     Pools       │     │   Machines      │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ idService (PK)  │────<│ service_id (FK) │     │ idMachine (PK)  │
│ name            │     │ idPool (PK)     │     │ hostname        │
│ base_path       │     │ name            │     │ ip_address      │
│ cpu_intensity   │     │ algorithm       │     │ ssh_port        │
└─────────────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ ServiceInstances│     │  PoolMembers    │     │ AutoscalingRules│
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ idInstance (PK) │     │ idPoolMember(PK)│     │ idRule (PK)     │
│ service_id (FK) │────<│ pool_id (FK)    │     │ pool_id (FK)    │
│ machine_id (FK) │────<│ instance_id(FK) │     │ metric_type     │
│ container_id    │     │ weight          │     │ threshold       │
│ port            │     └─────────────────┘     │ min_instances   │
│ status          │                             │ max_instances   │
└─────────────────┘                             └─────────────────┘

┌─────────────────┐
│    requests     │  <- request statistics
├─────────────────┤
│ id (PK)         │
│ algorithm       │
│ server_name     │
│ endpoint        │
│ total_time      │
│ success         │
│ created_at      │
└─────────────────┘
```

## 📁 Project Structure

```
reverse-proxy-lb/
├── backend/
│   ├── main.py           # load balancer (FastAPI)
│   ├── admin.py          # terminal admin panel
│   ├── db_schema.py      # database schema
│   ├── scriptA.sh        # container orchestrator
│   ├── load_test.js      # k6 scenarios
│   ├── data/
│   │   └── requests.db   # SQLite database
│   └── Dockerfile
├── converter/
│   ├── app.py            # conversion services
│   └── Dockerfile
├── frontend/
│   ├── src/
│   │   ├── App.js        # React application
│   │   └── AdminPanel.js # admin panel
│   ├── nginx.conf        # reverse proxy config
│   └── Dockerfile
└── docker-compose.yml
```

## 🔄 Load Balancing Algorithms

| Algorithm | Description |
|-----------|-------------|
| `round_robin` | Cycles through servers one by one |
| `random` | Picks a random server for each request |
| `least_connections` | Chooses the server with fewest active connections |
| `ip_hash` | Hashes client IP address for consistent routing |
| `power_of_two` | Picks two random servers, selects the less loaded one |

---

## 🧪 Testing

```bash
# 1. Start the container ecosystem
docker compose up -d

## Run the following in separate terminals:

# 2. Run the orchestrator
bash backend/scriptA.sh

# 3. Generate load
k6 run backend/load_test.js

# 4. Real-time monitoring
python3 backend/admin.py                    # terminal admin panel
open http://localhost                       # web interface
watch -n 2 'curl -s http://localhost:8000/servers | jq .healthy_count'
```

---

## 📌 Notes

- **Maximum number of containers**: 4 (set in `scriptA.sh`)
- **Health check interval**: every 10 seconds via Docker API
- **Container ports mapping**: srv1:8081, srv2:8082, srv3:8083, srv4:8084
- **Internal Docker network**: `lb-net` (for communication between services)

---

*This project was developed to demonstrate a load balancing system with dynamic scaling capabilities.*
