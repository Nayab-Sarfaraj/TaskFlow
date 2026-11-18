<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://img.shields.io/badge/TaskFlow-000000?style=for-the-badge&logoColor=white">
  <img alt="TaskFlow" src="https://img.shields.io/badge/TaskFlow-ffffff?style=for-the-badge&logoColor=black">
</picture>

**Enterprise Distributed Background Job Processing System**

Transactional Outbox · Distributed Leases · Auto-Recovery · Strict Tenant Isolation · Full Observability

[![FastAPI](https://img.shields.io/badge/FastAPI_0.128-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Python](https://img.shields.io/badge/Python_3.9+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL_17-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy_2.0-D71F00?style=flat-square&logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org)
[![Redis](https://img.shields.io/badge/Redis_7_Streams-DC382D?style=flat-square&logo=redis&logoColor=white)](https://redis.io)
[![Prometheus](https://img.shields.io/badge/Prometheus_Metrics-E6522C?style=flat-square&logo=prometheus&logoColor=white)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana_Dashboards-F46800?style=flat-square&logo=grafana&logoColor=white)](https://grafana.com)
[![Docker](https://img.shields.io/badge/Docker_Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://www.docker.com)

</div>

---

## What it does

**TaskFlow** is an asynchronous, multi-tenant job orchestration engine designed to process heavy background workloads without data loss, dual-write drift, or phantom duplicate executions.

Clients submit background tasks (bulk email campaigns, video transcoding, PDF/report generation, and SSRF-protected web crawling) over a REST API authenticated with hashed API keys. Every job submission commits atomically with an outbox event into PostgreSQL. A decoupled publisher streams events into Redis with backpressure control. Distributed workers acquire leases, maintain heartbeats, and report telemetry, while background scheduler and stale-worker detector daemons automatically heal aborted jobs and handle exponential backoff retries.

---

## Architecture

```mermaid
graph TB
    subgraph Clients["Clients & Tenants"]
        ClientA["Tenant Client A\n(X-API-Key)"]
        ClientB["Tenant Client B\n(X-API-Key)"]
        Admin["Platform Admin\n(X-Admin-Key)"]
    end

    subgraph API["FastAPI Application"]
        Router["REST Endpoints\n/api/v1/jobs | /api/v1/clients"]
        RateLimiter["Redis Sliding Window\nRate Limiter"]
        Idempotency["Fingerprint & Deduplication\nSHA-256 Engine"]
        CapacityLock["PostgreSQL Advisory Lock\nCapacity Guard"]
    end

    subgraph Persistence["PostgreSQL 17 (ACID Boundary)"]
        JobsTable[("jobs & job_attempts")]
        OutboxTable[("outbox_events")]
        TenantsTable[("clients & api_keys")]
        WorkersTable[("workers (heartbeats)")]
    end

    subgraph Messaging["Redis 7 Event Broker"]
        RedisStream[("Stream: job_events\nConsumer Group: workers")]
        RateLimitKeys[("Sliding Window Keys")]
    end

    subgraph Engine["Publisher & Scheduler Daemons"]
        Publisher["Outbox Publisher\nBackpressure & XADD"]
        Scheduler["Retry Scheduler\nExponential Backoff"]
        StaleDetector["Stale Worker Detector\nLease Reclamation"]
    end

    subgraph Workers["Distributed Worker Cluster"]
        W1["Worker Process 1\n(Concurrency: 3)"]
        W2["Worker Process 2\n(Concurrency: 3)"]
    end

    subgraph Handlers["Task Execution Handlers"]
        H_Email["Email Handler\nSMTP / Mailpit"]
        H_Video["Video Handler\nFFmpeg + Semaphore"]
        H_Report["Report Handler\nPDF / CSV Compiler"]
        H_Scraping["Scraping Handler\nSSRF Protected HTTP"]
    end

    subgraph Telemetry["Observability Stack"]
        Prometheus["Prometheus /metrics\nMultiprocess Aggregator"]
        Grafana["Grafana Dashboards"]
    end

    ClientA -->|Submit / Query| Router
    ClientB -->|Submit / Query| Router
    Admin -->|Provision Keys| Router
    Router --> RateLimiter
    RateLimiter --> RateLimitKeys
    Router --> Idempotency
    Router --> CapacityLock
    CapacityLock --> JobsTable
    Router -->|Atomic Commit| JobsTable
    Router -->|Atomic Commit| OutboxTable
    Router --> TenantsTable

    Publisher -->|Poll unpublished| OutboxTable
    Publisher -->|XADD bounded| RedisStream
    RedisStream -->|XREADGROUP| W1
    RedisStream -->|XREADGROUP| W2

    W1 -->|Heartbeat & Lease| WorkersTable
    W1 -->|Execute| Handlers
    Handlers -->|Attempt logs & outputs| JobsTable

    Scheduler -->|Poll retrying jobs| JobsTable
    Scheduler -->|Re-enqueue| OutboxTable
    StaleDetector -->|Scan dead heartbeats| WorkersTable
    StaleDetector -->|Reset expired leases| JobsTable

    W1 -.->|Metrics| Prometheus
    Router -.->|Metrics| Prometheus
    Prometheus --> Grafana
```

---

## Job Status Flow

```mermaid
stateDiagram-v2
    [*] --> QUEUED: Job submitted & committed to Outbox
    QUEUED --> PROCESSING: Worker claims lease via Redis event
    
    PROCESSING --> COMPLETED: Handler finishes successfully
    COMPLETED --> [*]

    PROCESSING --> RETRYING: Transient error & attempts < max_attempts
    note right of RETRYING: Exponential backoff with jitter calculated
    RETRYING --> QUEUED: Scheduler timer matures (scheduled_at <= now())

    PROCESSING --> FAILED: Non-retryable error OR attempts exhausted
    note right of FAILED: Moved to Dead-Letter status
    FAILED --> RETRYING: Operator manual retry POST /api/v1/jobs/{id}/retry
    FAILED --> [*]

    PROCESSING --> QUEUED: Worker crashes / lease expires (Stale Detector)
    QUEUED --> CANCELLED: Job cancelled by client
    CANCELLED --> [*]
```

---

## Core Pipeline — Step by Step

```mermaid
sequenceDiagram
    autonumber
    actor Tenant as Tenant Client
    participant API as FastAPI Gateway
    participant Redis as Redis 7
    participant PG as PostgreSQL 17
    participant Pub as Outbox Publisher
    participant Worker as Worker Process
    participant Target as External Service / SMTP / Web

    Tenant->>API: POST /api/v1/jobs/ (Payload, X-API-Key, Idempotency-Key)
    API->>API: Hash API Key & verify tenant status in DB
    API->>Redis: Check client sliding window rate limit
    API->>PG: Acquire pg_advisory_xact_lock & check queue capacity
    API->>PG: Check (client_id, idempotency_key) duplicate
    Note over API,PG: Job and OutboxEvent inserted atomically in single transaction
    API->>PG: INSERT INTO jobs & INSERT INTO outbox_events
    PG-->>API: Transaction Committed
    API-->>Tenant: 201 Created (job_id, status=queued)

    loop Outbox Publisher Polling Loop
        Pub->>Redis: Query xpending(job_events, workers)
        Note over Pub: Calculates capacity = max(0, REDIS_MAX_IN_FLIGHT - pending)
        Pub->>PG: SELECT unpublished events (batch_size <= capacity)
        Pub->>Redis: XADD job_events (job_id, event_type)
        Pub->>PG: UPDATE outbox_events SET published_at = now()
    end

    Queue->>Worker: Redis XREADGROUP consumes event
    Worker->>PG: Claim job & set lease_expires_at = now() + 60s
    Worker->>PG: INSERT INTO job_attempts (attempt_number, status=running)

    par Background Lease & Heartbeat Renewal
        loop Every lease interval
            Worker->>PG: UPDATE jobs SET lease_expires_at = now() + 60s
            Worker->>PG: UPDATE workers SET last_heartbeat = now()
        end
    and Job Execution
        Worker->>Target: Execute Handler (Email, Scraping, Video, Report)
        Target-->>Worker: Handler execution response / output
    end

    alt Success Path
        Worker->>PG: UPDATE jobs SET status=completed, completed_at=now()
        Worker->>PG: UPDATE job_attempts SET status=succeeded
        Worker->>PG: INSERT INTO job_outputs (output metadata)
        Worker->>Redis: XACK job_events workers message_id
    else Retryable Error
        Worker->>API: Failure Classifier flags error as transient
        Worker->>PG: UPDATE jobs SET status=retrying, scheduled_at=now()+delay
        Worker->>PG: UPDATE job_attempts SET status=failed, error=...
        Worker->>Redis: XACK job_events workers message_id
    else Permanent / Exhausted Error
        Worker->>PG: UPDATE jobs SET status=failed (Dead Letter)
        Worker->>PG: UPDATE job_attempts SET status=failed
        Worker->>Redis: XACK job_events workers message_id
    end
```

---

## Job Types & Execution Engine

Each job handler enforces rigorous input validation, isolation boundaries, and execution bounds:

| Job Type | Key Inputs | Limits & Controls | Security / Safety | Output Produced |
|---|---|---|---|---|
| **`email`** | `recipients`, `subject`, `body` | Up to 100 recipients; deduplicated & normalized; SMTP timeout bounds | RFC email regex validation; prevents empty address lists | Delivery status, recipient counts, sent timestamps |
| **`video`** | `url`, `resolution`, `download_size_bytes`, `generate_thumbnail` | 100 MB max size; width/height $\ge$ 144px; concurrency throttled by semaphore | SSRF validation on source URL; FFmpeg process sandbox | Transcoded video metadata, resolution verification, thumbnail path |
| **`scraping`** | `urls` (List of HTTP URLs) | Max 20 URLs per job; 10 MB HTTP response body ceiling; 15s timeout | Full SSRF blocker: resolves DNS & rejects RFC1918, 127.0.0.0/8, 169.254.0.0/16, cloud metadata; redirect validation | Response status, byte length, SHA-256 body hash, latency |
| **`report`** | `report_type`, `parameters`, `format` (`pdf`, `html`, `csv`) | 50 MB generated size ceiling; 120s max execution duration | Parameter schema sanitization; CPU execution budget enforcement | Generated document path, size in bytes, format summary |

---

## Project Structure

```
TaskFlow/
├── alembic/                         # Database schema migrations
│   ├── versions/                    # Versioned migration scripts
│   └── env.py                       # Alembic environment and model metadata
├── docker-compose.yml               # Dev stack (PostgreSQL, Redis, Mailpit, Prometheus, Grafana)
├── docker-compose.integration.yml   # Isolated test infrastructure stack
├── docs/                            # Benchmarks & architectural notes
│   └── phase9-performance-report.md # Locust load-testing evaluation report
├── monitoring/                      # Observability provisioning
│   ├── grafana/
│   │   ├── dashboards/              # Pre-configured TaskFlow dashboard JSON
│   │   └── provisioning/            # Auto-provisioned datasources
│   └── prometheus.yml               # Scrape config (15s scrape interval, multiprocess metrics)
├── scripts/                         # Automation & validation scripts
│   ├── submit_20_jobs.py            # Automated multi-job generator for metrics observation
│   ├── test-integration.sh          # Orchestrated integration test runner
│   └── test-failure.sh              # Worker crash & race recovery test runner
├── src/                             # Core application source
│   ├── main.py                      # FastAPI app, Prometheus exporter, tracing middleware
│   ├── logging_config.py            # JSON-formatted logging & contextvar correlation
│   ├── exceptions.py                # Domain exceptions (IdempotencyConflict, QueueCapacityExceeded)
│   ├── config/                      # Service configurations (Redis client, Retry policies)
│   ├── core/                        # Prometheus metric counters, histograms & gauges
│   ├── db/                          # Async database engine & SessionLocal factory
│   ├── dependencies/                # Auth dependencies (X-API-Key and X-Admin-Key guards)
│   ├── models/                      # SQLAlchemy 2.0 ORM definitions
│   │   ├── jobs.py                  # Job entity with status, lease, attempt counters
│   │   ├── job_attempts.py          # Per-attempt audit log
│   │   ├── job_outputs.py           # Output artifacts and execution results
│   │   ├── outbox_events.py         # Transactional outbox event store
│   │   ├── workers.py               # Worker registry with status and heartbeat timestamp
│   │   ├── client.py                # Tenant model and rate limit configuration
│   │   └── api_key.py               # Hashed API keys with expiration and revocation
│   ├── publisher/                   # Outbox processor
│   │   └── main.py                  # Polls unpublished events & writes to Redis Stream
│   ├── repositories/                # Database queries & PG advisory locking
│   ├── routers/                     # Route definitions (Jobs, Clients, API Keys, Health)
│   ├── scheduler/                   # Retry orchestrator
│   │   └── scheduler.py             # Re-enqueues retrying jobs using skip_locked transactions
│   ├── schemas/                     # Pydantic request / response schemas
│   ├── services/                    # Domain logic & Redis sliding-window rate limiting
│   ├── utils/                       # Security & reliability helpers
│   │   ├── ssrf.py                  # DNS lookup and IP range filter for scraping
│   │   ├── failure_classifier.py    # Transient vs permanent error classification
│   │   ├── retry_backoff.py         # Exponential backoff with full jitter
│   │   └── hash.py                  # Request fingerprinting (SHA-256)
│   └── workers/                     # Background execution engine
│       ├── main.py                  # Worker consumer loop & graceful shutdown handling
│       ├── lifecycle.py             # Worker registration & periodic heartbeat loop
│       ├── stale_delector.py        # Dead worker detection & lease reclamation
│       ├── email.py                 # SMTP bulk email worker handler
│       ├── video.py                 # FFmpeg video transcoding worker handler
│       ├── report.py                # Report generation worker handler
│       └── scraping.py              # SSRF-guarded web scraping worker handler
└── tests/                           # Complete test suite
    ├── integration/                 # End-to-end integration tests
    ├── failure/                     # Crash injection & split-brain recovery tests
    ├── load/                        # Locust load test scripts and results
    └── test_*.py                    # Unit tests, API key lifecycle, concurrency & acceptance tests
```

---

## Data Types & Schema Contracts

The interfaces below establish the strict contracts between API clients, storage, and worker handlers.

### Job Submission & API Contracts

```python
# src/schemas/jobs.py
class job_submission_request(BaseModel):
    job_type: JobType                        # "email" | "video" | "report" | "scraping"
    payload: dict[str, Any]                  # Handler-specific payload
    priority: int = Field(default=5, ge=1, le=10)
    max_retries: int = Field(default=3, ge=0, le=10)
    scheduled_at: Optional[datetime] = None  # Future scheduling (UTC, timezone-aware)

class job_response(BaseModel):
    id: int
    job_type: JobType
    status: JobStatus                        # queued, processing, completed, failed, cancelled, retrying
    payload: dict[str, Any]
    priority: int
    max_attempts: int
    scheduled_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
```

### Handler Specific Payloads

```python
# src/workers/email.py
class EmailJobPayload(BaseModel):
    recipients: List[str] = Field(min_length=1, max_length=100)
    subject: str = Field(min_length=1, max_length=998)
    body: str = Field(min_length=1)

# src/workers/video.py
class VideoJobPayload(BaseModel):
    url: HttpUrl
    resolution: str                          # Format: "WIDTHxHEIGHT" (e.g. "1280x720")
    download_size_bytes: int = Field(default=0, ge=0)
    generate_thumbnail: bool = False

# src/workers/scraping.py
class ScrapingJobPayload(BaseModel):
    urls: List[HttpUrl] = Field(min_length=1, max_length=20)

# src/workers/report.py
class ReportJobPayload(BaseModel):
    report_type: str = Field(default="generic", min_length=1, max_length=100)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    format: Literal["pdf", "html", "csv"] = "pdf"
    generated_size_bytes: int = Field(default=0, ge=0)
    execution_time_seconds: float = Field(default=0, ge=0)
```

---

## Key Implementation Notes

<details>
<summary><strong>Transactional Outbox — Guaranteed at-least-once delivery</strong></summary>

Dual writes to a database and a message queue without a distributed transaction coordinator lead to split-brain states: if the message broker write succeeds but the database transaction rolls back, phantom jobs run; if the database commit succeeds but the broker write fails, jobs are permanently lost.

TaskFlow writes the job record and an `outbox_events` record in the **exact same PostgreSQL transaction**. An asynchronous `publisher` daemon fetches unpublished events, pushes them to Redis Streams using `XADD`, and marks them published. The system stays completely consistent even during hard Redis or network outages.

</details>

<details>
<summary><strong>Distributed Worker Leases & Stale Worker Recovery</strong></summary>

Workers do not hold static database locks during long-running tasks. Instead, when a worker acquires a job from the Redis stream, it claims a lease by writing `lease_expires_at = now() + JOB_LEASE_DURATION_SECONDS` (default: 60s).

A background asyncio task inside the worker periodically renews this lease while execution is active. If a worker process crashes, is killed (`SIGKILL`), or suffers a CPU starvation lockup, its lease expires. The `stale_detector` background loop queries expired leases, marks dead workers in the `workers` table, and transitions abandoned jobs back to `QUEUED` for immediate reassignment.

</details>

<details>
<summary><strong>SSRF Protection for Web Scraping Tasks</strong></summary>

Allowing arbitrary webhooks or URLs opens services to Server-Side Request Forgery attacks, exposing cloud metadata services (`169.254.169.254`), container interfaces, or internal databases.

TaskFlow implements strict pre-flight DNS inspection in `src/utils/ssrf.py`:
1. Resolves target hostnames to concrete IP addresses prior to HTTP connection.
2. Checks all resolved IPs against prohibited subnets: RFC 1918 private ranges (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), loopbacks (`127.0.0.0/8`), link-local (`169.254.0.0/16`), multicast, and broadcast addresses.
3. Overrides the HTTP client transport to bind only to validated safe IPs and intercepts all `3xx` redirects to re-validate each redirect hop against SSRF rules.

</details>

<details>
<summary><strong>Concurrency Guards & PostgreSQL Advisory Locking</strong></summary>

Under heavy load surges, accepting unbound numbers of pending jobs exhausts database memory and drives connection pool timeouts.

TaskFlow uses PostgreSQL session transactional advisory locks (`pg_advisory_xact_lock(hashtext('taskflow:pending-job-capacity'))`) during submission. It checks active pending jobs (`QUEUED` + `RETRYING`) against `MAX_PENDING_JOBS`. If capacity is exceeded, it immediately responds with `HTTP 503 Service Unavailable` accompanied by a `Retry-After: <seconds>` header, protecting both the database pool and worker queues from cascading failures.

</details>

<details>
<summary><strong>Idempotent Replays via Request Fingerprinting</strong></summary>

When clients send an `Idempotency-Key` header:
1. The submission payload is normalized and digested into a SHA-256 fingerprint (`request_fingerprint`).
2. If the `(client_id, idempotency_key)` pair already exists with an **identical fingerprint**, TaskFlow returns the existing job status (`200 OK` / `201 Created`) without queuing a new task.
3. If an identical key is submitted with **different parameters**, TaskFlow rejects it with `HTTP 409 Conflict`, strictly enforcing idempotency semantics.

</details>

<details>
<summary><strong>Transient vs Permanent Error Classification & Smart Backoff</strong></summary>

Not all failures should be retried. Re-attempting an unauthenticated 401/403 or malformed payload wastes resources and exacerbates rate limits.

`src/utils/failure_classifier.py` distinguishes:
- **Permanent Errors** (e.g., SMTP invalid recipient, DNS NXDOMAIN, malformed video codecs, schema errors): Immediately marked `FAILED` (Dead Letter) with no retries.
- **Transient Errors** (e.g., SMTP 421/450 temporary rejection, HTTP 429/503 responses, connection timeouts): Scheduled with exponential backoff and randomized jitter:
  $$\text{delay} = \min(\text{max\_backoff}, \text{base} \times 2^{\text{attempt}}) + \text{jitter}$$

</details>

<details>
<summary><strong>Multiprocess Prometheus Aggregation</strong></summary>

FastAPI Uvicorn workers, publisher daemons, scheduler daemons, and worker consumers run in separate OS processes. Standard in-memory Prometheus counters produce fragmented metrics across processes.

TaskFlow configures `PROMETHEUS_MULTIPROC_DIR=/tmp/taskflow-prometheus` and imports `prometheus_client.multiprocess.MultiProcessCollector`. All counters, histograms (queue wait duration, job execution duration), and live gauges (`taskflow_active_jobs`, `taskflow_queue_depth`) are memory-mapped into shared files, allowing the `/metrics` endpoint to render a coherent, cluster-wide scrape format.

</details>

<details>
<summary><strong>Tenant Isolation & Constant-Time Auth Verification</strong></summary>

API keys are generated with high-entropy cryptographic strings (`secrets.token_urlsafe(32)`) and stored exclusively as SHA-256 hashes (`key_hash`). The raw key is exposed only once upon creation.

Authentication queries join `clients` and `api_keys` on `key_hash`, ensuring:
- Client isolation: Clients cannot read, list, or retry jobs belonging to another tenant (`client_id` filtered on every repository query).
- Constant-time verification: Key comparison prevents side-channel timing attacks.
- Instant revocation: Setting `revoked_at` invalidates access without affecting historical audit records.

</details>

---

## Tech Stack

| Component | Technology | Why |
|---|---|---|
| **API Framework** | FastAPI 0.128 + Uvicorn | Async ASGI framework, OpenAPI documentation, native Pydantic validation |
| **Database** | PostgreSQL 17 | Robust ACID transactions, row-level locking (`FOR UPDATE SKIP LOCKED`), advisory locks |
| **ORM & Migrations** | SQLAlchemy 2.0 (asyncpg) + Alembic | Modern type-safe async database queries with production migration tracking |
| **Broker / Streaming** | Redis 7 Streams | Low-latency stream distribution, consumer groups (`XREADGROUP`), acknowledgment (`XACK`) |
| **Validation** | Pydantic v2 | High-performance Rust-backed data parsing and payload validation |
| **Email Sandbox** | Mailpit | Headless SMTP server and modern Web UI for inspecting transactional emails in dev |
| **Media Processing** | FFmpeg | Industry-standard audio/video manipulation and thumbnail generation |
| **Metrics** | Prometheus Client | Low-cardinality multiprocess metrics collection exported on `/metrics` |
| **Dashboards** | Grafana | Pre-provisioned operational dashboards visualizing real-time queue depths & latencies |
| **Load Testing** | Locust 2.34 | High-concurrency load testing simulating realistic client traffic distributions |

---

## System Constraints & Product Limits

| Constraint | Config Variable | Default Value | Description |
|---|---|---|---|
| **Max Pending Jobs** | `MAX_PENDING_JOBS` | `1,000` | Global queue threshold before rejecting requests with HTTP 503 |
| **Worker Concurrency** | `WORKER_CONCURRENCY` | `3` | Maximum concurrent jobs handled per worker instance |
| **Job Lease Duration** | `JOB_LEASE_DURATION_SECONDS` | `60 s` | Worker lease timeout before a job is flagged as abandoned |
| **Worker Grace Period** | `WORKER_SHUTDOWN_GRACE_PERIOD`| `30 s` | Timeout for workers to finish in-flight jobs on `SIGTERM` |
| **Max Job Execution** | `MAX_JOB_EXECUTION_SECONDS` | `300 s` | Hard execution timeout per job attempt |
| **Publisher In-Flight Limit**| `REDIS_MAX_IN_FLIGHT` | `6` | Maximum unacknowledged events in Redis Stream before pausing outbox publisher |
| **Max Email Recipients** | `MAX_EMAIL_RECIPIENTS` | `100` | Recipient ceiling per bulk email job submission |
| **Max Video Download** | `MAX_VIDEO_SIZE_MB` | `100 MB` | Video file download ceiling |
| **Video Concurrency** | `MAX_VIDEO_CONCURRENCY` | `2` | Per-worker semaphore limit for concurrent FFmpeg processes |
| **Max Scraping URLs** | `MAX_SCRAPING_URLS` | `20` | Maximum URLs permitted per scraping job |
| **Max Scraping Response** | `MAX_HTTP_RESPONSE_MB` | `10 MB` | Maximum response byte limit per crawled URL |
| **Client Rate Limit** | `rate_limit_per_minute` | `60 / min` | Default sliding-window submission rate limit per client tenant |

---

## Environment Variables

Copy `.env.example` to `.env` and configure values for your environment:

| Variable | Description | Default |
|---|---|---|
| `POSTGRES_USER` | PostgreSQL user account | `nayab` |
| `POSTGRES_PASSWORD` | PostgreSQL user password | `taskflow` |
| `POSTGRES_DB` | PostgreSQL primary database | `taskflow` |
| `DATABASE_URL` | SQLAlchemy async connection URI | `postgresql+asyncpg://nayab:taskflow@localhost:5435/taskflow` |
| `REDIS_URL` | Redis connection URI | `redis://localhost:6379/0` |
| `TASKFLOW_REDIS_STREAM` | Redis Stream key for job events | `job_events` |
| `TASKFLOW_REDIS_CONSUMER_GROUP` | Consumer group name for worker pool | `workers` |
| `ADMIN_API_KEY` | Master key for managing clients & API keys (`X-Admin-Key`) | `your-admin-api-key-here` |
| `PROMETHEUS_MULTIPROC_DIR` | Shared metrics directory for multiprocess collection | `/tmp/taskflow-prometheus` |
| `SMTP_HOST` | Outgoing SMTP host for email jobs | `localhost` |
| `SMTP_PORT` | Outgoing SMTP port (Mailpit default: 1025) | `1025` |
| `MAILPIT_WEB_PORT` | Web UI port for Mailpit email preview | `8025` |
| `MAX_VIDEO_SIZE_MB` | Maximum video file size in megabytes | `100` |
| `MAX_VIDEO_CONCURRENCY` | Maximum concurrent FFmpeg renders per worker | `2` |
| `MAX_HTTP_RESPONSE_MB` | Maximum response size for scraping requests | `10` |
| `SCRAPING_REQUEST_TIMEOUT_SECONDS` | Timeout for individual scraping HTTP requests | `15` |
| `REPORT_MAX_SIZE_BYTES` | Maximum report file output size in bytes | `52428800` (50 MB) |
| `REPORT_MAX_EXECUTION_SECONDS` | Maximum allowed report generation time | `120` |

---

## Local Development & Quickstart

### Prerequisites

- **Python 3.9+**
- **Docker & Docker Compose**
- **FFmpeg** (required locally if running video workers outside containers)

### 1. Setup Environment & Start Dependencies

```bash
# Clone the repository
git clone https://github.com/Nayab-Sarfaraj/TaskFlow.git
cd TaskFlow

# Copy environment configuration
cp .env.example .env

# Start PostgreSQL, Redis, Mailpit, Prometheus, and Grafana
docker compose up -d
```

Verify service availability:
- **PostgreSQL**: `localhost:5435`
- **Redis**: `localhost:6379`
- **Mailpit Web UI**: [http://localhost:8025](http://localhost:8025)
- **Prometheus UI**: [http://localhost:9090](http://localhost:9090)
- **Grafana UI**: [http://localhost:3000](http://localhost:3000) (`admin` / `admin`)

### 2. Python Virtual Environment & Migrations

```bash
# Initialize and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run database migrations
alembic upgrade head
```

---

## Running the Services

For a complete local processing cluster, run each component in a separate terminal:

### Terminal 1: API Gateway
```bash
source .venv/bin/activate
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

### Terminal 2: Transactional Outbox Publisher
```bash
source .venv/bin/activate
python -m src.publisher.main
```

### Terminal 3: Distributed Worker
```bash
source .venv/bin/activate
WORKER_ID=worker-1 python -m src.workers.main
```

### Terminal 4: Retry Scheduler & Health Daemon
```bash
source .venv/bin/activate
python -m src.scheduler.scheduler
```

---

## API Quick Reference

### 1. Provisioning a Tenant Client & Generating API Key

```bash
# Register a client (Requires X-Admin-Key matching ADMIN_API_KEY)
curl -X POST http://localhost:8000/api/v1/clients/ \
  -H "Content-Type: application/json" \
  -H "X-Admin-Key: your-admin-api-key-here" \
  -d '{"name": "Acme Corp", "rate_limit_per_minute": 120}'

# Issue an API Key for Client ID 1
curl -X POST http://localhost:8000/api/v1/clients/1/keys \
  -H "Content-Type: application/json" \
  -H "X-Admin-Key: your-admin-api-key-here" \
  -d '{"name": "Production Key", "expires_at": null}'
```
*Save the returned key secret. It is hashed with SHA-256 and will not be displayed again.*

---

### 2. Submitting Background Jobs

Pass your API key in `X-API-Key`, along with optional `Idempotency-Key` and `X-Request-ID` headers:

#### Bulk Email Job
```bash
curl -X POST http://localhost:8000/api/v1/jobs/ \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <YOUR_API_KEY>" \
  -H "Idempotency-Key: welcome-email-user-101" \
  -d '{
    "job_type": "email",
    "payload": {
      "recipients": ["alex@example.com", "taylor@example.com"],
      "subject": "System Activation Notice",
      "body": "Welcome to your new enterprise workspace."
    },
    "priority": 8,
    "max_retries": 3
  }'
```

#### SSRF-Protected Web Scraping Job
```bash
curl -X POST http://localhost:8000/api/v1/jobs/ \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <YOUR_API_KEY>" \
  -d '{
    "job_type": "scraping",
    "payload": {
      "urls": ["https://httpbin.org/json", "https://api.github.com/zen"]
    },
    "priority": 5
  }'
```

#### PDF Report Compilation Job
```bash
curl -X POST http://localhost:8000/api/v1/jobs/ \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <YOUR_API_KEY>" \
  -d '{
    "job_type": "report",
    "payload": {
      "report_type": "monthly_reconciliation",
      "parameters": {"department": "finance", "quarter": 4},
      "format": "pdf",
      "execution_time_seconds": 2
    },
    "priority": 4
  }'
```

---

### 3. Monitoring & Manual Intervention

```bash
# Query jobs with status filtering and pagination
curl -X GET "http://localhost:8000/api/v1/jobs/?status=completed&limit=10&offset=0" \
  -H "X-API-Key: <YOUR_API_KEY>"

# Get detailed job status and attempt history
curl -X GET http://localhost:8000/api/v1/jobs/1 \
  -H "X-API-Key: <YOUR_API_KEY>"

# Manually trigger retry for a failed job
curl -X POST http://localhost:8000/api/v1/jobs/1/retry \
  -H "X-API-Key: <YOUR_API_KEY>"
```

---

## Scripts & Testing

### Running Tests

```bash
# Run unit, schema, and API key acceptance tests
pytest tests/test_health.py tests/test_api_key_lifecycle.py tests/test_tenant_isolation.py

# Run complete integration test suite with ephemeral Docker Compose environment
./scripts/test-integration.sh

# Run worker crash, split-brain, and lease-recovery tests
./scripts/test-failure.sh
```

### Batch Traffic Simulation
Generate a sustained batch of 20 jobs across multiple priorities with unique idempotency keys to observe Prometheus counter rates and Grafana dashboard panels:
```bash
TASKFLOW_API_KEY="<YOUR_API_KEY>" python scripts/submit_20_jobs.py
```

### High-Concurrency Load Testing (Locust)
Simulate hundreds of concurrent clients with mixed traffic (90% unique submissions, 10% idempotency replays):
```bash
locust -f tests/load/locustfile.py --host http://localhost:8000
```
Open [http://localhost:8089](http://localhost:8089) to view real-time latency percentiles, throughput charts, and failure distributions.
