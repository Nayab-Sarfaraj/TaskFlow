# Phase 9 Performance Report

Date: 2026-11-15  
Scope: observability, health checks, resilience testing, and Locust load-test analysis.

## Evidence and limitations

This report uses the retained Locust artefacts in `tests/load/results/` and the
current TaskFlow configuration. It does not infer metrics that were not retained.
In particular, no exported Prometheus query results exist for comparable
one-, two-, and three-worker steady-state periods, and no CPU, memory, or
PostgreSQL connection snapshots were saved during the run.

The retained API test was an earlier 500-user investigation:

- Files: `tests/load/results/investigate-500_*.csv`
- Virtual users: 500 after ramp-up at 25 users/second
- Actual request rate: 200.84 requests/second aggregate
- Test job: `report`, PDF output, configured execution delay of approximately
  two seconds
- Mix: Locust defines 90% new idempotency keys and 10% idempotency replays.

The high request rate was produced by the former short Locust wait time. It is
not comparable to the later controlled-RPS Locust configuration. Current load
tests assign a distinct load client/API key per user and use
`TASKFLOW_LOAD_TARGET_RPS` plus `TASKFLOW_LOAD_EXPECTED_USERS` to pace traffic.

## Test environment and configuration

| Component | Recorded configuration / observation |
| --- | --- |
| API | FastAPI/Uvicorn running locally on macOS; port 8000 |
| PostgreSQL | Docker Compose PostgreSQL; SQLAlchemy async pool `pool_size=10`, `max_overflow=10`, `pool_timeout=5s`, `pool_pre_ping=true` |
| Redis | Docker Compose Redis using stream `job_events` and consumer group `workers` |
| Worker | Worker process default `WORKER_CONCURRENCY=3`; each worker uses an `asyncio.Semaphore` to enforce this limit |
| Prometheus | Scrapes `/metrics`; Grafana dashboard was used for operational observation |
| Rate limiting | 60 job submissions per minute per client ID |
| Load clients | Current configuration provisions 1,000 deterministic load-test clients, preventing a single test client from dominating rate limits |

## Worker throughput

The intended Grafana query is:

```promql
sum(rate(taskflow_jobs_completed_total[1m]))
```

No retained Prometheus export identifies comparable steady-state windows for
each worker count. The values therefore remain unavailable rather than being
estimated from queued/completed job counts.

| Worker processes | Jobs completed/sec | Status | Notes |
| --- | ---: | --- | --- |
| 1 | Unavailable | Not captured | One worker has a configured concurrency ceiling of 3. |
| 2 | Unavailable | Not captured | A two-worker setup has a configured ceiling of 6 concurrent jobs. This is a ceiling, not a measured throughput result. |
| 3 | Unavailable | Not captured | A three-worker setup has a configured ceiling of 9 concurrent jobs. This is a ceiling, not a measured throughput result. |

Grafana did confirm that the completed-jobs rate, active-jobs gauge, queue
backlog, execution-duration histogram, and queue-wait histogram can update
during processing. The dashboard screenshot history was not exported with
timestamps sufficient for a worker-count comparison.

## API latency and result counts

The following values are taken directly from
`investigate-500_stats.csv`. They include both successful and rejected
requests because Locust's retained aggregate CSV does not split latency by
HTTP status code.

| Request group | Requests | Successful | Failed | p50 | p95 | p99 | Mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unique submission | 32,471 | 1,643 | 30,828 | 2.30 s | 4.50 s | 5.00 s | 2.217 s |
| Idempotency replay | 3,557 | 246 | 3,311 | 2.30 s | 4.50 s | 5.00 s | 2.226 s |
| **Aggregate** | **36,028** | **1,889** | **34,139** | **2.30 s** | **4.50 s** | **5.00 s** | **2.218 s** |

The failures CSV records only HTTP 503 outcomes:

| HTTP status | Count | Interpretation |
| --- | ---: | --- |
| 201 | 1,889 | Accepted request or idempotent replay response |
| 429 | 0 recorded | No rate-limit failures in this retained CSV |
| 503 | 34,139 | Overload protection response; the retained failure body does not identify whether each instance was pending-capacity protection or database-pool saturation |
| 500 | 0 recorded | No HTTP 500 rows in the retained Locust failure CSV |

Successful-only submission latency and rejected-only latency are **unavailable**
from the existing CSVs. Future Locust runs should export response status labels
or separate request names for accepted and rejected responses.

## Utilization and backlog

| Resource | Available measurement | Result / interpretation |
| --- | --- | --- |
| PostgreSQL connections | No saved `pg_stat_activity` output | Unavailable. The configured pool permits up to 20 connections per API process (10 base + 10 overflow). |
| PostgreSQL pool | API logs from the investigation | SQLAlchemy QueuePool timeouts were observed under high load. This is a confirmed bottleneck for that test. Pool timeouts now map to HTTP 503 with `Retry-After: 1`, rather than an HTTP 500. |
| CPU and memory | No recorded host/container samples | Unavailable. |
| Redis stream backlog | Grafana was observed during load | Backlog rose while submissions exceeded worker capacity, then drained when processing caught up. No numeric time-series export was retained. |
| Worker concurrency | Grafana `taskflow_active_jobs` and worker configuration | The gauge changed under load and returned toward zero after work completed. Individual worker execution is capped at 3. No complete per-worker utilization series was retained. |

For a future capture, run this while the workload is active and save its output:

```sql
SELECT state, COUNT(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY state;
```

## Bottlenecks and findings

| Finding | Evidence | Interpretation |
| --- | --- | --- |
| Job execution is approximately two seconds | Load payload sets `execution_time_seconds` to 2; worker logs during the run showed starts and completions about two seconds apart | Individual report job execution is relatively short. |
| Queue wait was observed around 250 seconds | Operational Grafana observation during the high-backlog run | Arrival rate exceeded available worker capacity, producing significant queue pressure. The exact average was not exported. |
| Queue-wait p95 saturated near 10 seconds in the dashboard | Grafana observation | The configured histogram bucket range is too low for the real queue wait. A 10-second p95 is a bucket ceiling, not the actual p95 when waits are much longer. |
| API saturation occurred before worker capacity was the only limit | 500-user Locust run: 94.76% failures, all recorded as 503; pool timeout logs were observed | The API/database connection path was the immediate limiting resource at approximately 200.84 requests/sec. |
| Rate limiting can distort load tests | Existing limiter is 60 requests/minute per client; prior tests used one client | The test now uses unique load clients, so rate limiting is no longer the dominant result for normal 25–50 RPS controlled tests. |
| Worker capacity is finite | `WORKER_CONCURRENCY=3` per process | Add workers or increase concurrency only after checking downstream limits and resource use. |

## Expected and unexpected errors

| Outcome | Classification | Notes |
| --- | --- | --- |
| HTTP 429 | Expected protection | Expected only when a client exceeds the 60/minute limit. None were recorded in the retained 500-user CSV. |
| HTTP 503 from pending-job capacity | Expected protection | Indicates durable queue admission was intentionally rejected; callers should retry after the advertised delay. |
| HTTP 503 from database pool timeout | Expected overload response after the fix | SQLAlchemy pool exhaustion is now returned as 503 with `Retry-After: 1`, not 500. |
| HTTP 500 | Unexpected | Earlier investigation found pool timeout as a cause. The retained Locust CSV records no 500 responses; the mapping to 503 removes this known overload path from 500 handling. |
| Redis errors during a worker run | Operational incident | Redis connection errors prevented ACK/recovery temporarily and can stop consumption until connectivity recovers. Accepted jobs remain durable in PostgreSQL through the outbox design. |

## Targets versus observations

| Target | Observation | Result |
| --- | --- | --- |
| API p95 under 300 ms | Aggregate p95 was 4.50 s in the uncontrolled 500-user run | Not met under this overload test. A controlled 25 RPS result has not yet been retained. |
| 1,000 concurrent clients | Load harness supports 1,000 distinct test clients; no retained 1,000-user result is available | Harness ready; measurement unavailable. |
| Zero duplicate job creation | Idempotency replay scenario is implemented; no database reconciliation export was retained | Not fully evidenced by retained artefacts. |
| Zero lost accepted jobs | PostgreSQL/outbox/recovery behaviour was tested in Phase 9; no combined load-run reconciliation export is retained | Not fully evidenced by retained artefacts. |
| No unhandled errors | No Locust exception rows; 34,139 controlled 503 rejections occurred | No recorded client-side exceptions, but the run was saturated. |
| Worker concurrency limits respected | Enforced with per-worker semaphore; no complete utilization export retained | Implementation evidence available; load-run proof incomplete. |

## Recommendations

1. Use the controlled Locust configuration and increase from 25 RPS gradually;
   retain a separate CSV prefix for each rate and worker count.
2. Record the Prometheus completed-jobs query for one, two, and three workers
   over comparable steady-state windows before claiming worker scaling values.
3. Export PostgreSQL connection states, CPU, memory, Redis stream backlog, and
   queue-depth metrics with every benchmark.
4. Increase the queue-wait histogram bucket range beyond 10 seconds so p95
   represents real backlog delays.
5. Size the API SQLAlchemy pool and PostgreSQL `max_connections` together,
   accounting for API, worker, publisher, scheduler, recovery, and test
   processes. Do not increase pool size without verifying database capacity.
6. Keep 503 as the overload response and let callers retry with backoff;
   reserve HTTP 500 for unexpected server defects.
7. Store status-specific latency data in future Locust output to distinguish
   healthy accepted-request latency from fast rejection latency.

## Conclusion

Phase 9 established observable API, queue, worker, and health behaviour. The
retained 500-user test demonstrates that an uncontrolled approximately
200 RPS submission workload overloads the API/database pool and produces
protective 503 responses. It does not establish sustainable throughput or
worker-scaling measurements. Those missing measurements are explicitly
identified above and should be captured with the paced, multi-client Locust
configuration before using the figures for capacity planning.
