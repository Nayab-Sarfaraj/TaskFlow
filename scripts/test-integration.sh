#!/usr/bin/env bash
set -euo pipefail

export TASKFLOW_INTEGRATION=1
export DATABASE_URL="${DATABASE_URL:-postgresql+asyncpg://taskflow_test:taskflow_test@localhost:5436/taskflow_test}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6380/0}"
export TASKFLOW_REDIS_STREAM="${TASKFLOW_REDIS_STREAM:-taskflow:integration:jobs}"
export TASKFLOW_REDIS_CONSUMER_GROUP="${TASKFLOW_REDIS_CONSUMER_GROUP:-taskflow-integration-workers}"

case "${DATABASE_URL##*/}" in
  *test*) ;;
  *) echo "Refusing integration tests: DATABASE_URL must name a test database." >&2; exit 2 ;;
esac

docker compose -f docker-compose.integration.yml up -d
for attempt in $(seq 1 30); do
  if docker compose -f docker-compose.integration.yml exec -T test-db \
    pg_isready -U taskflow_test -d taskflow_test >/dev/null; then
    break
  fi
  if [ "$attempt" = "30" ]; then
    echo "Test PostgreSQL did not become ready." >&2
    exit 1
  fi
  sleep 1
done
.venv/bin/alembic upgrade head
.venv/bin/python -m pytest tests/integration -q
