#!/usr/bin/env bash
# failure-recovery-demo.sh — the phase-8 end-to-end demo.
#
# One unattended run proves that a worker killed mid-provisioning is recovered
# by lease expiry and idempotent re-execution:
#   1. bring up the compose stack, wait for it and the orchestrator to be ready
#   2. create an environment, wait for `ready`
#   3. connect from a one-off psql as the provisioned role (usability)
#   4. create a second environment and kill -9 the orchestrator mid-provisioning
#   5. restart; show the stale lease expiring and the environment reaching `ready`
#   6. assert exactly one database and one role exist for the interrupted env
#   7. print the trace/metric endpoints worth inspecting afterwards
#
# Exit code 0 means the whole story held. Any failed step aborts loudly.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

API_BASE="http://localhost:8080"
# The compose postgres host port is configurable so the demo still runs when
# an unrelated process already holds 5432. DATABASE_URL below is exported for
# both `make migrate` and the orchestrator binary.
PG_HOST_PORT="${PG_HOST_PORT:-5432}"
if ! nc -z localhost "$PG_HOST_PORT" 2>/dev/null; then :; else PG_HOST_PORT=15432; fi
export DATABASE_URL="postgres://orchestrator:orchestrator@localhost:${PG_HOST_PORT}/orchestrator?sslmode=disable"
export PG_HOST_PORT
# Short lease/timeout so the demo does not sit through the production 90s lease
# after the kill. The lease must exceed the step timeout (config.Validate).
DEMO_STEP_TIMEOUT="3s"
DEMO_LEASE_DURATION="5s"
LOG_FILE="$(mktemp -t orchestrator-demo-log)"
ORCH_PID=""
ORCH_PID_FILE=""

cleanup() {
  local code=$?
  if [[ -n "$ORCH_PID" ]]; then
    kill "$ORCH_PID" 2>/dev/null || true
    wait "$ORCH_PID" 2>/dev/null || true
  fi
  if [[ $code -ne 0 ]]; then
    echo >&2
    echo "demo failed (exit $code); last orchestrator log lines:" >&2
    tail -40 "$LOG_FILE" >&2 || true
  fi
}
trap cleanup EXIT

stepn() { echo; echo "── $1 ──"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

wait_url() { # url timeout_seconds
  local url=$1 deadline=$((SECONDS + ${2:-30}))
  until curl -fsS -o /dev/null "$url" 2>/dev/null; do
    (( SECONDS < deadline )) || return 1
    sleep 0.5
  done
}

# Latest status value of GET /v1/environments/{id} (the API reports "status").
env_state() {
  curl -fsS "$API_BASE/v1/environments/$1" 2>/dev/null |
    sed -n 's/.*"status":"\([^"]*\)".*/\1/p'
}

# One-off psql against the compose postgres, used for assertions.
psql_admin() { docker compose exec -T postgres psql -U orchestrator -d orchestrator -Atq "$@"; }

wait_env_ready() { # env_id timeout_seconds
  local id=$1 deadline=$((SECONDS + ${2:-60})) got
  while :; do
    got="$(env_state "$id")"
    [[ "$got" == "ready" ]] && return 0
    case "$got" in
      failed|rolling_back|deleted|cancelled|rolled_back) return 1 ;;
    esac
    (( SECONDS < deadline )) || return 1
    sleep 0.5
  done
}

str_field() { # python-extract a string field from stdin JSON
  python3 -c 'import json, sys
f = json.load(sys.stdin)
print(f.get(sys.argv[1]) or "")' "$1"
}

# Deterministic naming mirrors internal/adapters/naming.go: prefix + "_" +
# sanitize(envID)[:28] + "_" + first 8 hex chars of the sha256 of the env id.
# A lowercase UUID keeps '-', alphanumerics are kept too, so the middle part
# is just the id's first 28 characters.
resource_name() { # prefix env_id
  local hash
  hash="$(printf '%s' "$2" | shasum -a 256 | cut -c1-8)"
  echo "${1}_${2:0:28}_${hash}"
}

metrics_counter() { # metric_name — current summed value of a counter on /metrics
  curl -fsS "$API_BASE/metrics" 2>/dev/null |
    awk -v m="$1" '$1 == m || index($1, m"{") == 1 { sum += $2 } END { print sum + 0 }'
}

start_orchestrator() {
  (
    export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317
    export STEP_TIMEOUT="$DEMO_STEP_TIMEOUT" LEASE_DURATION="$DEMO_LEASE_DURATION"
    exec ./bin/orchestrator
  ) >>"$LOG_FILE" 2>&1 &
  ORCH_PID=$!
}

stepn "1/7 bring up the compose stack, wait for readiness"
docker compose up -d
READY=0
for _ in $(seq 1 60); do
  if docker compose exec -T postgres pg_isready -U orchestrator -d orchestrator >/dev/null 2>&1; then
    READY=1; break
  fi
  sleep 1
done
[[ "$READY" == "1" ]] || fail "postgres never became ready"
READY=0
for _ in $(seq 1 60); do
  if docker compose exec -T vault vault status >/dev/null 2>&1; then
    READY=1; break
  fi
  sleep 1
done
[[ "$READY" == "1" ]] || fail "vault never became ready"

echo "Applying database migrations"
make migrate

echo "Building and starting the orchestrator on :8080"
make build
start_orchestrator
wait_url "$API_BASE/healthz" 30 || { tail -20 "$LOG_FILE" >&2; fail "orchestrator never became healthy"; }
wait_url "$API_BASE/readyz" 30 || fail "orchestrator dependencies never became ready"

create_env() { # tenant name idempotency_key
  curl -fsS -X POST "$API_BASE/v1/environments" \
    -H "Idempotency-Key: $3" -H 'Content-Type: application/json' \
    -d "{\"tenantId\":\"$1\",\"name\":\"$2\",\"spec\":{}}"
}

credentials_for() { # env_id
  curl -fsS -X POST "$API_BASE/v1/environments/$1/credentials" \
    -H 'X-Caller-Id: operator'
}

stepn "2/7 create an environment, wait for ready"
ENV1_ID="$(create_env demo-tenant demo-clean "demo-clean-e2e-$SECONDS-$$" | str_field environmentId)"
[[ -n "$ENV1_ID" ]] || fail "environment one was not accepted"
echo "environment one: $ENV1_ID"
wait_env_ready "$ENV1_ID" 60 || fail "environment one never reached ready"

stepn "3/7 connect from a one-off psql as the provisioned role to prove usability"
read -r CREDS_DB CREDS_USER CREDS_PASS < <(credentials_for "$ENV1_ID" | {
  python3 -c 'import json, sys
f = json.load(sys.stdin)
print(f.get("database") or "", f.get("username") or "", f.get("password") or "")'
})
[[ -n "$CREDS_DB" && -n "$CREDS_USER" && -n "$CREDS_PASS" ]] || fail "could not retrieve credentials for environment one"
docker compose exec -T -e PGPASSWORD="$CREDS_PASS" postgres \
  psql -h 127.0.0.1 -U "$CREDS_USER" -d "$CREDS_DB" \
  -c 'select current_user, current_database();' >/dev/null \
  || fail "the provisioned role could not connect to its database"
echo "provisioned role $CREDS_USER queried $CREDS_DB successfully"

stepn "4/7 create a second environment, then kill -9 the orchestrator mid-provisioning"

# Start BEFORE the create: docker exec round trips are slower than the whole
# workflow, so a poll-per-iteration from this script would routinely miss it.
# A single psql session inside the postgres container polls the row
# server-side every 10 ms (\watch), streaming to a file. Once it reports a
# running step with a live owner for the environment being created,
# SIGKILL lands on an orphanable, in-flight step. A workflow that reaches
# ready with no observed live lease fails loudly: the kill would prove
# nothing.
WATCH_OUT="$(mktemp -t demo-lease-watch)"
WATCH_PID=""
WATCH_SQL="SELECT count(*) FROM workflow_steps ws
JOIN environments e ON e.id = ws.environment_id
WHERE e.name = 'demo-interrupted' AND e.state NOT IN ('ready', 'failed', 'deleted', 'cancelled', 'rolled_back')
  AND ws.state = 'running' AND ws.lease_owner <> ''"
cleanup_watch() {
  if [[ -n "$WATCH_PID" ]]; then
    kill "$WATCH_PID" 2>/dev/null || true
    wait "$WATCH_PID" 2>/dev/null || true
  fi
}
trap 'cleanup_watch; cleanup' EXIT
( docker compose exec -T postgres psql -U orchestrator -d orchestrator -Atq \
    >"$WATCH_OUT" 2>/dev/null <<SQL
$WATCH_SQL
\watch 0.01
SQL
) &
WATCH_PID=$!

ENV2_ID="$(create_env demo-tenant demo-interrupted "demo-interrupted-e2e-$SECONDS-$$" | str_field environmentId)"
[[ -n "$ENV2_ID" ]] || { cleanup_watch; fail "environment two was not accepted"; }
echo "environment two: $ENV2_ID"

KILLABLE=0
for _ in $(seq 1 600); do
  if grep -qvx '0' "$WATCH_OUT" 2>/dev/null; then
    KILLABLE=1
    echo "a step of $ENV2_ID now holds a live lease"
    break
  fi
  [[ "$(env_state "$ENV2_ID")" != "ready" ]] || break
  sleep 0.05
done
cleanup_watch
[[ "$KILLABLE" == "1" ]] ||
  fail "no step of environment two was observed holding a live lease before it reached ready; killing now proves nothing"

echo "environment two is mid-provisioning with a held lease; sending SIGKILL to the orchestrator"
LEASES_BEFORE="$(metrics_counter provisioning_lease_expirations_total)"
kill -9 "$ORCH_PID" 2>/dev/null || true
wait "$ORCH_PID" 2>/dev/null || true
ORCH_PID=""
echo "orchestrator killed while $ENV2_ID was provisioning"

stepn "5/7 restart; the orphaned step's lease expires and it is re-executed"
start_orchestrator
wait_url "$API_BASE/healthz" 30 || { tail -20 "$LOG_FILE" >&2; fail "restarted orchestrator never became healthy"; }
wait_url "$API_BASE/readyz" 30 || fail "restarted orchestrator dependencies never became ready"
echo "waiting for the stale lease (≤ ${DEMO_LEASE_DURATION}) to be reclaimed and the workflow to finish…"
wait_env_ready "$ENV2_ID" 60 || fail "environment two never reached ready after the crash"
echo "environment two reached ready after restart"

# The recovery is only proven observable if the lease-expiration counter moved:
# the reclaim that re-executed the orphaned step must be counted.
LEASES_AFTER="$(metrics_counter provisioning_lease_expirations_total)"
[[ "${LEASES_AFTER:-0}" -gt "${LEASES_BEFORE:-0}" ]] ||
  fail "provisioning_lease_expirations_total did not grow after restart (before=$LEASES_BEFORE after=$LEASES_AFTER); the reclaim that recovered the SIGKILL was not counted"
echo "lease expiration counter grew ($LEASES_BEFORE -> $LEASES_AFTER): the reclaim was counted"

read -r DB2 ROLE2 PASS2 < <(credentials_for "$ENV2_ID" | {
  python3 -c 'import json, sys
f = json.load(sys.stdin)
print(f.get("database") or "", f.get("username") or "", f.get("password") or "")'
})
[[ -n "$DB2" && -n "$ROLE2" && -n "$PASS2" ]] || fail "could not retrieve credentials after recovery"

stepn "6/7 assert exactly one database and one role exist for the interrupted environment"
DB_EXPECTED="$(resource_name px "$ENV2_ID")"
ROLE_EXPECTED="$(resource_name pu "$ENV2_ID")"
DB_COUNT="$(psql_admin -c "SELECT count(*) FROM pg_database WHERE datname = '$DB_EXPECTED'")"
ROLE_COUNT="$(psql_admin -c "SELECT count(*) FROM pg_roles WHERE rolname = '$ROLE_EXPECTED'")"
[[ "$DB_COUNT" == "1" ]] || fail "expected exactly one database named $DB_EXPECTED, found $DB_COUNT"
[[ "$ROLE_COUNT" == "1" ]] || fail "expected exactly one role named $ROLE_EXPECTED, found $ROLE_COUNT"
# And the recovered credential must actually work.
docker compose exec -T -e PGPASSWORD="$PASS2" postgres \
  psql -h 127.0.0.1 -U "$ROLE2" -d "$DB2" -c 'select 1;' >/dev/null \
  || fail "the recovered environment is not usable with its provisioned role"
echo "exactly one database ($DB_EXPECTED) and one role ($ROLE_EXPECTED) for $ENV2_ID, and it is usable"

stepn "7/7 done — where to look next"
cat <<EOF

Traces and metrics produced by this run:
  traces            one trace per environment provisioning, one span per
                    step attempt, exported OTLP to localhost:4317 (see the
                    compose otel-collector; a local Jaeger/Grafana Tempo can
                    point its OTLP receiver there as well)
  raw metrics       curl http://localhost:8080/metrics
                    provisioning_lease_expirations_total counted the reclaim
                    that recovered the SIGKILL above (asserted in step 5/6)
  prometheus        http://localhost:9090
  grafana dashboard deploy/dashboard.json (datasource: Prometheus on :9090)
  runbook           docs/runbook.md
demo orchestrator log (kept): $LOG_FILE
EOF

rm -f "$ORCH_PID_FILE"
exit 0
