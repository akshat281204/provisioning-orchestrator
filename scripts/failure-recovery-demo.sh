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
DEMO_STEP_PAUSE="750ms"
LOG_FILE="$(mktemp -t orchestrator-demo-log)"
ORCH_PID=""
ORCH_PID_FILE=""
WATCH_PID=""

cleanup() {
  # The caller's status, saved before cleanup_watch can clobber $?.
  local code=${1:-$?}
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

cleanup_watch() {
  if [[ -n "$WATCH_PID" ]]; then
    kill "$WATCH_PID" 2>/dev/null || true
    wait "$WATCH_PID" 2>/dev/null || true
  fi
}

# One trap, and it saves $? before any cleanup function runs, so a failed
# run still prints the orchestrator log tail.
trap 'code=$?; cleanup_watch; cleanup "$code"' EXIT

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

start_orchestrator() {
  (
    export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317
    export STEP_TIMEOUT="$DEMO_STEP_TIMEOUT" LEASE_DURATION="$DEMO_LEASE_DURATION"
    # Deterministic interception: the verify step holds its running window
    # for at least the pause, so the watcher sees a live lease and the
    # kill lands mid-flight instead of racing the fast workflow.
    export STEP_PAUSE="$DEMO_STEP_PAUSE" STEP_PAUSE_KINDS=verify_usable
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

# Start BEFORE the create and handshake before creating anything: docker
# exec + psql startup latency (~100 ms) used to exceed the whole workflow,
# so a poll-per-iteration from this script would routinely miss it. A single
# psql session inside the postgres container polls every 10 ms (\watch),
# streaming to a file; once it streams, the environment is created. The
# verify step additionally holds its running window for STEP_PAUSE, so a
# running step with a live owner is observed for many consecutive polls and
# SIGKILL lands on an orphanable, in-flight step. A workflow that reaches
# ready with no observed live lease fails loudly: the kill would prove
# nothing.
WATCH_OUT="$(mktemp -t demo-lease-watch)"
DEMO_NAME="demo-interrupted-$SECONDS-$$"
# The seed row makes the watcher emit a verdict every 10 ms even before the
# environment exists ('WAIT'), so the shell can handshake on the first WAIT
# and create the environment only once psql is verifiably streaming — no
# poll is ever lost to docker/psql startup latency.
WATCH_SQL="SELECT CASE
  WHEN e.id IS NULL THEN 'WAIT'
  WHEN e.state = 'ready' THEN 'READY'
  WHEN ws.id IS NOT NULL THEN 'RUNNING|' || ws.id::text || '|' || ws.attempts::text
  ELSE 'WAIT' END
FROM (VALUES (1)) AS seed
LEFT JOIN environments e ON e.name = '$DEMO_NAME'
LEFT JOIN LATERAL (SELECT id, attempts FROM workflow_steps
  WHERE environment_id = e.id AND state = 'running' AND lease_owner <> ''
  ORDER BY step_index LIMIT 1) ws ON true"
( docker compose exec -T postgres psql -U orchestrator -d orchestrator -Atq \
    >"$WATCH_OUT" 2>/dev/null <<SQL
$WATCH_SQL
\watch 0.01
SQL
) &
WATCH_PID=$!

# Handshake: do not create environment two until the watcher has streamed
# its first WAIT, proving it is polling. STEP_PAUSE (see start_orchestrator)
# then holds the verify step in its running window long enough that the
# observation, the authoritative DB re-read, and the kill cannot lose the
# race against the workflow.
HANDSHAKE=0
for _ in $(seq 1 200); do
  if grep -q '^WAIT$' "$WATCH_OUT" 2>/dev/null; then HANDSHAKE=1; break; fi
  sleep 0.05
done
[[ "$HANDSHAKE" == "1" ]] || { cleanup_watch; fail "lease watcher never started streaming"; }

ENV2_ID="$(create_env demo-tenant "$DEMO_NAME" "demo-interrupted-e2e-$SECONDS-$$" | str_field environmentId)"
[[ -n "$ENV2_ID" ]] || { cleanup_watch; fail "environment two was not accepted"; }
echo "environment two: $ENV2_ID"

KILLABLE=0
RECLAIM_STEP_ID=""
RECLAIM_ATTEMPTS=""
for _ in $(seq 1 2000); do
  OBSERVED="$(grep -E '^RUNNING\|' "$WATCH_OUT" 2>/dev/null | tail -n 1 || true)"
  if [[ -n "$OBSERVED" ]]; then
    # Freeze the process immediately, then read the authoritative current row.
    # The watcher line may already be stale by the time this shell is scheduled;
    # SIGSTOP prevents a subsequent step from finishing during confirmation.
    kill -STOP "$ORCH_PID" 2>/dev/null || true
    RUNNING_ROW="$(psql_admin -c "SELECT ws.id || '|' || ws.attempts FROM workflow_steps ws JOIN environments e ON e.id = ws.environment_id WHERE e.id = '$ENV2_ID' AND ws.state = 'running' AND ws.lease_owner <> '' ORDER BY ws.step_index LIMIT 1")"
    if [[ -n "$RUNNING_ROW" ]]; then
      IFS='|' read -r RECLAIM_STEP_ID RECLAIM_ATTEMPTS <<< "$RUNNING_ROW"
      [[ -n "$RECLAIM_STEP_ID" && "$RECLAIM_ATTEMPTS" =~ ^[0-9]+$ ]] ||
        fail "database reported an invalid running-step record: $RUNNING_ROW"
      KILLABLE=1
      kill -9 "$ORCH_PID" 2>/dev/null || true
      wait "$ORCH_PID" 2>/dev/null || true
      ORCH_PID=""
      break
    fi
    # A stale watcher row can race a fast step completion. Resume the process
    # and keep watching rather than falsely claiming a successful fault test.
    kill -CONT "$ORCH_PID" 2>/dev/null || true
  fi
  # The server-side watcher reports READY once this exact run is already ready;
  # fail immediately rather than waiting out the full observation window.
  grep -q '^READY$' "$WATCH_OUT" 2>/dev/null && break
  sleep 0.001
done
cleanup_watch
[[ "$KILLABLE" == "1" ]] ||
  fail "no step of environment two was observed holding a live lease before it reached ready; killing now proves nothing"

echo "orchestrator killed immediately after observing a live step lease for $ENV2_ID"

stepn "5/7 restart; the orphaned step's lease expires and it is re-executed"
start_orchestrator
wait_url "$API_BASE/healthz" 30 || { tail -20 "$LOG_FILE" >&2; fail "restarted orchestrator never became healthy"; }
wait_url "$API_BASE/readyz" 30 || fail "restarted orchestrator dependencies never became ready"
echo "waiting for the stale lease (≤ ${DEMO_LEASE_DURATION}) to be reclaimed and the workflow to finish…"
wait_env_ready "$ENV2_ID" 60 || fail "environment two never reached ready after the crash"
echo "environment two reached ready after restart"

# The process-local counter resets on SIGKILL, so prove recovery from the
# durable attempt count captured before the kill instead of comparing metrics
# across process lifetimes.
ATTEMPTS_AFTER="$(psql_admin -c "SELECT attempts FROM workflow_steps WHERE id = '$RECLAIM_STEP_ID'")"
[[ "$ATTEMPTS_AFTER" =~ ^[0-9]+$ && "$ATTEMPTS_AFTER" -gt "$RECLAIM_ATTEMPTS" ]] ||
  fail "the orphaned step was not reclaimed (step=$RECLAIM_STEP_ID attempts=$RECLAIM_ATTEMPTS->$ATTEMPTS_AFTER)"
echo "orphaned step $RECLAIM_STEP_ID was reclaimed (attempts $RECLAIM_ATTEMPTS -> $ATTEMPTS_AFTER)"

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
                    step attempt, exported OTLP to the compose
                    otel-collector on localhost:4317; inspect them in the
                    collector's debug exporter output
                    (docker compose logs otel-collector), or point a local
                    Jaeger/Grafana Tempo at that OTLP receiver
  raw metrics       curl http://localhost:8080/metrics
                    lease-expiration counters are process-local and reset on
                    restart; durable reclaim proof is the step attempt count
                    printed in step 5
  prometheus        http://localhost:9090
  grafana dashboard deploy/dashboard.json (datasource: Prometheus on :9090)
  runbook           docs/runbook.md
demo orchestrator log (kept): $LOG_FILE
EOF

rm -f "$ORCH_PID_FILE"
exit 0
