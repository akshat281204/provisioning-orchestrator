#!/usr/bin/env bash
# benchmark.sh — the phase-9 benchmark: run the load profile against the
# compose stack and record the result with the machine and configuration
# context needed to read it honestly (CPU, memory, Go version, Postgres
# settings, concurrency).
#
# Reproducibility: the load profile is fully parameterized by environment
# variables with fixed defaults (LOAD_ENVS=16, LOAD_RATE_PER_S=4,
# LOAD_WORKERS=4); the same command on the same stack produces a comparable
# run. The output file lands in benchmark-results/ with a timestamp.
#
# Usage: make benchmark   (requires `make up` first)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RESULTS_DIR="$ROOT/benchmark-results"
mkdir -p "$RESULTS_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_FILE="$RESULTS_DIR/benchmark-$STAMP.txt"
JSON_FILE="$RESULTS_DIR/load-report-$STAMP.json"

# --- dependency check ---------------------------------------------------------
# The Postgres host port comes from compose itself, never from probing: on a
# host where 5432 was free, `make up` binds it to 5432, and a naive "5432 is
# open, so compose must be on 15432" guess points the benchmark at nothing.
PG_HOST_PORT="${PG_HOST_PORT:-}"
if [ -z "$PG_HOST_PORT" ]; then
  if port_out="$(docker compose port postgres 5432 2>/dev/null)" && [ -n "$port_out" ]; then
    PG_HOST_PORT="${port_out##*:}"
  else
    # No compose container (or docker not running): fall back to probing the
    # known ports with a real connect, 5432 first.
    for p in 5432 15432; do
      if nc -z localhost "$p" 2>/dev/null; then PG_HOST_PORT="$p"; break; fi
    done
  fi
fi
if [ -z "$PG_HOST_PORT" ] || ! nc -z localhost "$PG_HOST_PORT" 2>/dev/null; then
  echo "postgres not reachable — run \`make up\` first" >&2
  exit 1
fi
if ! nc -z localhost 8200 2>/dev/null; then
  echo "vault is not reachable on :8200 — run \`make up\` first" >&2
  exit 1
fi
export TEST_VAULT_ADDR="${TEST_VAULT_ADDR:-http://127.0.0.1:8200}"
if [ -z "${TEST_DATABASE_URL:-}" ]; then
  export TEST_DATABASE_URL="postgres://orchestrator:orchestrator@localhost:${PG_HOST_PORT}/orchestrator?sslmode=disable"
fi

# --- environment context ------------------------------------------------------
context_file="$RESULTS_DIR/environment-$STAMP.txt"
{
  echo "benchmark environment context"
  echo "date:            $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git commit:      $(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "git dirty:       $(git status --porcelain 2>/dev/null | wc -l | tr -d ' ') changed files"
  echo "os/arch:         $(uname -s)/$(uname -m)"
  echo "cpu count:       $(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu)"
  echo "memory:          $(sysctl -n hw.memsize 2>/dev/null || grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}')"
  echo "go version:      $(go version | awk '{print $3}')"
  echo "load profile:    LOAD_ENVS=${LOAD_ENVS:-16} LOAD_RATE_PER_S=${LOAD_RATE_PER_S:-4} LOAD_WORKERS=${LOAD_WORKERS:-4}"
  echo "database url:    $TEST_DATABASE_URL"
  echo "vault addr:      $TEST_VAULT_ADDR"
  echo
  echo "postgres settings (via docker compose exec):"
  for setting in server_version shared_buffers max_connections work_mem maintenance_work_mem synchronous_commit fsync full_page_writes; do
    value="$(docker compose exec -T postgres psql -U orchestrator -d orchestrator -tAc "show $setting" 2>/dev/null || echo unavailable)"
    echo "  $setting: $value"
  done
} | tee "$context_file"

# --- run the load profile -----------------------------------------------------
echo "running the load profile (results: $RESULTS_FILE)..."
# pipefail (set above) propagates the test's exit status through tee: a
# failed run fails the benchmark. A skipped run is caught below, because a
# skip writes no JSON report.
if ! LOAD_RESULTS_FILE="$JSON_FILE" \
  go test -p 1 -count=1 -tags=integration -v -timeout 20m ./test/load 2>&1 | tee "$RESULTS_FILE"; then
  echo "load test FAILED — see $RESULTS_FILE" >&2
  exit 1
fi

# A skipped load test ("postgres not reachable, skipping") also exits 0, so
# gate on the machine-readable report actually existing.
if [ ! -s "$JSON_FILE" ]; then
  echo "load test produced no JSON report at $JSON_FILE — it likely skipped." >&2
  echo "Check that postgres and vault are up (\`make up\`) and that TEST_DATABASE_URL is correct." >&2
  exit 1
fi

# The test itself asserts the guarantees (100% ready, zero duplicates); its
# exit code above is the gate. Surface the headline numbers too.
grep -E "ready rate|time-to-ready|duplicate resources" "$RESULTS_FILE" || true

echo
echo "results written:"
echo "  $RESULTS_FILE"
echo "  $JSON_FILE"
echo "  $context_file"
