#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PGVECTOR_IMAGE="pgvector/pgvector@sha256:a36250871de0833b8757561c72f2477ef1ddd1101afa4e617fb552e0de514c6b"
fixture_container="ccf-pgvector-fixture-$$-${RANDOM}"
fixture_password="ccf-fixture-only"

cleanup() {
  if ! docker rm --force "$fixture_container" >/dev/null 2>&1; then
    echo "WARNING: failed to remove PostgreSQL fixture container $fixture_container" >&2
  fi
}
trap cleanup EXIT

docker run --detach --rm --pull=missing \
  --name "$fixture_container" \
  --env POSTGRES_PASSWORD="$fixture_password" \
  --env POSTGRES_DB=ccf_fixture \
  --publish 127.0.0.1::5432 \
  "$PGVECTOR_IMAGE" >/dev/null

for _ in $(seq 1 120); do
  if docker exec "$fixture_container" pg_isready --username postgres --dbname ccf_fixture >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
docker exec "$fixture_container" pg_isready --username postgres --dbname ccf_fixture >/dev/null

fixture_port="$(docker port "$fixture_container" 5432/tcp | sed -n 's/.*://p' | tail -1)"
fixture_dsn="postgresql://postgres:${fixture_password}@127.0.0.1:${fixture_port}/ccf_fixture"

PGPASSWORD="$fixture_password" psql "$fixture_dsn" --set ON_ERROR_STOP=1 <<'SQL' >/dev/null
CREATE SCHEMA ccf_runtime;
CREATE SCHEMA vector_extensions;
CREATE EXTENSION vector WITH SCHEMA vector_extensions;
SQL

PGPASSWORD="$fixture_password" \
PGOPTIONS='-c search_path=ccf_runtime,pg_catalog' \
  psql "$fixture_dsn" --set ON_ERROR_STOP=1 --file "$ROOT/sql/postgres-reference.sql" >/dev/null

fixture_result="$(PGPASSWORD="$fixture_password" PGOPTIONS='-c search_path=pg_catalog' \
  psql "$fixture_dsn" --no-align --tuples-only --set ON_ERROR_STOP=1 <<'SQL'
SELECT
  (SELECT n.nspname = 'vector_extensions'
     FROM pg_extension AS e
     JOIN pg_namespace AS n ON n.oid = e.extnamespace
    WHERE e.extname = 'vector')
  AND EXISTS (
    SELECT 1 FROM pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = 'ccf_runtime' AND c.relname = 'projection_embedding'
  )
  AND EXISTS (
    SELECT 1 FROM pg_attribute AS a
    JOIN pg_class AS c ON c.oid = a.attrelid
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    JOIN pg_type AS t ON t.oid = a.atttypid
    JOIN pg_namespace AS tn ON tn.oid = t.typnamespace
    WHERE n.nspname = 'ccf_runtime'
      AND c.relname = 'projection_embedding'
      AND a.attname = 'embedding'
      AND tn.nspname = 'vector_extensions'
      AND t.typname = 'vector'
  );
SQL
)"

if [[ "$fixture_result" != "t" ]]; then
  echo "FAIL: multi-schema pgvector fixture did not create the qualified projection" >&2
  exit 1
fi

echo "Multi-schema PostgreSQL pgvector conformance case passes."
