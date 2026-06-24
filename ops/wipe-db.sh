#!/usr/bin/env bash
#
# wipe-db.sh — fully clear the Bulletin database, then rebuild an empty schema.
#
# This destroys ALL data (subscribers, events, clusters, digests, the apalis job
# queue, ...) and re-applies migrations to leave a clean, empty schema. It is
# irreversible; a pg_dump backup is taken first.
#
# Why a script instead of running psql by hand: a bare `DROP SCHEMA public` wipe
# scatters several footguns that each surface as a confusing runtime error —
#   * the recreated `public` ends up owned by `postgres`, not `pg_database_owner`,
#     so the owner role can no longer grant USAGE and the runtime role sees
#     "relation ... does not exist";
#   * the owner role loses CREATE on the new schema, so `migrate` fails with
#     "no schema has been selected to create in";
#   * the apalis queue lives in its own `apalis` schema, untouched by a
#     public-only drop.
# This script does the wipe in the right order, ownership-preserving, and then
# ASSERTS the rebuild actually succeeded (tables present + runtime role can see
# them) before restarting the service.
#
# Usage:
#   sudo ops/wipe-db.sh                 # full wipe (drop + rebuild schema)
#   sudo ops/wipe-db.sh --truncate      # keep schema, just empty the data tables
#   sudo ops/wipe-db.sh --yes           # skip the interactive confirmation
#
# Env overrides (defaults match the NixOS module in nix/module.nix):
#   DB_NAME       (default: bulletin)
#   OWNER_ROLE    (default: bulletin)
#   RUNTIME_ROLE  (default: bulletin_app)
#   PG_SUPERUSER  (default: postgres)        OS user with DB superuser peer auth
#   PGHOST        (default: /run/postgresql) unix-socket dir the server listens on
#   PSQL          (default: autodetected)    path to the psql binary
#   PG_DUMP       (default: autodetected)    path to the pg_dump binary
#   APP_SERVICE   (default: bulletin.service)
#   MIGRATE_UNIT  (default: bulletin-migrate.service)
#   GRANT_UNIT    (default: bulletin-grant-public.service)
#   BACKUP_DIR    (default: /var/lib/bulletin/backups)

set -euo pipefail

DB_NAME="${DB_NAME:-bulletin}"
OWNER_ROLE="${OWNER_ROLE:-bulletin}"
RUNTIME_ROLE="${RUNTIME_ROLE:-bulletin_app}"
PG_SUPERUSER="${PG_SUPERUSER:-postgres}"
# Connect over the same unix socket the NixOS module wires everything else through
# (its connection strings all pin host=/run/postgresql). Passed as an explicit -h
# argument below — psql/pg_dump otherwise fall back to libpq's compile-time default
# socket dir, which on a NixOS host is not where the server listens, and the bare
# connection fails with a misleading "cannot connect".
PGHOST_DIR="${PGHOST:-/run/postgresql}"
APP_SERVICE="${APP_SERVICE:-bulletin.service}"
MIGRATE_UNIT="${MIGRATE_UNIT:-bulletin-migrate.service}"
GRANT_UNIT="${GRANT_UNIT:-bulletin-grant-public.service}"
BACKUP_DIR="${BACKUP_DIR:-/var/lib/bulletin/backups}"

# Resolve a postgres client binary. The NixOS module never puts psql/pg_dump on the
# system PATH (it always calls them by absolute store path), so a bare `psql` here is
# liable to be "command not found" — which the masked preflight used to misreport as a
# connection failure. Honor an explicit override, else PATH, else derive the package
# bindir from the running postgresql.service. Resolves to an absolute path so it works
# unchanged under `sudo -u "$PG_SUPERUSER"`.
resolve_pg_bin() {
  local name="$1" override="$2"
  if [[ -n "$override" ]]; then echo "$override"; return 0; fi
  if command -v "$name" >/dev/null 2>&1; then command -v "$name"; return 0; fi
  local bindir
  bindir="$(systemctl show -p ExecStart --value postgresql.service 2>/dev/null \
    | sed -n 's#.*path=\(/[^ ;]*\)/bin/postgres.*#\1/bin#p' | head -n1)"
  if [[ -n "$bindir" && -x "$bindir/$name" ]]; then echo "$bindir/$name"; return 0; fi
  return 1
}

if ! PSQL="$(resolve_pg_bin psql "${PSQL:-}")"; then
  echo "error: cannot find the 'psql' binary. Set PSQL=/path/to/psql." >&2
  exit 1
fi
if ! PG_DUMP="$(resolve_pg_bin pg_dump "${PG_DUMP:-}")"; then
  echo "error: cannot find the 'pg_dump' binary. Set PG_DUMP=/path/to/pg_dump." >&2
  exit 1
fi

MODE="full"
ASSUME_YES=0
for arg in "$@"; do
  case "$arg" in
    --truncate) MODE="truncate" ;;
    --yes|-y)   ASSUME_YES=1 ;;
    -h|--help)  sed -n '2,38p' "$0"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

# Run psql as the DB superuser, fail on any SQL error, quiet chrome. -h pins the unix
# socket dir so the connection doesn't depend on libpq's compile-time default.
psql_super() {
  sudo -u "$PG_SUPERUSER" "$PSQL" -h "$PGHOST_DIR" -v ON_ERROR_STOP=1 -qAt -d "$DB_NAME" "$@"
}

# --- preflight ----------------------------------------------------------------

if [[ "$(id -u)" -ne 0 ]]; then
  echo "error: run as root (it stops the service and acts as the DB superuser)." >&2
  exit 1
fi

# Identify the target so the operator can sanity-check before confirming. Surface
# psql's own error on failure instead of swallowing it — a blank "cannot connect" hides
# the actual cause (wrong socket dir, missing role, server down, ...).
probe_err="$(mktemp)"
if ! host="$(psql_super -c \
  "select coalesce(inet_server_addr()::text,'local-socket') || ' / ' || current_database();" \
  2>"$probe_err")" || [[ -z "$host" ]]; then
  echo "error: cannot connect to database '$DB_NAME' as '$PG_SUPERUSER' via socket '$PGHOST_DIR'." >&2
  if [[ -s "$probe_err" ]]; then
    echo "       psql reported:" >&2
    sed 's/^/         /' "$probe_err" >&2
  fi
  rm -f "$probe_err"
  exit 1
fi
rm -f "$probe_err"

echo "============================================================"
echo " Bulletin database wipe"
echo "   target      : $host"
echo "   db name     : $DB_NAME"
echo "   mode        : $MODE  (full = drop+rebuild schema, truncate = empty data only)"
echo "   service     : $APP_SERVICE"
echo " THIS DESTROYS ALL DATA. A backup will be taken first."
echo "============================================================"

if [[ "$ASSUME_YES" -ne 1 ]]; then
  read -r -p "Type the database name ('$DB_NAME') to confirm: " confirm
  if [[ "$confirm" != "$DB_NAME" ]]; then
    echo "aborted: confirmation did not match." >&2
    exit 1
  fi
fi

# --- 1. stop the app ----------------------------------------------------------
echo "==> stopping $APP_SERVICE"
systemctl stop "$APP_SERVICE"

# --- 2. backup (before any destructive change) --------------------------------
echo "==> backing up to $BACKUP_DIR"
mkdir -p "$BACKUP_DIR"
backup_file="$BACKUP_DIR/manual-wipe-$(date +%Y%m%dT%H%M%S).dump"
sudo -u "$PG_SUPERUSER" "$PG_DUMP" -h "$PGHOST_DIR" -Fc "$DB_NAME" > "$backup_file"
echo "    wrote $backup_file"

# --- 3. wipe ------------------------------------------------------------------
if [[ "$MODE" == "full" ]]; then
  echo "==> dropping schemas (public + apalis), recreating public (ownership-preserving)"
  # CREATE SCHEMA ... AUTHORIZATION pg_database_owner keeps the schema owned by the
  # DB owner role, so the owner role retains the implicit ownership it needs to
  # grant USAGE to the runtime role on the next migrate. (A plain CREATE SCHEMA as
  # the superuser would leave it owned by the superuser and break that path.)
  psql_super <<SQL
DROP SCHEMA IF EXISTS apalis CASCADE;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public AUTHORIZATION pg_database_owner;
GRANT USAGE, CREATE ON SCHEMA public TO $OWNER_ROLE;
SQL

  echo "==> re-running migrations"
  # The grant-public oneshot (re)grants owner CREATE; the migrate oneshot applies
  # the SQL migrations, sets up the apalis schema, and re-grants the runtime role.
  # `restart` forces the RemainAfterExit oneshots to actually re-execute.
  systemctl restart "$GRANT_UNIT"
  systemctl restart "$MIGRATE_UNIT"
else
  echo "==> truncating all data tables in schema public (RESTART IDENTITY CASCADE)"
  # Build the TRUNCATE over every base table except sqlx's bookkeeping, so the
  # migration ledger (and thus the schema) is preserved.
  psql_super <<SQL
DO \$\$
DECLARE
  tbls text;
BEGIN
  SELECT string_agg(format('%I.%I', schemaname, tablename), ', ')
    INTO tbls
    FROM pg_tables
   WHERE schemaname IN ('public', 'apalis')
     AND tablename <> '_sqlx_migrations';
  IF tbls IS NOT NULL THEN
    EXECUTE 'TRUNCATE ' || tbls || ' RESTART IDENTITY CASCADE';
  END IF;
END
\$\$;
SQL
fi

# --- 4. verify the rebuild actually worked ------------------------------------
echo "==> verifying"

# 4a. core tables exist
if ! psql_super -c "select to_regclass('public.connection') is not null;" | grep -qx 't'; then
  echo "FAIL: table public.connection missing after rebuild — check '$MIGRATE_UNIT' logs." >&2
  exit 1
fi

# 4b. the runtime role can actually SEE the schema (USAGE) — the gotcha that
#     manifests as "relation ... does not exist" at runtime.
usage=$(psql_super -c "select has_schema_privilege('$RUNTIME_ROLE','public','USAGE');")
sel=$(psql_super -c "select has_table_privilege('$RUNTIME_ROLE','public.connection','SELECT');")
if [[ "$usage" != "t" || "$sel" != "t" ]]; then
  echo "FAIL: runtime role '$RUNTIME_ROLE' lacks access (USAGE=$usage, SELECT=$sel)." >&2
  echo "      check public schema ownership and grant_runtime_role." >&2
  exit 1
fi

# 4c. data is actually gone
rows=$(psql_super -c "select coalesce((select count(*) from subscriber),0) + coalesce((select count(*) from event),0);")
echo "    tables present, runtime role has access, residual rows (subscriber+event): $rows"

# --- 5. start the app ---------------------------------------------------------
echo "==> starting $APP_SERVICE"
systemctl start "$APP_SERVICE"

echo "done. tail logs with: journalctl -u $APP_SERVICE -f"
