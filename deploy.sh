#!/usr/bin/env bash
# deploy.sh — build, upload and create (and optionally run) one example as a graal job.
#
#   ./deploy.sh <example-dir> [options]
#
# Options:
#   -p, --project ID     project to create the job in            (or $PROJECT_ID)
#   -i, --identity ID    identity the job runs as                (or $IDENTITY_ID)
#   -f, --job-file FILE  job definition inside the example       (default: job.json)
#       --run            start a run right away and stream its logs; the exit code is the run's
#       --no-build       skip the example's build step
#       --dry-run        build and print the job that would be created; upload nothing, create nothing
#   -h, --help
#
# Each example has a `graal.env` next to its `job.json`, read by this script:
#   BUILD='mvn -B -q -DskipTests package'        # optional, run in the example directory
#   LIBRARIES=(LIBRARY_KEY=target/app.jar ...)   # files to upload; <LIBRARY_KEY> gets the key
#   REQUIRES=(BUCKET ...)                        # other <PLACEHOLDERS>, taken from the environment
#
# Every `<NAME>` left in the job file after that is an error: nothing half-filled is ever sent.
# Prerequisites: graalctl, logged in (`graalctl login`); the example's own build tools.
set -euo pipefail

usage() { sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; }
die() { printf 'deploy.sh: %s\n' "$*" >&2; exit 1; }

EXAMPLE="" JOB_FILE=job.json RUN=0 BUILD_STEP=1 DRY=0
PROJECT="${PROJECT_ID:-}" IDENTITY="${IDENTITY_ID:-}"
while [ $# -gt 0 ]; do
  case "$1" in
    -p|--project) PROJECT=${2:?}; shift 2 ;;
    -i|--identity) IDENTITY=${2:?}; shift 2 ;;
    -f|--job-file) JOB_FILE=${2:?}; shift 2 ;;
    --run) RUN=1; shift ;;
    --no-build) BUILD_STEP=0; shift ;;
    --dry-run) DRY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) die "unknown option $1 (see --help)" ;;
    *) [ -z "$EXAMPLE" ] || die "one example at a time"; EXAMPLE=$1; shift ;;
  esac
done
[ -n "$EXAMPLE" ] || { usage; exit 2; }
ROOT=$(cd "$(dirname "$0")" && pwd)
DIR=$(cd "$ROOT/$EXAMPLE" 2>/dev/null && pwd) || DIR=$(cd "$EXAMPLE" 2>/dev/null && pwd) || die "no example $EXAMPLE"
[ -f "$DIR/$JOB_FILE" ] || die "$DIR/$JOB_FILE not found"
[ -f "$DIR/graal.env" ] || die "$DIR/graal.env not found: this directory is not a deployable example"

BUILD="" LIBRARIES=() REQUIRES=()
# shellcheck source=/dev/null
. "$DIR/graal.env"

if [ "$DRY" = 0 ]; then
  command -v graalctl >/dev/null || die "graalctl not found: https://github.com/graalsystems/graalctl"
  [ -n "$PROJECT" ] || die "no project: --project ID or PROJECT_ID (graalctl projects list)"
fi

if [ "$BUILD_STEP" = 1 ] && [ -n "$BUILD" ]; then
  echo "==> build: $BUILD"
  (cd "$DIR" && bash -c "$BUILD")
fi

job=$(cat "$DIR/$JOB_FILE")
fill() { # fill NAME VALUE — replace every <NAME>
  local name=$1 value=$2
  job=${job//"<$name>"/$value}
}

for entry in "${LIBRARIES[@]}"; do
  name=${entry%%=*} path=${entry#*=}
  if [ ! -f "$DIR/$path" ]; then
    [ "$DRY" = 1 ] || die "$path not found in $EXAMPLE (build it, or drop --no-build)"
    echo "deploy.sh: (dry run) $path not built yet" >&2
  fi
  if [ "$DRY" = 1 ]; then
    fill "$name" "dry-run-$(basename "$path")"
  else
    echo "==> upload: $path"
    key=$(cd "$DIR" && graalctl libraries upload "$path")
    [ -n "$key" ] || die "upload of $path returned no key"
    fill "$name" "$key"
  fi
done

for name in "${REQUIRES[@]}"; do
  value=${!name:-}
  if [ -z "$value" ]; then
    [ "$DRY" = 1 ] && value="dry-run-$name" || die "this example needs \$$name (see its README)"
  fi
  fill "$name" "$value"
done

if [ -n "$IDENTITY" ]; then
  fill IDENTITY_ID "$IDENTITY"
else
  # No identity given: drop the line rather than send a placeholder (jobs without identity are allowed
  # when they read no secret and no bucket).
  job=$(printf '%s\n' "$job" | grep -v '"identity_id": "<IDENTITY_ID>"')
fi

left=$(printf '%s' "$job" | grep -o '<[A-Z][A-Z0-9_]*>' | sort -u | tr '\n' ' ' || true)
[ -z "$left" ] || die "placeholders left in $JOB_FILE: $left"

if [ "$DRY" = 1 ]; then
  printf '%s\n' "$job"
  exit 0
fi

tmp=$(mktemp "${TMPDIR:-/tmp}/graal-job.XXXXXX.json")
trap 'rm -f "$tmp"' EXIT
printf '%s\n' "$job" > "$tmp"
JOB=$(graalctl jobs create --project "$PROJECT" -f "$tmp" -o id)
echo "==> job created: $JOB"
if [ "$RUN" = 1 ]; then
  exec graalctl jobs run "$JOB" --follow
fi
echo "    run it with: graalctl jobs run $JOB --follow"
