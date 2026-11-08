#!/usr/bin/env bash
# deploy-project.sh — deploy one example PROJECT into graal: the project, an identity to run its jobs,
# the rights that identity needs, the jobs (code uploaded as libraries) and the workflow that chains them.
#
#   projects/deploy-project.sh <project-dir> [options]
#
# Options:
#   -i, --identity ID   run the jobs as this existing identity: no identity and no rights are created
#       --run           run the workflow once deployed and wait for its outcome (exit code = outcome)
#       --dry-run       print what would be sent; send nothing
#   -h, --help
#
# Re-running it UPDATES what is already there (found by name): job code, options and workflow tasks are
# replaced, runs and their history are kept. Nothing is ever deleted.
#
# A project directory holds:
#   project.json    {"name", "description", "labels", "jobs": [<job dir>, ...]}   (jobs in creation order)
#   jobs/<job>/     job.json + graal.env (+ code), the same format as the single-job examples of this repo
#   workflow.json   the workflow; a task's "ref": "<JOB:<job dir>>" becomes the id of that job
#
# Without --identity, the script creates the identity `<project name>-runner` and gives it: Project
# Contributor on the project, Job Reader on each job, Catalog Contributor on the tenant (read and write
# the Iceberg tables by name) and Infrastructure Reader on the infrastructure. Granting rights needs a
# tenant administrator; otherwise ask one for an identity and pass it with --identity.
#
# Prerequisites: graalctl logged in (`graalctl login`, or GRAAL_TOKEN / GRAAL_CLIENT_ID+SECRET), jq.
# GRAAL_INFRASTRUCTURE (or graalctl's context) names the infrastructure when there is more than one.
set -euo pipefail

usage() { sed -n '2,27p' "$0" | sed 's/^# \{0,1\}//'; }
die() { printf 'deploy-project.sh: %s\n' "$*" >&2; exit 1; }
say() { printf '==> %s\n' "$*"; }

DIR="" IDENTITY="${IDENTITY_ID:-}" RUN=0 DRY=0
while [ $# -gt 0 ]; do
  case "$1" in
    -i|--identity) IDENTITY=${2:?}; shift 2 ;;
    --run) RUN=1; shift ;;
    --dry-run) DRY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) die "unknown option $1 (see --help)" ;;
    *) [ -z "$DIR" ] || die "one project at a time"; DIR=$1; shift ;;
  esac
done
[ -n "$DIR" ] || { usage; exit 2; }
HERE=$(cd "$(dirname "$0")" && pwd)
DIR=$(cd "$DIR" 2>/dev/null && pwd) || DIR=$(cd "$HERE/$DIR" 2>/dev/null && pwd) || die "no project $DIR"
[ -f "$DIR/project.json" ] || die "$DIR/project.json not found: not a project directory"
command -v jq >/dev/null || die "jq not found"
[ "$DRY" = 1 ] || command -v graalctl >/dev/null || die "graalctl not found: https://github.com/graalsystems/graalctl"

NAME=$(jq -r .name "$DIR/project.json")
mapfile -t JOBS < <(jq -r '.jobs[]' "$DIR/project.json")
TMP=$(mktemp -d "${TMPDIR:-/tmp}/graal-project.XXXXXX")
trap 'rm -rf "$TMP"' EXIT

# Role ids by name, from the installation (never hard-coded).
role() {
  graalctl api /roles -q size=200 -o json | jq -r --arg n "$1" '(.content? // .)[] | select(.name == $n) | .id' | head -1
}
# POST /assignments unless the same one exists.
grant() { # grant <role name> <principal id> <resource type> <resource id>
  local rid existing
  rid=$(role "$1"); [ -n "$rid" ] || die "role '$1' not found"
  existing=$(graalctl api /assignments -q "resource_type=$3" -q "resource_id=$4" -o json 2>/dev/null \
    | jq -r --arg r "$rid" --arg p "$2" '(.content? // .)[] | (.assignment // .) | select(.role_id == $r and .principal_id == $p) | .id' \
    | head -1 || true)
  if [ -n "$existing" ]; then echo "    $1 on $3 already granted"; return; fi
  jq -n --arg r "$rid" --arg p "$2" --arg t "$3" --arg i "$4" \
    '{role_id: $r, principal_type: "client", principal_id: $p, resource_type: $t, resource_id: $i}' > "$TMP/assignment.json"
  graalctl api /assignments -k assignment -d @"$TMP/assignment.json" -o json >/dev/null
  echo "    $1 on $3 granted"
}

# ── Jobs: build the definitions first (fails before anything is created if a placeholder is left) ─
declare -A JOB_FILE
for job in "${JOBS[@]}"; do
  jdir="$DIR/jobs/$job"
  [ -f "$jdir/job.json" ] && [ -f "$jdir/graal.env" ] || die "jobs/$job: job.json or graal.env missing"
  BUILD="" LIBRARIES=() REQUIRES=()
  # shellcheck source=/dev/null
  . "$jdir/graal.env"
  [ -z "$BUILD" ] || { say "build $job: $BUILD"; (cd "$jdir" && bash -c "$BUILD"); }
  def=$(cat "$jdir/job.json")
  for entry in "${LIBRARIES[@]}"; do
    var=${entry%%=*} path=${entry#*=}
    [ -f "$jdir/$path" ] || die "jobs/$job: $path not found"
    if [ "$DRY" = 1 ]; then key="dry-run-$(basename "$path")"; else
      key=$(cd "$jdir" && graalctl libraries upload "$path"); [ -n "$key" ] || die "upload of $path returned no key"
      echo "    uploaded jobs/$job/$path"
    fi
    def=${def//"<$var>"/$key}
  done
  for var in "${REQUIRES[@]}"; do
    [ -n "${!var:-}" ] || die "jobs/$job needs \$$var (see the project's README)"
    def=${def//"<$var>"/${!var}}
  done
  printf '%s\n' "$def" > "$TMP/job-$job.json"
  JOB_FILE[$job]="$TMP/job-$job.json"
done

if [ "$DRY" = 1 ]; then
  say "project $NAME"; jq -c 'del(.jobs)' "$DIR/project.json"
  for job in "${JOBS[@]}"; do say "job $job"; cat "${JOB_FILE[$job]}"; done
  say "workflow"; cat "$DIR/workflow.json"
  exit 0
fi

# ── Infrastructure (an id or a name; the only one when there is a single one) ──────────────────────
INFRA=${GRAAL_INFRASTRUCTURE:-}
if [ -z "$INFRA" ]; then
  INFRA=$(graalctl infrastructures list -o json | jq -r '(.content? // .) | if length == 1 then .[0].id else empty end')
  [ -n "$INFRA" ] || die "several infrastructures: set GRAAL_INFRASTRUCTURE (graalctl infrastructures list)"
elif ! [[ $INFRA =~ ^[0-9a-f-]{36}$ ]]; then
  INFRA=$(graalctl infrastructures list -o json | jq -r --arg n "$INFRA" '(.content? // .)[] | select(.name == $n) | .id' | head -1)
  [ -n "$INFRA" ] || die "no infrastructure named $GRAAL_INFRASTRUCTURE"
fi
export GRAAL_INFRASTRUCTURE=$INFRA

# ── Project ────────────────────────────────────────────────────────────────────────────────────────
PROJECT=$(graalctl projects list -o json | jq -r --arg n "$NAME" '(.content? // .)[] | select(.name == $n) | .id' | head -1)
if [ -n "$PROJECT" ]; then
  say "project $NAME exists: $PROJECT"
else
  args=(--name "$NAME" --description "$(jq -r .description "$DIR/project.json")")
  while IFS= read -r label; do args+=(--label "$label"); done < <(jq -r '(.labels // {}) | to_entries[] | "\(.key)=\(.value)"' "$DIR/project.json")
  PROJECT=$(graalctl projects create "${args[@]}" -o id)
  say "project $NAME created: $PROJECT"
fi

# ── Identity ───────────────────────────────────────────────────────────────────────────────────────
GRANT=0
if [ -z "$IDENTITY" ]; then
  GRANT=1
  IDENTITY=$(graalctl identities list -o json | jq -r --arg n "$NAME-runner" '(.content? // .)[] | select(.name == $n) | .id' | head -1)
  if [ -n "$IDENTITY" ]; then
    say "identity $NAME-runner exists: $IDENTITY"
  else
    jq -n --arg n "$NAME-runner" --arg d "Runs the jobs of the example project $NAME" '{name: $n, description: $d}' > "$TMP/identity.json"
    IDENTITY=$(graalctl api /identities -k identity -d @"$TMP/identity.json" -o json | jq -r .id)
    say "identity $NAME-runner created: $IDENTITY"
  fi
fi

# ── Jobs: create, or replace the code and options of the job of the same name ─────────────────────
declare -A JOB_ID
existing_jobs=$(graalctl jobs list --project "$PROJECT" -o json)
for job in "${JOBS[@]}"; do
  f=${JOB_FILE[$job]}
  sed -i "s/<IDENTITY_ID>/$IDENTITY/g" "$f"
  left=$(grep -o '<[A-Z][A-Z0-9_]*>' "$f" | sort -u | tr '\n' ' ' || true)
  [ -z "$left" ] || die "jobs/$job: placeholders left: $left"
  jname=$(jq -r .name "$f")
  id=$(jq -r --arg n "$jname" '(.content? // .)[] | select(.name == $n) | .id' <<<"$existing_jobs" | head -1)
  if [ -n "$id" ]; then
    jq '[{op: "replace", path: "/description", value: .description},
         {op: "replace", path: "/identity_id", value: .identity_id},
         {op: "replace", path: "/timeout_seconds", value: .timeout_seconds},
         {op: "replace", path: "/libraries", value: .libraries},
         {op: "replace", path: "/options", value: .options}]' "$f" > "$TMP/patch.json"
    graalctl jobs patch "$id" -f "$TMP/patch.json" -o id >/dev/null
    say "job $jname updated: $id"
  else
    id=$(graalctl jobs create --project "$PROJECT" -f "$f" -o id)
    say "job $jname created: $id"
  fi
  JOB_ID[$job]=$id
done

# ── Rights of the identity ────────────────────────────────────────────────────────────────────────
if [ "$GRANT" = 1 ]; then
  say "rights of $NAME-runner"
  TENANT=$(graalctl whoami -o json | jq -r '.tenant // empty')
  [ -n "$TENANT" ] || die "tenant unknown"
  grant "Project Contributor" "$IDENTITY" project "$PROJECT"
  grant "Catalog Contributor" "$IDENTITY" tenant "$TENANT"
  grant "Infrastructure Reader" "$IDENTITY" infrastructure "$INFRA"
  for job in "${JOBS[@]}"; do grant "Job Reader" "$IDENTITY" job "${JOB_ID[$job]}"; done
fi

# ── Workflow ──────────────────────────────────────────────────────────────────────────────────────
wf=$(sed "s/<IDENTITY_ID>/$IDENTITY/g" "$DIR/workflow.json")
for job in "${JOBS[@]}"; do wf=${wf//"<JOB:$job>"/${JOB_ID[$job]}}; done
left=$(grep -o '<[A-Z][A-Z0-9_:a-z-]*>' <<<"$wf" | sort -u | tr '\n' ' ' || true)
[ -z "$left" ] || die "workflow.json: placeholders left: $left"
printf '%s\n' "$wf" > "$TMP/workflow.json"
wname=$(jq -r .name "$TMP/workflow.json")
WORKFLOW=$(graalctl workflows list -o json | jq -r --arg n "$wname" --arg p "$PROJECT" \
  '(.content? // .)[] | select(.name == $n and .project_id == $p) | .id' | head -1)
if [ -n "$WORKFLOW" ]; then
  jq '[{op: "replace", path: "/description", value: .description},
       {op: "replace", path: "/identity_id", value: .identity_id},
       {op: "replace", path: "/tasks", value: .tasks}]' "$TMP/workflow.json" > "$TMP/patch.json"
  graalctl workflows patch "$WORKFLOW" -f "$TMP/patch.json" -o id >/dev/null
  say "workflow $wname updated: $WORKFLOW"
else
  WORKFLOW=$(graalctl workflows create --project "$PROJECT" -f "$TMP/workflow.json" -o id)
  say "workflow $wname created: $WORKFLOW"
fi

if [ "$RUN" = 1 ]; then
  say "run of $wname"
  exec graalctl workflows run "$WORKFLOW" --infrastructure "$INFRA" --wait
fi
echo "    run it with: graalctl workflows run $WORKFLOW --infrastructure $INFRA --wait"
