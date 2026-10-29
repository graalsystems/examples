# bash_example

A shell job defined entirely in its JSON: no code to upload. It prints the variables graal sets
in every run and lists `/workspace`:

- `GRAAL_LOGICAL_DATE`: the date a scheduled or backfill run processes (unset when the run is
  started by hand),
- `GRAAL_ATTEMPT`: the attempt number when a run is retried,
- `GRAAL_BUCKET`: the project's bucket.

- **Job type:** `bash` (core type, `toolbox` image, Alpine)
- **Entry point:** `options.lines`, run in order; keep them POSIX `sh` compatible
- **Dependencies:** none

## Build

Nothing to build.

## Run with graalctl

```sh
sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

To see `GRAAL_LOGICAL_DATE` filled in, schedule the job, for example every day at 06:00:

```sh
graalctl jobs schedule "$JOB_ID" "0 6 * * *"
```

To run a longer script, upload it as a library (`graalctl libraries upload script.sh`), add
`{"type": "file", "key": "<LIBRARY_KEY>"}` to `libraries` in [`job.json`](job.json) and call it
from `lines`, for example `sh /workspace/script.sh`.
