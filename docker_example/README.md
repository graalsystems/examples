# docker_example

A batch job packaged as your own container image: graal pulls it and runs its command once, in one pod.
The program summarises sales with pandas, writes `report.csv` to the run's workspace and prints it.

- **Job type:** `docker`
- **Entry point:** `options.image` and `options.command`. The command is a shell line (run by the
  image's `/bin/sh`); the job's `parameters` are its arguments (`"$@"`).
- **Image rules:** graal runs the image as UID 10001, without any capability, with a read-only root
  filesystem; `/tmp` and `/workspace` are writable. The image needs a `/bin/sh` and must not need root.
  The [`Dockerfile`](Dockerfile) shows the minimum.
- **Registry:** the image must come from a registry your installation allows. The platform has an
  allowlist (`GRAAL_RUNTIME_ALLOWED_REGISTRIES`), and each tenant can have its own
  (`runtime-allowed-registries` tenant metadata), with the pull credential of the tenant's registry
  (`registry-secret-id`), both set by the platform administrator. A job never carries registry
  credentials: `credentials` is refused.

## Build and push the image

```sh
REGISTRY=registry.example.com/my-team      # a registry your installation allows
docker build -t "$REGISTRY/graal-examples/sales-report:1.0.0" .
docker push "$REGISTRY/graal-examples/sales-report:1.0.0"
```

Try it as graal will run it:

```sh
docker run --rm --read-only --user 10001:10001 --cap-drop ALL --tmpfs /tmp --tmpfs /workspace \
  "$REGISTRY/graal-examples/sales-report:1.0.0" sh -c 'python /app/report.py "$@"' graal-docker Bretagne
```

## Deploy

```sh
IMAGE="$REGISTRY/graal-examples/sales-report:1.0.0" \
  ./deploy.sh docker_example --project "$PROJECT_ID" --identity "$IDENTITY_ID" --run
```

Or by hand:

```sh
sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" -e "s|<IMAGE>|$REGISTRY/graal-examples/sales-report:1.0.0|" \
  job.json > job.local.json
JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The log ends with the revenue per region and `written /workspace/report.csv (3 regions), uid=10001`.
Change `parameters` to `["Bretagne"]` to keep one region.

A `400` naming the image means its registry is not allowed for your tenant.
