# ci-github-actions

A GitHub Actions workflow, [`run-graal-job.yml`](run-graal-job.yml), that installs `graalctl`
and runs an existing graal job, streaming its logs into the workflow log. The workflow step
fails when the run fails, so a pipeline can gate on a job (a database migration, a data quality
check, a model build...).

It replaces the former Azure Pipelines `migration` example: instead of fetching tokens and
polling the API with `curl` and `jq`, it uses `graalctl jobs run --follow`, whose exit code is the
outcome of the run (`0` succeeded, `1` failed or cancelled, `124` timed out).

The file is kept outside `.github/workflows/` so that it does not run in this repository.

## Set up

1. Create application credentials (an OpenID Connect client with the client credentials grant)
   for the tenant, allowed to run the job.
2. In your repository, add these secrets (*Settings → Secrets and variables → Actions*):

   | Secret | Example |
   |---|---|
   | `GRAAL_API_URL` | `https://graal.example.com/api/v1` |
   | `GRAAL_KEYCLOAK_URL` | `https://sso.example.com` (without `/realms/...`) |
   | `GRAAL_TENANT` | your tenant |
   | `GRAAL_CLIENT_ID` | the application's client id |
   | `GRAAL_CLIENT_SECRET` | the application's client secret |

3. Add the repository variable `GRAAL_JOB_ID` with the id of the job to run
   (`graalctl jobs list --project <PROJECT_ID>`), or give it when starting the workflow by hand.
4. Copy `run-graal-job.yml` to `.github/workflows/` and adapt the triggers.

## Notes

- `--timeout 3600` stops waiting after one hour; keep the workflow's `timeout-minutes` above it.
- Set `GRAALCTL_VERSION` in the install step to pin a `graalctl` release.
- To choose where the run executes, set `GRAAL_INFRASTRUCTURE`
  (see `graalctl infrastructures list`).
- To deploy new code before running, add steps that upload it with
  `graalctl libraries upload` and update the job with `graalctl jobs patch`.
