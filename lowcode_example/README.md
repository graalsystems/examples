# lowcode_example

A low-code pipeline: a graph of boxes (read → aggregate → quality checks → write) that graal turns into
pandas or PySpark code at run time. No code to write, nothing to build.

| Task | Box | What it does |
|---|---|---|
| `read_sales` | Read, local CSV | reads `sales.csv`, uploaded as a library of the job (`/workspace/sales.csv`) |
| `quantity_by_region` | Aggregation | sum of `quantity` and number of lines per `region` |
| `check_report` | Expect (quality) | 3 rows, `region` unique and never null, quantities ≥ 1 — the run fails if one check fails |
| `write_report` | Write, local CSV | writes `/tmp/sales_by_region.csv` in the run's pod |

- **Job type:** `lowcode`
- **Entry point:** `options.definition`, the graph itself (the DAG of the low-code engine, kept as
  [`sales_by_region.dag.json`](sales_by_region.dag.json) and copied into [`job.json`](job.json));
  `options.mode` is the language it is turned into: `pandas` (one pod) or `pyspark`.
- **Outputs:** the run's **Quality** tab lists the four checks and their observed values, and its
  **Lineage** tab shows the column-level lineage (`quantity` → `sum_quantity`). The CSV written to
  `/tmp` disappears with the pod: replace the `write_report` connector by an S3 bucket or an Iceberg
  table of the project to keep the result.
- **Edit it visually:** in the console, *Low-code editor → New graph*, then build the same boxes —
  or start from a template. The job wizard takes the graph and stores its DAG in the job.

## Deploy

```sh
./deploy.sh lowcode_example --project "$PROJECT_ID" --identity "$IDENTITY_ID" --run
```

Or by hand:

```sh
CSV_KEY=$(graalctl libraries upload sales.csv)
sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" -e "s/<LIBRARY_KEY>/$CSV_KEY/" job.json > job.local.json
JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The log shows the generated code's lineage (`GRAAL_OPENLINEAGE`) and a `GRAAL_QUALITY_REPORT` line with
`"status":"passed"`, then the run completes. Change `"max": 3` of the `row_count` check to `2` to see a
failing check stop the run.

After editing the graph, copy it back into `job.json` (`options.definition`):

```sh
jq --slurpfile dag sales_by_region.dag.json '.options.definition = $dag[0]' job.json > job.tmp && mv job.tmp job.json
```
