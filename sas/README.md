# sas

A SAS program (`fish_report.sas`) run by **graal-sas**, the SAS language interpreter of the graal
Data Platform: no SAS licence is needed. It reads a CSV file of the project, cleans it with DATA
steps, prints and summarises it, aggregates it with PROC SQL, and writes an HTML report (with a
chart) and a CSV export.

| Step | Statements |
|---|---|
| Parameter | `%SYSGET(MIN_YEAR)` (from the job's `env`), `%IF` in open code |
| Read | `DATA` + `INFILE 'data/catch.csv' DLM=',' DSD FIRSTOBS=2` + `INPUT` |
| Clean | `WHERE`, computed column, `FORMAT`, `DROP` |
| Describe | `PROC CONTENTS`, `PROC PRINT (OBS=5)`, `PROC MEANS MIN MAX MEAN` with `CLASS` |
| Aggregate | `PROC SQL` `CREATE TABLE ... GROUP BY` |
| Report | `ODS HTML`, `PROC PRINT`, `PROC SGPLOT VBAR`, `PROC EXPORT DBMS=CSV` |

`data/catch.csv` is a small **synthetic** sample (generated, not real catch figures) with the
columns of a fisheries catch dataset.

- **Job type:** `sas` (specialised type: available only if your installation configured the `sas`
  runtime image, graal-sas 1.0.0)
- **Project:** a zip archive of this directory (`archive`); the main program is `program`, a path
  inside the archive. A git repository (`repository`: `url`, `path`, `revision`, and for a private
  repository `username` and `password`, which is sealed when the job is saved) can replace the
  archive.
- **Language coverage:** graal-sas runs a published subset of the SAS language (DATA step, macro
  language, PROC SQL/SORT/MEANS/FREQ/PRINT/REPORT/TABULATE/TRANSPOSE/FORMAT/IMPORT/EXPORT/SGPLOT,
  ODS HTML/EXCEL/CSV, libraries on directories, S3, Iceberg and PostgreSQL). A statement outside
  that subset stops the run with exit code 3, an `ERROR` of the program with exit code 2; neither
  is retried. Check the coverage list of your installation before porting a program.
- **Outputs:** relative output paths (`ODS HTML FILE=`, `PROC EXPORT OUTFILE=`) go to the run's
  output directory, with the listing. With `output: s3://<BUCKET>/<prefix>`, graal publishes them,
  at the end of the run, under `<prefix>/<job name>/<run id>/` in that bucket (a bucket of the
  project). The run's results list the outputs and the tables created.
- **Credentials:** never in the program. A `LIBNAME` password or secret key is written
  `"${secret:<name>}"` and resolved from the secrets of the job's identity.

## Build

Package the project (the archive root holds `fish_report.sas` and `data/`):

```sh
zip -r fish_report.zip fish_report.sas data
```

## Run with graalctl

```sh
ARCHIVE_KEY=$(graalctl libraries upload fish_report.zip)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$ARCHIVE_KEY/" \
    -e "s/<BUCKET>/$BUCKET/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The log has the form of a SAS log (`NOTE:`, `WARNING:`, `ERROR:`) followed by the listing. Remove
`output` from [`job.json`](job.json) to run without publishing. Job and run parameters are
available to the program as `&SYSPARM`.

If the API answers `501 feature_disabled`, the `sas` type is not enabled on your installation.
The options a specialised type accepts depend on the runtime image configured there: check the
installation's API reference (`/api/v1/docs`) and adjust [`job.json`](job.json).
