# notebook_example

A Jupyter notebook run by graal from the first cell to the last, as a batch job: a small sales report
with pandas, parameterised by the job.

- **Job type:** `notebook` (specialised type: available only if your installation configured the
  `notebook` runtime image)
- **Entry point:** `options.notebook`, the path of the `.ipynb` in the run's workspace — the notebook
  is uploaded as a library, so its file name is enough. (`options.source` takes the notebook JSON
  inline instead.)
- **Parameters:** `options.parameters` are injected as a new cell right after the cell tagged
  `parameters` (papermill's convention), so they override its defaults. Here: `region` and
  `min_quantity`.
- **Outcome:** the run fails if a cell raises (here, the `assert` when the filter leaves no line), or
  if a cell runs longer than `cell_timeout_seconds`. The run's results give the number of cells
  executed and the last output. When the installation has MLflow, the executed notebook and its HTML
  rendering are logged as artifacts of an MLflow run, in the experiment named by `options.experiment`
  (default `notebooks`).

## Deploy

```sh
./deploy.sh notebook_example --project "$PROJECT_ID" --identity "$IDENTITY_ID" --run
```

Or by hand:

```sh
NB_KEY=$(graalctl libraries upload sales_report.ipynb)
sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" -e "s/<LIBRARY_KEY>/$NB_KEY/" job.json > job.local.json
JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The log ends with a `GRAAL_RUN_RESULTS` line whose `last_output` is `TOTAL REVENUE 1911.0` with the
parameters of `job.json` (`min_quantity: 50` drops no line; try `"region": "Bretagne"`).

Try it locally first: `jupyter nbconvert --to notebook --execute sales_report.ipynb` (pandas needed).

If the API answers `501 feature_disabled`, the `notebook` type is not enabled on your installation.
