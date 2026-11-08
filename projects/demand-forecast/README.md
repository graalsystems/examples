# Demand forecast: machine learning on Iceberg tables

Daily unit sales of six stores and three product families, forecast 28 days ahead by three jobs
chained by the workflow `demand-forecast-weekly`:

```text
prepare ──────────────▶ train (notebook) ─────────▶ publish
forecast_sales_history   forecast_predictions        forecast_weekly
                         + MLflow experiment
```

| Job | Type | Reads | Writes | What it does |
|---|---|---|---|---|
| `forecast-prepare` | `python` | — | `forecast_sales_history` | Two years of daily sales (trend, weekly and yearly cycles, promotions, holidays, Sunday closings) and the next 28 days with their planned promotions and closings, `units` left empty |
| `forecast-train` | `notebook` | `forecast_sales_history` | `forecast_predictions` | Gradient boosting (scikit-learn) on features known 28 days ahead; validated on the last 8 weeks against a naive baseline (same weekday four weeks earlier) and **fails if it does not beat it**; metrics in the MLflow experiment `demand-forecast`; forecasts the empty days |
| `forecast-publish` | `python` | `forecast_predictions`, `forecast_sales_history` | `forecast_weekly` | Weekly units per store and family next to the same weeks one year earlier; fails on an incomplete forecast |

The sales are **synthetic** (`prepare.py`, seeded). On them the model reaches a WAPE of about 7 %
on the validation weeks, against about 21 % for the naive baseline.

## Deploy and run

```sh
projects/deploy-project.sh projects/demand-forecast --run
```

## Where to look

- **The executed notebook**: the run of `forecast-train` keeps it, with its HTML version, as the
  output of a run in the project's MLflow experiment `demand-forecast`.
- **The metrics**: same experiment, run `gradient-boosting`: `wape`, `wape_baseline_lag_28`, `mae`,
  `improvement_vs_baseline`, and the parameters of the model.
- **The forecast**: the tables `forecast_predictions` (per day) and `forecast_weekly` (per week) in
  the catalog.

## What to try next

- **Tune the model without touching the code**: the notebook's `parameters` cell is overridden by
  the job's `parameters` (`max_iter`, `learning_rate`, `max_leaf_nodes`, `validation_days`). Change
  them in `jobs/train/job.json`, redeploy, rerun, and compare the runs in MLflow.
- **Use your own sales**: replace `prepare.py` by a job that reads your source and writes the same
  columns (`store`, `family`, `day`, `units`, `promotion`, `closed`), with `units` empty on the days
  to forecast.
