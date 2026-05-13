# Game Retention Specific Analytics Framework

A configurable game retention analytics framework for event-log based user retention diagnosis.

This public repository has been sanitized. All company/project names, internal file names, raw data references, and business-specific conclusions have been replaced with generic examples.

## What It Does

- Upload CSV, Excel, or Parquet event logs through a web UI.
- Map arbitrary source columns to standard analysis fields.
- Parse JSON parameter columns, such as `event_params`, into virtual fields.
- Calculate retention, churn users, cohort matrices, paths, segments, and diagnostic reports.
- Run a deterministic agent-style diagnosis workflow with data checks, segmentation, funnel/path analysis, and model-assisted attribution.

## Repository Layout

```text
game_retention_framework/
  api/                 FastAPI backend, routes, adapters, agent tools
  core/                Retention and cohort analytics primitives
  config/              Generic YAML configuration templates
  docs/                Public, sanitized usage and design docs
  scripts/             Placeholder for local/private analysis scripts
  web/                 React + Vite frontend
  run_analysis.py      CLI entrypoint
```

## Data Contract

The framework expects event-level logs with fields that can be mapped to:

| Standard Field | Meaning |
|---|---|
| `user_id` | Stable user/account/device identifier |
| `event_time` | Event timestamp |
| `event_date` | Event date |
| `reg_date` | Registration or first-active date |
| `event_name` | Event type |
| `country` | Optional country/region dimension |
| `channel` | Optional acquisition/source dimension |
| `json_params` | Optional JSON event-parameter column |

Example source columns:

```text
user_id,event_time,event_date,reg_date,event_name,country,channel,event_params
```

## Quick Start: CLI

```bash
pip install -r requirements.txt

python run_analysis.py \
  --config config/example_game_config.yaml \
  --data /path/to/example_events.csv \
  --output output/example_run/
```

## Quick Start: Web

Backend:

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

Frontend:

```bash
cd web
npm install
npm run dev
```

Then open:

```text
http://127.0.0.1:5173/
```

## JSON Parameter Mapping

Many games store event-specific fields in a JSON column. The framework supports a two-stage mapping:

1. Map main columns such as `user_id`, `event_time`, `event_name`, and `json_params`.
2. Detect keys from `json_params`, then assign roles:
   - progress dimension, for example `level_id`
   - result state, for example `state`
   - numeric metric, for example `step`
   - segment dimension, for example `level_id`

For event-specific JSON fields, configure `relevant_events` before judging parameter missingness. This avoids treating unrelated event rows as missing JSON parameters.

## Privacy Notes

This repository is intended for public demonstration and framework reuse only.

- Do not commit raw logs, exports, user-level data, company files, or private reports.
- Put local data under `temp/` or `output/`; both are ignored by Git.
- Keep project-specific scripts in a private workspace, or sanitize them before publishing.
- Replace real event names, business metrics, and time windows with generic examples before sharing.

## Validation

Common checks used during development:

```bash
cd web
npm run build
```

```bash
python -m py_compile api/routes/analysis.py api/routes/upload.py api/routes/validation.py core/analytics.py
```

## License

Private/internal use by default. Add an explicit license before distributing outside your intended scope.
