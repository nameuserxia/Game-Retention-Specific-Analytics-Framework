# Web API Guide

This guide uses generic examples only.

## 1. Install Backend Dependencies

```bash
pip install -r requirements.txt
```

## 2. Start Backend

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

Open:

```text
http://localhost:8000/docs
```

## 3. Start Frontend

```bash
cd web
npm install
npm run dev
```

Open:

```text
http://127.0.0.1:5173/
```

## 4. Upload

Supported file types:

- `.csv`
- `.xlsx`
- `.xls`
- `.parquet`

Example columns:

```text
user_id,event_time,event_date,reg_date,event_name,country,channel,event_params
```

## 5. Field Mapping

Map source columns to:

- user ID
- event time
- event date
- registration date
- event name
- optional country
- optional channel
- optional JSON params

## 6. JSON Key Detection

If a JSON parameter column is selected, the backend samples the column and returns discovered keys.

Recommended role examples:

| JSON Key | Role |
|---|---|
| `level_id` | progress dimension |
| `state` | result state |
| `step` | numeric metric |

Set `relevant_events` before treating missing JSON keys as instrumentation issues.

## 7. Analyze

`POST /api/analyze?session_id=<id>`

Request body:

```json
{
  "mapping": {
    "user_id": "user_id",
    "event_time": "event_time",
    "event_date": "event_date",
    "reg_date": "reg_date",
    "event_name": "event_name",
    "country": "country",
    "channel": "channel",
    "json_params": "event_params"
  },
  "analysis_config": {
    "reg_start": "2026-01-01",
    "reg_end": "2026-01-31",
    "retention_days": 1,
    "min_sample_size": 30,
    "cohort_freq": "W",
    "max_days": 30,
    "segment_by_country": true,
    "segment_by_channel": true,
    "game_genre": "casual"
  },
  "param_config": {
    "json_params_col": "json_params",
    "progress_key": "level_id",
    "result_key": "state",
    "numeric_keys": ["step"],
    "segment_keys": ["level_id"],
    "relevant_events": ["level_start", "level_complete"]
  }
}
```

## 8. Privacy

The web app stores uploaded files in `temp/` for the active session. This directory is ignored by Git and should not be published.
