# Retention Analysis Guide

This document describes the public, sanitized analysis logic used by the framework. It intentionally avoids real project names, raw data characteristics, internal event definitions, and company-specific conclusions.

## 1. Analysis Goal

The framework helps analysts answer:

- What is the D1/D7/D30 retention level?
- Which cohort, channel, region, version, or JSON-derived segment is underperforming?
- Which path or funnel step is associated with churn?
- Which features are most predictive of retention or churn?
- What business action should be tested next?

## 2. Standard Event Log

Example input schema:

| Field | Description |
|---|---|
| `user_id` | Stable user identifier |
| `event_time` | Timestamp with second-level precision |
| `event_date` | Event date |
| `reg_date` | Registration or first-active date |
| `event_name` | Event type |
| `country` | Optional geographic dimension |
| `channel` | Optional acquisition/source dimension |
| `event_params` | Optional JSON string with event-specific parameters |

The exact source column names can be different. The UI and YAML config map source columns to these standard fields.

## 3. Retention Definition

For D+N retention:

```python
check_date = reg_date + timedelta(days=N)
retained = user_has_any_event_on(check_date)
```

The default active definition is "the user has any event on the target date." Projects can override this with a stricter function, such as requiring a gameplay event.

## 4. Cohort Matrix

The cohort matrix groups users by registration date, week, or month, then calculates active rates for D+0 through D+N.

The default implementation uses vectorized joins and pivot tables to avoid expensive per-user loops.

## 5. JSON Parameter Handling

Event logs often contain event-specific JSON keys. A key such as `level_id` may only exist for level events, while login or purchase events should not be expected to contain it.

The framework therefore uses conditional validation:

1. User maps the JSON column, for example `event_params`.
2. Backend detects available keys.
3. User declares business roles for keys.
4. User configures `relevant_events`.
5. Missingness is judged only inside matched relevant events.
6. User-level coverage is calculated alongside event-row missingness.

This prevents false warnings caused by heterogeneous event streams.

## 6. Diagnostic Flow

The agent-style diagnosis follows four steps:

1. Data sanity: required columns, date continuity, duplicate risk, JSON coverage with event alignment.
2. Segmentation: compare retention by channel, country, version, and virtual JSON dimensions.
3. Path and funnel: inspect key event sequences or configured funnel steps.
4. Attribution: combine segment impact and model feature importance into business-readable recommendations.

## 7. Report Template

The generated report follows:

```text
[Data Checkup]
Data quality score, warnings, and validation notes.

[Anomaly Location]
Which segment is underperforming and by how much.

[Core Attribution]
Top model or rule-based drivers.

[Business Strategy]
Concrete follow-up experiments or instrumentation checks.
```

## 8. Public Sharing Rules

Before publishing analysis material:

- Replace real project names with generic names.
- Replace raw field names if they are internal.
- Remove exact sample sizes, dates, revenue values, and sensitive benchmarks.
- Remove user-level exports and private event taxonomies.
- Keep only reusable methods and anonymized examples.
