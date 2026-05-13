# Common Errors

This sanitized checklist captures reusable engineering and analytics lessons without exposing private project details.

## 1. Confusing Event Rows With Users

Use `nunique(user_id)` for user counts. Do not use row counts when the dataset is event-level.

## 2. Judging JSON Missingness Globally

Many JSON keys only belong to specific event types. Configure `relevant_events` and calculate missingness only inside those events.

## 3. Date Format Mismatch

Always verify date parsing before analysis. A silent `NaT` conversion can make retention empty or misleading.

## 4. Registration Window Misalignment

Use the same registration window for all downstream calculations. Mixed windows make segment comparisons invalid.

## 5. Survivor Bias

Do not compare first-day churn users against multi-day retained users using all available days. Limit feature extraction to the same observation window.

## 6. Duplicate Event Keys

`user_id + event_time` may not be unique in high-volume event logs. Prefer a real event id when available, or include `event_name` and parameter hashes in duplicate checks.

## 7. Segment Sample Size Too Small

Flag low-sample segments. Retention swings in tiny segments should not drive business decisions.

## 8. Funnel Steps Not Ordered

Use `event_time` for path and funnel ordering. `event_date` alone may collapse event order within a day.

## 9. Treating Optional Dimensions as Required

Country, channel, version, and JSON params can be optional. The framework should degrade gracefully when they are absent.

## 10. Publishing Private Artifacts

Never commit raw logs, user exports, internal event dictionaries, or project-specific reports to a public repository.
