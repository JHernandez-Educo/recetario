{{ config(materialized='table') }}

/*==============================================================
  marts.date_DIM — calendar, 2026-2030.
==============================================================*/

SELECT
	CAST(to_char(t.d, 'YYYYMMDD') AS int) AS "date_id_PK"
	, CAST(t.d AS date) AS full_date
	, CAST(EXTRACT(year FROM t.d) AS int) AS year
	, CAST(EXTRACT(month FROM t.d) AS int) AS month
	, to_char(t.d, 'FMMonth') AS month_name
	, CAST(EXTRACT(day FROM t.d) AS int) AS day
	, CAST(EXTRACT(isodow FROM t.d) AS int) AS iso_weekday
	, CAST(EXTRACT(week FROM t.d) AS int) AS iso_week
	, EXTRACT(isodow FROM t.d) IN (6, 7) AS is_weekend
FROM generate_series(DATE '2026-01-01', DATE '2030-12-31', INTERVAL '1 day') AS t(d)
WHERE 1=1
