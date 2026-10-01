{{ config(materialized='incremental', alias='price_events') }}

/*==============================================================
  clean_vtex.price_events — VTEX stores (D1, Éxito, Euro, Jumbo) price history.
  Grain: (store, sku, captured_at). Raw keeps every sighting; a
  product listed in several categories can be seen more than once
  per crawl, sometimes with different prices. Rule: keep the price
  seen most often in that crawl; on a tie, the lowest.
==============================================================*/

WITH source_rows AS (
	SELECT
		'vtex' AS platform
		, store
		, CAST(sku AS varchar) AS sku
		, ean AS ean
		, CAST(product_id AS varchar) AS product_id
		, product_name
		, CAST(price_cop AS bigint) AS price_cop
		, CAST(list_price_cop AS bigint) AS list_price_cop
		, CAST(available_qty AS bigint) AS available_qty
		, source
		, captured_at
	FROM {{ source('raw_vtex', 'price_events') }}
	WHERE 1=1
	{% if is_incremental() %}
		AND captured_at > (SELECT max(captured_at) FROM {{ this }})
	{% endif %}
)

, sightings AS (
	SELECT
		*
		, COUNT(*) OVER(PARTITION BY store, sku, captured_at, price_cop) AS price_sightings
	FROM source_rows
	WHERE 1=1
)

, ranked AS (
	SELECT
		*
		, ROW_NUMBER() OVER(
			PARTITION BY store, sku, captured_at
			ORDER BY price_sightings DESC, price_cop ASC, list_price_cop ASC NULLS LAST
		) AS rnk
	FROM sightings
	WHERE 1=1
)

SELECT
	platform
	, store
	, sku
	, ean
	, product_id
	, product_name
	, price_cop
	, list_price_cop
	, available_qty
	, source
	, captured_at
FROM ranked
WHERE 1=1
	AND rnk = 1
