{{ config(materialized='incremental') }}

/*==============================================================
  marts.price_events_FACT — the price history fact.
  Grain: one row per (product, observation time).
  price_per_gram_cop is THE comparison measure: it is only as
  good as products_DIM.net_grams (null when content is unknown
  or sold by volume/count).
==============================================================*/

WITH all_sources AS (
	-- Conformed across sources: the union lives here, not in clean
	SELECT * FROM {{ ref('vtex_price_events') }}
	UNION ALL
	SELECT * FROM {{ ref('shopify_price_events') }}
)

SELECT
	{{ dbt_utils.generate_surrogate_key(['e.store', 'e.sku']) }} AS "product_id_FK"
	, s."store_id_PK" AS "store_id_FK"
	, CAST(to_char(timezone('America/Bogota', e.captured_at), 'YYYYMMDD') AS int) AS "date_id_FK"
	, e.price_cop
	, e.list_price_cop
	, CASE
		WHEN p.net_grams > 0 THEN round(e.price_cop / p.net_grams, 4)
		ELSE NULL
	END AS price_per_gram_cop
	, e.available_qty
	, e.source
	, e.captured_at
FROM all_sources e
INNER JOIN {{ ref('stores_DIM') }} s
	ON e.store = s.store_key
LEFT JOIN {{ ref('products_DIM') }} p
	ON e.store = p.store_key
	AND e.sku = p.sku
WHERE 1=1
{% if is_incremental() %}
	AND e.captured_at > (SELECT max(captured_at) FROM {{ this }})
{% endif %}
