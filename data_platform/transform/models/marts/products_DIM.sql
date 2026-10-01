{{ config(materialized='table') }}

/*==============================================================
  marts.products_DIM — one row per (store, sku) ever seen, built
  from the SCD2 snapshot so discontinued products keep their row
  (is_current = false) and price_events_FACT never orphans.
  ingredient_id_FK arrives in the recipe phase.
==============================================================*/

{%- set snapshot_cols = 'product_key, store, sku, ean, product_id, product_name, item_name, brand, category_path, link, net_grams, net_ml, net_count, price_cop, list_price_cop, is_available, dbt_valid_from, dbt_valid_to' -%}

WITH all_sources AS (
	-- Conformed across sources: the union lives here, not in clean
	SELECT {{ snapshot_cols }} FROM {{ ref('vtex_products_SNAPSHOT') }}
	UNION ALL
	SELECT {{ snapshot_cols }} FROM {{ ref('shopify_products_SNAPSHOT') }}
)

, latest AS (
	SELECT
		*
		-- Selection, not deduplication: newest snapshot version per product
		, ROW_NUMBER() OVER(PARTITION BY product_key ORDER BY dbt_valid_from DESC) AS rnk
	FROM all_sources
	WHERE 1=1
)

SELECT
	{{ dbt_utils.generate_surrogate_key(['l.store', 'l.sku']) }} AS "product_id_PK"
	, s."store_id_PK" AS "store_id_FK"
	, l.store AS store_key
	, l.sku
	, l.ean
	, l.product_id AS source_product_id
	, l.product_name
	, l.item_name
	, l.brand
	, l.category_path
	, l.link
	, l.net_grams
	, l.net_ml
	, l.net_count
	, l.price_cop AS current_price_cop
	, l.list_price_cop AS current_list_price_cop
	, l.is_available
	, l.dbt_valid_to IS NULL AS is_current
	, l.dbt_valid_from AS attributes_valid_from
FROM latest l
INNER JOIN {{ ref('stores_DIM') }} s
	ON l.store = s.store_key
WHERE 1=1
	AND l.rnk = 1
