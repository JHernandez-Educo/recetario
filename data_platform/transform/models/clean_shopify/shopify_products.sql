{{ config(materialized='table', alias='products') }}

/*==============================================================
  clean_shopify.products — current Shopify catalog (Mundo Huevo),
  one row per (store, variant). Column list matches clean_vtex.products
  so the marts can union both.
==============================================================*/

SELECT
	'shopify' AS platform
	, p.store
	, CAST(v.id AS varchar) AS sku
	, CAST(NULL AS varchar) AS ean
	, CAST(p.id AS varchar) AS product_id
	, p.title AS product_name
	, v.title AS item_name
	, p.vendor AS brand
	, p.product_type AS category_path
	, CAST(NULL AS varchar) AS link
	, CAST(round(CAST(v.price AS decimal(18,2))) AS bigint) AS price_cop
	, CAST(round(CAST(v.compare_at_price AS decimal(18,2))) AS bigint) AS list_price_cop
	, TRUE AS price_check_ok
	, CAST(NULL AS bigint) AS available_qty
	, v.available AS is_available
	-- Shopify grams is merchant-entered and often 0: hint, not truth (PRD 6.3)
	, CAST(NULLIF(v.grams, 0) AS decimal(18,3)) AS net_grams
	, CAST(NULL AS decimal(18,3)) AS net_ml
	, CAST(NULL AS bigint) AS net_count
	, p.captured_at
FROM {{ source('raw_shopify', 'products__variants') }} v
INNER JOIN {{ source('raw_shopify', 'products') }} p
	ON v._dlt_parent_id = p._dlt_id
WHERE 1=1
