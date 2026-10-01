{{ config(materialized='table', alias='products') }}

/*==============================================================
  clean_vtex.products — current VTEX catalog, one row per
  (store, sku): D1, Éxito, Euro, Jumbo. Net content is parsed
  from each store's spec dialect into net_grams / net_ml / net_count.
==============================================================*/

WITH vtex_parsed AS (
	SELECT
		'vtex' AS platform
		, store
		, CAST(sku AS text) AS sku
		, ean
		, CAST(product_id AS varchar) AS product_id
		, product_name
		, item_name
		, brand
		, category_path
		, link
		, CAST(price_cop AS bigint) AS price_cop
		, CAST(list_price_cop AS bigint) AS list_price_cop
		, price_check_ok
		, CAST(available_qty AS bigint) AS available_qty
		, is_available
		, captured_at
		-- Net content: Éxito = "Factor Neto PUM", D1 = "Valor de Medida", Euro = "Cantidad".
		-- Kept as raw text here: values are dirty ("50 G" embeds the unit in the number).
		, COALESCE(
			(CAST(specs_json AS jsonb) -> 'Factor Neto PUM' ->> 0)
			, (CAST(specs_json AS jsonb) -> 'Valor de Medida' ->> 0)
			, (CAST(specs_json AS jsonb) -> 'Cantidad' ->> 0)
		) AS spec_qty_raw
		, lower(COALESCE(
			(CAST(specs_json AS jsonb) -> 'Unidad de Medida' ->> 0)
			, (CAST(specs_json AS jsonb) -> 'Unidad De Medida' ->> 0)
		)) AS spec_unit_field
		-- Jumbo buries content in a nested JSON string; unit_multiplier_un is kg-scaled
		-- (0.5 with unit 'gr' = 500 g). Best-effort inference, guarded by range tests.
		, (CAST((CAST(specs_json AS jsonb) -> 'ProductData' ->> 0) AS jsonb) ->> 'measurement_unit_un') AS jumbo_unit
		, CAST((CAST((CAST(specs_json AS jsonb) -> 'ProductData' ->> 0) AS jsonb) ->> 'unit_multiplier_un') AS decimal(18,3)) AS jumbo_mult
	FROM {{ source('raw_vtex', 'products') }}
	WHERE 1=1
)

, vtex_typed AS (
	SELECT
		*
		-- Explicit parse, never silent coercion: numeric part of the qty text,
		-- comma decimals normalized; unparseable becomes NULL by design
		, CAST(NULLIF(replace(substring(spec_qty_raw from '([0-9]+[.,]?[0-9]*)'), ',', '.'), '') AS decimal(18,3)) AS spec_qty
		-- Unit: the unit field wins; else trailing letters of the qty text ("50 G" -> g)
		, COALESCE(spec_unit_field, lower(NULLIF(substring(spec_qty_raw from '([a-zA-Z]+)\s*$'), ''))) AS spec_unit
	FROM vtex_parsed
	WHERE 1=1
)

, vtex AS (
	SELECT
		platform
		, store
		, sku
		, ean
		, product_id
		, product_name
		, item_name
		, brand
		, category_path
		, link
		, price_cop
		, list_price_cop
		, price_check_ok
		, available_qty
		, is_available
		, CASE
			WHEN spec_unit IN ('gramo', 'gramos', 'gr', 'g', 'grs') THEN spec_qty
			WHEN spec_unit IN ('kilogramo', 'kilogramos', 'kg') THEN spec_qty * 1000
			WHEN spec_unit IS NULL AND jumbo_unit = 'gr' THEN jumbo_mult * 1000
			ELSE NULL
		END AS net_grams
		, CASE
			WHEN spec_unit IN ('mililitro', 'mililitros', 'ml') THEN spec_qty
			WHEN spec_unit IN ('litro', 'litros', 'l', 'lt') THEN spec_qty * 1000
			ELSE NULL
		END AS net_ml
		, CASE
			WHEN spec_unit IN ('und', 'unidad', 'unidades', 'un') THEN CAST(spec_qty AS bigint)
			ELSE NULL
		END AS net_count
		, captured_at
	FROM vtex_typed
	WHERE 1=1
)

SELECT * FROM vtex
