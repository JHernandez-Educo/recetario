-- A row where the COP price and the cart-link centavos value disagree means a
-- possible x100 unit error (PRD R4) — catastrophic for COGS. Any row = build fails.
SELECT
	store
	, sku
	, product_name
	, price_cop
FROM {{ ref('vtex_products') }}
WHERE 1=1
	AND price_check_ok = FALSE
