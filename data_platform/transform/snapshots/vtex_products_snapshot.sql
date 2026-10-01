{% snapshot vtex_products_SNAPSHOT %}

{{
	config(
		schema='snapshots_vtex',
		alias='products_SNAPSHOT',
		unique_key='product_key',
		strategy='check',
		check_cols=['product_name', 'brand', 'price_cop', 'list_price_cop', 'net_grams', 'net_ml', 'net_count', 'is_available', 'category_path']
	)
}}

/*==============================================================
  Attribute history for products (dbt-native SCD2): whenever a
  tracked column changes, dbt closes the old row (dbt_valid_to)
  and opens a new one. captured_at is deliberately NOT tracked —
  it changes every crawl and would version every row every run.
==============================================================*/

SELECT
	store || ':' || sku AS product_key
	, *
FROM {{ ref('vtex_products') }}

{% endsnapshot %}
