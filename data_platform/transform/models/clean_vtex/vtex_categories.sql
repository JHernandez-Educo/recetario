{{ config(materialized='table', alias='categories') }}

/*==============================================================
  clean_vtex.categories — VTEX category trees, typed and deduped view
  of crawl scope. Grain: (store, category_id).
==============================================================*/

SELECT
	store
	, CAST(category_id AS bigint) AS category_id
	, name
	, path
	, url_path
	, CAST(depth AS int) AS depth
	, is_leaf
	, captured_at
FROM {{ source('raw_vtex', 'categories') }}
WHERE 1=1
