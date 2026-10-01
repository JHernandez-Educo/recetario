{#- Use the custom schema name exactly as configured (clean, marts).
    dbt's default would prefix it as "main_clean"; we want plain "clean". -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
	{%- if custom_schema_name is none -%}
		{{ target.schema }}
	{%- else -%}
		{{ custom_schema_name | trim }}
	{%- endif -%}
{%- endmacro %}
