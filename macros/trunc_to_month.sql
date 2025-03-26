
{% macro trunc_to_month(date_column) -%}
  {%- if target.type == 'redshift' -%}
    DATE_TRUNC('month', {{ date_column }})
  {%- elif target.type in ('spark', 'databricks') -%}
    TRUNC({{ date_column }}, 'MONTH')
  {%- elif target.type in ('trino', 'starburst') -%}
  {%- endif -%}
{%- endmacro %}