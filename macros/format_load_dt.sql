{% macro format_load_dt() %}
{% if target.type == 'redshift' %}
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')
{% elif target.type == 'databricks' %}
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddHHmmss')
{% elif target.type == 'trino' %}
    FORMAT_DATETIME(current_timestamp, 'yyyyMMddHHmmss')
{% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type for format_load_dt macro") }}
{% endif %}
{% endmacro %}
