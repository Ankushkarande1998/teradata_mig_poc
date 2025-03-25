{% macro sum_window(column, partition_by) %}
    {% if target.type == 'redshift' or target.type == 'starburst' %}
        SUM({{ column }}) OVER (PARTITION BY {{ partition_by }})
    {% elif target.type == 'databricks' %}
        SUM({{ column }}) OVER (PARTITION BY {{ partition_by }} ORDER BY NULL)
    {% else %}
        SUM({{ column }}) OVER (PARTITION BY {{ partition_by }})
    {% endif %}
{% endmacro %}
