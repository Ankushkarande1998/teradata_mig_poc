{% macro drop_tables_by_prefix(prefix) %}
{% if execute %}
    {% set query %}
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{{ target.schema }}'
          AND lower(table_name) LIKE '{{ prefix }}%'
    {% endset %}

    {% set results = run_query(query) %}
    
    {% if results %}
        {% for row in results %}
            {% set table_name = row['table_name'] %}
            
            {% if target.type == 'redshift' %}
                {% set drop_command %}
                    DROP TABLE IF EXISTS {{ target.schema }}.{{ table_name }}
                {% endset %}
            {% elif target.type == 'databricks' %}
                {% set drop_command %}
                    DROP TABLE IF EXISTS {{ target.database }}.{{ target.schema }}.{{ table_name }}
                {% endset %}
            {% elif target.type == 'starburst' %}
                {% set drop_command %}
                    DROP TABLE IF EXISTS {{ target.schema }}.{{ table_name }}
                {% endset %}
            {% else %}
                {% set drop_command %}
                    DROP TABLE IF EXISTS {{ target.schema }}.{{ table_name }}
                {% endset %}
            {% endif %}
            
            {{ log(drop_command, info=true) }}
            {% do run_query(drop_command) %}
        {% endfor %}
    {% else %}
        {{ log("No tables found with prefix: " ~ prefix ~ " in schema: " ~ target.schema, info=true) }}
    {% endif %}
{% else %}
    {{ log("Macro executed in compile mode; no tables will be dropped.", info=true) }}
{% endif %}
{% endmacro %}
