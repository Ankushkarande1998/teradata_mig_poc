-- macros/like_any.sql
{% macro like_any(column, patterns) %}
(
  {% for pattern in patterns %}
    {{ column }} LIKE '{{ pattern }}'
    {% if not loop.last %} OR {% endif %}
  {% endfor %}
)
{% endmacro %}
