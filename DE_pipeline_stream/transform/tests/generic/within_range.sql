{#
    A custom GENERIC test: reusable on any column, from YAML, like dbt's built-in
    not_null or unique:

        columns:
          - name: bikes_available
            data_tests:
              - within_range: {min_value: 0}

    A test is a query that returns the offending rows. Zero rows means it passes.
#}
{% test within_range(model, column_name, min_value=none, max_value=none) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and (
    {% if min_value is not none %} {{ column_name }} < {{ min_value }} {% else %} false {% endif %}
    or
    {% if max_value is not none %} {{ column_name }} > {{ max_value }} {% else %} false {% endif %}
  )
{% endtest %}
