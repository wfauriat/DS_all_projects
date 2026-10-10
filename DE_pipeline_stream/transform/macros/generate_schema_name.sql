{#
    Where each layer's models go.

    By default, dbt names a custom schema "<target schema>_<custom>" (main_staging,
    main_marts), which makes sense when several people share one database, each
    with their own target schema. Here there is one warehouse, and each layer is
    simply its own schema: staging, intermediate, marts, snapshots, reference.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
