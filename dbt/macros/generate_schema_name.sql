{# Use the configured schema (an Iceberg namespace) as-is instead of dbt's
   default "<target>_<custom>" naming. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
