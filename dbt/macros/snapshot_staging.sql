{#
  Override of dbt-spark's snapshot staging for Iceberg. Upstream creates a
  persistent view by bare name, assuming the default catalog is Spark's
  session catalog. Here the default catalog is the Iceberg REST catalog
  (`lake`), so the view lands in a namespace that doesn't exist and the second
  snapshot run fails (VIEW_NOT_FOUND). A temporary view needs no catalog, can
  still be DESCRIBEd for its columns, and is what the Iceberg MERGE path
  already references by bare identifier.
#}
{% macro spark_build_snapshot_staging_table(strategy, sql, target_relation) %}
    {%- set tmp_relation = api.Relation.create(identifier=target_relation.identifier ~ '__dbt_tmp',
                                               schema=none, database=none, type='view') -%}
    {% call statement('build_snapshot_staging_relation') %}
        create or replace temporary view {{ tmp_relation }} as
        {{ snapshot_staging_table(strategy, sql, target_relation) }}
    {% endcall %}
    {% do return(tmp_relation) %}
{% endmacro %}
