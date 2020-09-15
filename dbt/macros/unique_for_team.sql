{% macro test_unique_for_team(model) %}

{% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}

select count(*)
from (
    select
      {{ column_name }}

    from {{ model }}
    group by {{ column_name }}, team_name
    having count(*) > 1

) validation_errors

{% endmacro %}