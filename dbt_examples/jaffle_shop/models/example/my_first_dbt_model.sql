-- Materialized as a table; the project default (dbt_project.yml) is a view.
{{ config(materialized='table') }}

with source_data as (

    select 1 as id
    union all
    select 2 as id
    union all
    select null as id

)

select *
from source_data
where id is not null
