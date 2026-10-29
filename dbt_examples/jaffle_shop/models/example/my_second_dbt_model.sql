-- ref() makes dbt build my_first_dbt_model first.
select *
from {{ ref('my_first_dbt_model') }}
where id = 1
