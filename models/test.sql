{{ config(materialized='table') }}

with source_data as (

    select mydate as _date, high as _high from 
    pc_dbt_db.dbt_ssuire.snowtable
)

select *
from source_data
