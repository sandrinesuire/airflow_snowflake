{{ config(materialized='table') }}

select * from {{ ref("prices_py")}} order by MYDATE