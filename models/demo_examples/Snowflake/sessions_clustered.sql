{{
  config(
    materialized='table',
    cluster_by=['session_start']
  )
}}

select
  o_custkey,
  min(o_orderdate) as session_start,
  max(o_orderdate) as session_end,
  count(*) as count_pageviews

from {{ source('tpch', 'orders') }}
group by 1