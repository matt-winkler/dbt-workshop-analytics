
{{
    config(
        materialized='incremental',
        unique_key='o_orderkey',
        alias = 'orders'
    )
}}

with source as (

    select * from {{ source('tpch', 'orders') }}

),

rename as (

    select
    
        {{ dbt_utils.surrogate_key(
            ['o_orderkey', 
            'getdate()']) }}
                as o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment,
        -- add field for freshness
        -- on full-refresh it will fail freshness
        -- on incremental run it will pass freshness
        
        {% if is_incremental() %}
            getdate()
        {% else %}
            dateadd(day,-1,getdate())
        {% endif %}
            as o_inserted_at

    from source
    

)

select * from rename
