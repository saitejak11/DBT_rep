{{
    config(
        materialized='incremental',
        uniquekey='uniquecustkey'
     
       
    )
}}
 
select distinct
    {{ dbt_utils.generate_surrogate_key(['uniquecustkey']) }} as cust_code,
    cust_key,
    cust_first_name,
    cust_street,
    cust_addr1,
    cust_addr2,
    cast(cust_city as varchar) as cust_city,
    md5_column,
    current_timestamp as SF_INSERT_DTTM,
    current_timestamp as SF_UPDATE_DTTM
from {{ ref('stg_dim_cust') }}
 
 
{% if is_incremental() %}
  where md5_column not in (select md5_column  from {{ this }})
{% endif %}
 