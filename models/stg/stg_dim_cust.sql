with user as (
        select distinct
            cust_key,
            cust_first_name,
            cust_street,
            cust_addr1,
            cust_addr2,
            cust_city,
            current_timestamp as SF_INSERT_DTTM,
            current_timestamp as SF_UPDATE_DTTM
        from {{ source("src", "customer") }}
    -- DBT_DEV.SRC.CUSTOMER
    )
-- Main Select
select
    cust_key,
    cust_first_name,
    cust_street,
    cust_addr1,
    cust_addr2,
    cast(cust_city as varchar) as cust_city,
    (cust_key||'_'||cust_first_name||'_'||cust_street||'_'||cust_addr1||'_'||cust_addr2) as uniquecustkey,
    md5(nvl(cast(cust_key as varchar()), '')|| nvl(cast(cust_first_name as varchar()), '') || nvl(cast(cust_street as varchar()), '')|| nvl(cast(cust_addr1 as varchar()), '')|| nvl(cast(cust_addr2 as varchar()), '') ) as md5_column,
    current_timestamp as SF_INSERT_DTTM,
    current_timestamp as SF_UPDATE_DTTM
from user
 