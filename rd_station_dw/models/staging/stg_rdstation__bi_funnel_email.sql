with
source as (
    select * from {{ source('rd_station','raw_bi_funnel_email') }}
),
clean_id as (
    select 
    *,
    row_number() over (partition by id) as rn
    from source
    where id is not null or id <> ''
),
deduped as (
    select * from clean_id
    where rn = 1
),
casting as (
    select 
    id,
    email,
    to_date(created_month,'YYYY-MM-DD') as created_month,
    (to_timestamp(created_date, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC' - interval '3 hours')::timestamp as created_date,
    to_date(lead_created_month,'YYYY-MM-DD') as lead_created_month,
    (to_timestamp(lead_created_date, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC' - interval '3 hours')::timestamp as lead_created_date,
    to_date(mql_created_month,'YYYY-MM-DD') as mql_created_month,
    (to_timestamp(mql_created_date, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC' - interval '3 hours')::timestamp as mql_created_date,
    to_date(sal_created_month,'YYYY-MM-DD') as sal_created_month,
    (to_timestamp(sal_created_date, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC' - interval '3 hours')::timestamp as sal_created_date,
    (to_timestamp(first_touch_date, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC' - interval '3 hours')::timestamp as first_touch_date,
    (to_timestamp(last_touch_date, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC' - interval '3 hours')::timestamp as last_touch_date,
    account_first_touch,
    account_last_touch,
    channel_first_touch,
    channel_last_touch,
    campaign_first_touch,
    campaign_last_touch,
    identifier_first_touch,
    identifier_last_touch,
    icp_score,
    lead_channel,
    lead_type,
    country,
    qty_conversions::int,
    is_new_lead::boolean,
    converted_to_mql::boolean,
    is_blank_mql::boolean,
    converted_to_sal::boolean,
    is_a::boolean,
    is_b::boolean,
    is_c::boolean,
    is_d::boolean,
    is_abc::boolean
    from deduped
)
select * from casting