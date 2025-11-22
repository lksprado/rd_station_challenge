with
source as (
    select * from {{ source('rd_station','raw_metas_email') }}
),
renamed as (
    select 
    conta,
    canal_mkt,
    etapa,
    perfil,
    mes::int,
    meta::int,
    to_date(data,'DD/MM/YYYY') as data
    from source
)
select * from renamed