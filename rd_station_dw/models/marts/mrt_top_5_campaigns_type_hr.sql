with 
leads as (
    select * from {{ ref('stg_rdstation__bi_funnel_email') }}
)
SELECT
row_number() OVER (PARTITION BY 1 ORDER BY COUNT(id) DESC) AS RANK,
'Top 5 Campaigns Lead Type HR' AS top,
campaign_last_touch,
COUNT(id) AS qt_leads
FROM leads
WHERE lead_type = 'HR'
GROUP BY campaign_last_touch
ORDER BY COUNT(id) DESC
LIMIT 5