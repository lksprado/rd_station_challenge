with 
leads as (
    select * from {{ ref('stg_rdstation__bi_funnel_email') }}
),
top_leads as (
    SELECT
        'Top 5 Campaign in Leads' as top,
        campaign_last_touch,
        icp_score,
        COUNT(id) AS qt_leads
    FROM leads
    GROUP BY campaign_last_touch, icp_score
    ORDER BY COUNT(id) DESC
    LIMIT 5
),
top_mql as (
    SELECT 
        'Top 5 Campaign in MQL' as top,
        campaign_last_touch,
        icp_score,
        COUNT(id) AS qt_leads
    FROM leads
    WHERE converted_to_mql is TRUE AND campaign_last_touch IS NOT NULL 
    GROUP BY campaign_last_touch, icp_score
    ORDER BY COUNT(id) DESC
    LIMIT 5
),
top_sal as (
    SELECT 
        'Top 5 Campaign in SAL' as top,
        campaign_last_touch,
        icp_score,
        COUNT(id) AS qt_leads
    FROM leads
    WHERE converted_to_sal is TRUE AND campaign_last_touch IS NOT NULL 
    GROUP BY campaign_last_touch, icp_score
    ORDER BY COUNT(id) DESC
    LIMIT 5
),
united AS (
	select * from top_leads
	union all 
	select * from top_mql
	union all 
	select * from top_sal
)
SELECT 
row_number() OVER (PARTITION BY top ORDER BY qt_leads DESC) AS RANK,
*
FROM united
ORDER BY top, qt_leads desc