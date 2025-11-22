with 
leads as (
    select * from {{ ref('stg_rdstation__bi_funnel_email') }}
),
metas as (
    select * from {{ref('stg_rdstation__meta_email')}}
),
leads_base AS (
	SELECT 
	created_date::date,
	account_last_touch,
	icp_score AS perfil,
	lead_type,
	count(id) AS qt_leads_diario,
	count(CASE WHEN converted_to_mql IS true THEN id END) AS qt_mql_diario,
	count(CASE WHEN converted_to_sal IS true THEN id END) AS qt_sal_diario,
	count(DISTINCT email) AS qt_email_diario,
	max(last_touch_date) AS ultimo_horario_conversao_dia
	FROM leads
	WHERE EXTRACT(MONTH FROM created_date) >= 11
	GROUP BY 
	created_date::date,
	account_last_touch,
	icp_score,
	lead_type
),
leads_calculated AS (
	SELECT *,
	EXTRACT(MONTH FROM created_date) AS mes,
	CASE 
	    WHEN qt_leads_diario = 0 THEN 0
	    ELSE qt_mql_diario::float / qt_leads_diario
	END AS conversao_lead_para_mql_diario,
	CASE 
	    WHEN qt_mql_diario = 0 THEN 0
	    ELSE qt_sal_diario::float / qt_mql_diario
	END AS conversao_mql_para_sal_diario,
	SUM(qt_leads_diario) OVER(PARTITION BY DATE_TRUNC('month', created_date) ORDER BY created_date) AS mtd_leads,
	SUM(qt_mql_diario) OVER(PARTITION BY DATE_TRUNC('month', created_date) ORDER BY created_date) AS mtd_mql,
	SUM(qt_sal_diario) OVER(PARTITION BY DATE_TRUNC('month', created_date) ORDER BY created_date) AS mtd_sal
	FROM leads_base
),
metas_base AS (
    SELECT
        mes,
        conta,
        perfil,
        SUM(CASE WHEN etapa = 'LEAD' THEN meta END) AS meta_lead_mensal,
        SUM(CASE WHEN etapa = 'MQL'  THEN meta END) AS meta_mql_mensal,
        SUM(CASE WHEN etapa = 'SAL'  THEN meta END) AS meta_sal_mensal
    FROM metas
    GROUP BY 
    mes,
    conta,
    perfil
),
metas_com_diaria AS (
    SELECT
        mes,
        conta,
        perfil,
  		meta_lead_mensal,
  		meta_mql_mensal,
        meta_sal_mensal,
  		meta_lead_mensal / 30 AS meta_lead_diaria,
        meta_mql_mensal / 30 AS meta_mql_diaria,
        meta_sal_mensal / 30 AS meta_sal_diaria
    FROM metas_base
),
final_leads AS (
	SELECT
	created_date,
	account_last_touch AS conta,
	perfil,
	lead_type,
	qt_leads_diario,
	qt_mql_diario,
	qt_sal_diario,
	qt_email_diario,
	ultimo_horario_conversao_dia,
	mes,
	round(conversao_lead_para_mql_diario::numeric,2) AS conversao_lead_para_mql_diario,
	round(conversao_mql_para_sal_diario::numeric,2) AS conversao_mql_para_sal_diario,
	mtd_leads,
	mtd_mql,
	mtd_sal
	FROM leads_calculated
),
final_model AS (
	SELECT 
	t1.created_date,
	t1.conta,
	t1.perfil,
	t1.mes,
	t1.lead_type,
	t1.qt_leads_diario,
	t1.qt_mql_diario,
	t1.qt_sal_diario,
	t1.qt_email_diario,
	t1.ultimo_horario_conversao_dia,
	t1.conversao_lead_para_mql_diario,
	t1.conversao_mql_para_sal_diario,
	t1.mtd_leads,
	t1.mtd_mql,
	t1.mtd_sal,
	t2.meta_lead_mensal,
	t2.meta_mql_mensal,
	t2.meta_sal_mensal,
	t2.meta_lead_diaria,
	t2.meta_mql_diaria,
	t2.meta_sal_diaria
	FROM final_leads t1 
	JOIN metas_com_diaria t2 
	ON 1=1 
	AND t1.mes = t2.mes 
	AND t1.conta = t2.conta 
	AND t1.perfil = t2.perfil
)
SELECT * FROM final_model
