
/*****************
--ADDING COMMENT -MICHAEL WATKIN'S DM CODE
*****************/


CREATE VOLATILE TABLE MW_DATES AS 
(
SELECT OD_DATE AS PULL_DT, WEEKSTARTDATE,
TRIM(FISCALWEEK_ID - 200)||TRIM(1) AS RANGESTART,
TRIM(FISCALWEEK_ID-1)||TRIM(6) AS RANGEEND,
WEEKSTARTDATE-3 AS D001,
WEEKSTARTDATE-33 AS D030,
WEEKSTARTDATE-93 AS D090,
WEEKSTARTDATE-183 AS D180,
WEEKSTARTDATE-363 AS D360,
WEEKSTARTDATE-728 AS D725,
WEEKSTARTDATE-1090 AS D1090
FROM OD.OD_DAY
WHERE OD_DATE = (SEL PULL_DT FROM CUSTOMER_V.CAMPAIGN_DIM WHERE CAMPAIGN_ID = 'DM1017')
) WITH DATA
ON COMMIT PRESERVE ROWS;


--USE THIS FOR TRAINING FROM CONTACT HISTORY
--IF THERE ARE RECORDS IN CONTACT HISTORY TO USE
/*
create multiset volatile table mw_ch as
(sel agent_id, 
CASE WHEN ((PIT_LIFECYCLE_HVB_NM LIKE 'HV%' OR PIT_LIFECYCLE_HVB_NM LIKE 'ACTIV%') AND PIT_CUSTOMER_TYPE_NM = 'BUSINESS') THEN 'BS'
			WHEN ((PIT_LIFECYCLE_HVB_NM NOT LIKE 'HV%' AND PIT_LIFECYCLE_HVB_NM NOT LIKE 'ACTIV%') AND PIT_CUSTOMER_TYPE_NM = 'BUSINESS') THEN 'BL'
			WHEN ((PIT_LIFECYCLE_HVB_NM LIKE 'HV%' OR PIT_LIFECYCLE_HVB_NM LIKE 'ACTIV%') AND PIT_CUSTOMER_TYPE_NM = 'HOME OFFICE') THEN 'HO'
			WHEN ((PIT_LIFECYCLE_HVB_NM NOT LIKE 'HV%' AND PIT_LIFECYCLE_HVB_NM NOT LIKE 'ACTIV%') AND PIT_CUSTOMER_TYPE_NM = 'HOME OFFICE') THEN 'HL'
			WHEN ((PIT_LIFECYCLE_HVB_NM LIKE 'HV%' OR PIT_LIFECYCLE_HVB_NM LIKE 'ACTIV%') AND PIT_CUSTOMER_TYPE_NM = 'CONSUMER') THEN 'CO'
			WHEN ((PIT_LIFECYCLE_HVB_NM NOT LIKE 'HV%' AND PIT_LIFECYCLE_HVB_NM NOT LIKE 'ACTIV%') AND PIT_CUSTOMER_TYPE_NM = 'CONSUMER') THEN 'CL'
			ELSE 'UNKN'
			END
			AS SEGMENT, 			
pit_lifecycle_hvb_nm LCYCLE,
PIT_CUSTOMER_TYPE_NM          as CTYPE,
PIT_CHANNEL_PREFERENCE_NM as CPREF

from customer_v.campaign_contact_history
where campaign_id = 'SP1118'
and demo_cd <> 'Z'
and PIT_CHANNEL_PREFERENCE_NM     <> 'CONTRACT ONLY'
and PIT_LIFECYCLE_HVB_NM not in ('INACTVE','PROSPECT')
)
with data
on commit preserve rows;*/



---IF CONTACT HISTORY IS BLEEPED UP USE THIS TO GO TO THE CERES TABLE



CREATE MULTISET VOLATILE TABLE MW_CH AS
(
SEL 
A.NEW_AGENT_ID AS AGENT_ID, 
CASE WHEN ((OVERALL_LIFECYCLE LIKE 'HV%' OR OVERALL_LIFECYCLE LIKE 'ACTIV%') AND CUSTOMER_TYPE = 'BUSINESS') THEN 'BS'
			WHEN ((OVERALL_LIFECYCLE NOT LIKE 'HV%' AND OVERALL_LIFECYCLE NOT LIKE 'ACTIV%') AND CUSTOMER_TYPE = 'BUSINESS') THEN 'BL'
			WHEN ((OVERALL_LIFECYCLE LIKE 'HV%' OR OVERALL_LIFECYCLE LIKE 'ACTIV%') AND CUSTOMER_TYPE = 'HOME OFFICE') THEN 'HO'
			WHEN ((OVERALL_LIFECYCLE NOT LIKE 'HV%' AND OVERALL_LIFECYCLE NOT LIKE 'ACTIV%') AND CUSTOMER_TYPE = 'HOME OFFICE') THEN 'HL'
			WHEN ((OVERALL_LIFECYCLE LIKE 'HV%' OR OVERALL_LIFECYCLE LIKE 'ACTIV%') AND CUSTOMER_TYPE = 'CONSUMER') THEN 'CO'
			WHEN ((OVERALL_LIFECYCLE NOT LIKE 'HV%' AND OVERALL_LIFECYCLE NOT LIKE 'ACTIV%') AND CUSTOMER_TYPE = 'CONSUMER') THEN 'CL'
			ELSE 'UNKN' END
			SEGMENT,
CHANNEL_PREFERENCE AS CPREF, 
OVERALL_LIFECYCLE AS LCYCLE,
CUSTOMER_TYPE AS CTYPE
FROM
OD.AGENT_OLD2NEW A
JOIN
CERES_USERDB.DM10_2017_FINAL_FINAL                                   B    
ON A.OLD_AGENT_ID = CAST(B.MAIL_AGENT_ID AS INT)
WHERE DEMO_CD <> 'Z'
AND CUSTOMER_TYPE <> 'CONSUMER'
AND CHANNEL_PREFERENCE                 <> 'CONTRACT ONLY'
AND OVERALL_LIFECYCLE NOT IN ('INACTVE','PROSPECT')
)
WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATS ON MW_CH INDEX (AGENT_ID);




--USE THIS FOR SCORING AGENT SETS
/*create multiset volatile table mw_ch as
(
sel 
 agent_id, 
channel_preference as ctype, 
lifecycle_new as lcycle
from
customer_v.mpa_agent_score
where dm_mailable_flag = 1
and (mkb_ty > 0
and sales_ty > 29.99
and Channel_Preference                 <> 'CONTRACT ONLY'
and overall_lifecycle not in ('INACTVE','PROSPECT'))
)
with data
on commit preserve rows;*/


---UNCOMMENT FOR SCORING PR/VI RECORDS
/*CREATE VOLATILE MULTISET TABLE VT_RJO_USTERR_SUPPRESSION AS (
SELECT AGENT_ID FROM  CERES_USERDB_V.MPA_SUPPRESSION
WHERE SUPPRESSION IN
       (
--INCLUDED IN PRELIMINARY SEGMENT BBD4
        'IMMEDIATE OPT OUT'
       ,'DO NOT USE FLAG'
       ,'ACXIOM IMMEDIATE OPT OUT'
       ,'DSF UNDELIVERABLE'
       ,'NCOA UNDELIVERABLE'
       ,'OFFICE DEPOT ADDRESS MATCH'
       ,'UNDELIVERABLE RETURNED MAIL'
       ,'SPECIAL CONTRACT SUPPRESS'
       ,'COMPETITION SUPPRESS'
       ,'PRISONS'
       ,'ACXIOM INACCURATE RECORD'
       ,'SAG OFFICES'
       ,'FTC OFFICES'
       --NEW TO SUPPRESSION TABLE; INCLUDED IN PRELIMINARY SEGMENT BBD4
       ,'ACTIVE CONTRACT SUPPRESS'
       --INCLUDED IN PRELIMINARY SEGMENTS BB78
       ,'MAIL OPT OUT'  --PURPOSE CODE 5
       ,'REL_MAIL_OPT' --PURPOSE CODE 5920 (COMPARABLE TO 5)
       ,'OMX_HARD_KILL_MAIL_OPT' --PURPOSE CODE 5005 (COMPARABLE TO 5005)
       ,'CANADIAN_PROVINCE_SUPPRESSION' --CANADIAN PROVINCE SUPPRESSIONS
       ,'DSF UNDELIVERABLE' --pURPOSE CODE 102              DUPLICATE LISTING
       ,'NCOA UNDELIVERABLE'  --PURPOSE CODE 104            DUPLICATE LISTING
       ,'DMA MAIL SUPPRESS NOT ACTIVE' --NEW TO SUPPRESSION TABLE; INCLUDED IN PRELIMINARY SEGMENT BB78
       --SUPPRESS ON BOTH CUSTOMER AND PROSPECT
       ,'NATIONAL ASSOCIATION OF HOME BUILDERS - NAH'
       ,'EXECUTIVE SUPPRESS'
       ,'IN-STORE PROMOTIONS'
       ,'MAILOPT OUT LOYALTY'
       ,'MXP_MAIL_OPT' --PURPOSE CODE 5702
       ,'SUSPENDED WORKLIFE CONTRACT SUPPRESS'
       ,'SUSPENDED WORKLIFE MEMBERS'
       ,'SUSPENDED STAR TEACHERS'
       ,'UNDELIVERABLE RETURNED MAIL' --DUPLICATE LISTING
       --'US TERRITORIES'
       ,'SPECIAL CONTRACT SUPPRESS'  --DUPLICATE LISTING
       ,'TAA CA'
       ,'SALES FORCE PROSPECT'
       ,'HGPO'
       ,'LOW IGM'
       ,'DECEASED/COMPANY NAME'
       ,'DECEASED/NO COMPANY NAME'
       ,'DECEASED_BUSINESS'
       ,'STORE LOCATION'  --ADDED ON 2013-10-22
       )
GROUP  BY 1)
WITH DATA
PRIMARY INDEX (AGENT_ID)
ON COMMIT PRESERVE ROWS;

CREATE VOLATILE SET TABLE VT_RJO_USTERR_UNIVERSE AS (
SELECT AGENT_ID, STATE_PROVINCE_CD 
FROM CUSTOMER_V.AGENT_FACT
WHERE STATE_PROVINCE_CD IN ('PR', 'VI'
)
AND AGENT_ID NOT IN (
SELECT AGENT_ID FROM  VT_RJO_USTERR_SUPPRESSION
GROUP  BY 1)
AND MKB_LTD_CNT > 0
)
WITH DATA
PRIMARY INDEX (AGENT_ID)
ON COMMIT PRESERVE ROWS;

insert into  mw_ch 
sel agent_id, channel_preference, lifecycle_new
from 
customer_v.mpa_agent_score
where dm_mailable_flag = 0
and (mkb_ty > 0
and sales_ty > 29.99
and Channel_Preference                 <> 'CONTRACT ONLY'
and overall_lifecycle not in ('INACTVE','PROSPECT'))
and agent_id in 
(sel agent_id from VT_RJO_USTERR_UNIVERSE )*/




CREATE MULTISET TABLE TEMPDB.MW_CPN_HIST_CNT
AS
(
SELECT 
A.PARENT_ORDER_ID, 
B.COUPON_ID, 
ALLOW_COMBINE_COUPON_IND,
IPS_VEHICLE_NM,
COUPON_DESC    ,
COUPON_USE_LIMIT_NUM,
PROMO_TYPE_CD,
SERIALIZED_NM,
MKT_VEHICLE_NM,
CASE WHEN COUPON_DESC LIKE '% OD GC %' THEN 1 ELSE 0 END GC_CPN,
LOB_ID,
MIN(A.ORDER_DT) ORDER_DATE,
MAX(A.AGENT_ID) AS AGENT_ID,
COUNT(DISTINCT B.COUPON_ID) AS PROMO_COUNT
FROM
CUSTOMER_V.MKT_SALES_DETAIL_FACT     A
JOIN
CUSTOMER_V.COUPON_IPS_DIM B
ON A.DISCOUNT_ID = B.COUPON_ID
WHERE AGENT_ID > 0 
 AND DISCOUNT_ID > 0
AND ORDER_DT <= (SELECT D001 FROM MW_DATES)
AND ORDER_DT >= (SEL D725 FROM MW_DATES)
AND AGENT_ID IN (SEL AGENT_ID FROM MW_CH)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
WITH DATA;

COLLECT STATS ON  TEMPDB.MW_CPN_HIST_CNT INDEX (PARENT_ORDER_ID);


create volatile table mw_coupon_vehicles_cnt
as
(

sel 
agent_id, 
ZEROIFNULL(SUM(CASE when vehicle  = 	'AFFILIATES'	and sel_recency < 91 THEN CPNS ELSE 0  end))	AFFILIATES_090D,
ZEROIFNULL(SUM(CASE when vehicle  = 	'AFFILIATES'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	AFFILIATES_180D,
ZEROIFNULL(SUM(CASE when vehicle  = 	'AFFILIATES'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	AFFILIATES_270D,
ZEROIFNULL(SUM(CASE when vehicle  = 	'AFFILIATES'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	AFFILIATES_365D,
ZEROIFNULL(SUM(CASE when vehicle  = 	'AFFILIATES'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	AFFILIATES_725D,
ZEROIFNULL(SUM(CASE when vehicle  = 	'BIG_EVENT'	and sel_recency < 91 THEN CPNS ELSE 0  end))	BIG_EVENT_090D,
ZEROIFNULL(SUM(CASE when vehicle  = 	'BIG_EVENT'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	BIG_EVENT_180D,
ZEROIFNULL(SUM(CASE when vehicle  = 	'BIG_EVENT'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	BIG_EVENT_270D,
ZEROIFNULL(SUM(CASE when vehicle  = 	'BIG_EVENT'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	BIG_EVENT_365D,
ZEROIFNULL(SUM(CASE when vehicle  = 	'BIG_EVENT'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	BIG_EVENT_725D,
ZEROIFNULL(SUM(CASE when vehicle  = 	'CATALOG'	and sel_recency < 91 THEN CPNS ELSE 0  end))	CATALOG_090D,
zeroifnull(sum(case when vehicle  = 	'CATALOG'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CATALOG_180D,
zeroifnull(sum(case when vehicle  = 	'CATALOG'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CATALOG_270D,
zeroifnull(sum(case when vehicle  = 	'CATALOG'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CATALOG_365D,
zeroifnull(sum(case when vehicle  = 	'CATALOG'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CATALOG_725D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT'	and sel_recency < 91 THEN CPNS ELSE 0  end))	CONTRACT_090D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CONTRACT_180D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CONTRACT_270D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CONTRACT_365D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CONTRACT_725D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_DM'	and sel_recency < 91 THEN CPNS ELSE 0  end))	CONTRACT_DM_090D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_DM'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CONTRACT_DM_180D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_DM'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CONTRACT_DM_270D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_DM'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CONTRACT_DM_365D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_DM'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CONTRACT_DM_725D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_EMAIL'	and sel_recency < 91 THEN CPNS ELSE 0  end))	CONTRACT_EMAIL_090D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_EMAIL'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CONTRACT_EMAIL_180D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_EMAIL'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CONTRACT_EMAIL_270D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_EMAIL'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CONTRACT_EMAIL_365D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_EMAIL'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CONTRACT_EMAIL_725D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_ONLINE'	and sel_recency < 91 THEN CPNS ELSE 0  end))	CONTRACT_ONLINE_090D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_ONLINE'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CONTRACT_ONLINE_180D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_ONLINE'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CONTRACT_ONLINE_270D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_ONLINE'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CONTRACT_ONLINE_365D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_ONLINE'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CONTRACT_ONLINE_725D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_OTHER'	and sel_recency < 91 THEN CPNS ELSE 0  end))	CONTRACT_OTHER_090D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_OTHER'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CONTRACT_OTHER_180D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_OTHER'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CONTRACT_OTHER_270D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_OTHER'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CONTRACT_OTHER_365D,
zeroifnull(sum(case when vehicle  = 	'CONTRACT_OTHER'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CONTRACT_OTHER_725D,
zeroifnull(sum(case when vehicle  = 	'CPD'	and sel_recency < 91 THEN CPNS ELSE 0  end))	CPD_090D,
zeroifnull(sum(case when vehicle  = 	'CPD'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CPD_180D,
zeroifnull(sum(case when vehicle  = 	'CPD'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CPD_270D,
zeroifnull(sum(case when vehicle  = 	'CPD'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CPD_365D,
zeroifnull(sum(case when vehicle  = 	'CPD'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CPD_725D,
zeroifnull(sum(case when vehicle  = 	'CREDIT_OFFER'	and sel_recency < 91 THEN CPNS ELSE 0  end))	CREDIT_OFFER_090D,
zeroifnull(sum(case when vehicle  = 	'CREDIT_OFFER'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CREDIT_OFFER_180D,
zeroifnull(sum(case when vehicle  = 	'CREDIT_OFFER'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CREDIT_OFFER_270D,
zeroifnull(sum(case when vehicle  = 	'CREDIT_OFFER'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CREDIT_OFFER_365D,
zeroifnull(sum(case when vehicle  = 	'CREDIT_OFFER'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CREDIT_OFFER_725D,
zeroifnull(sum(case when vehicle  = 	'CUSTOMER_SAT'	and sel_recency < 91 THEN CPNS ELSE 0  end))	CUSTOMER_SAT_090D,
zeroifnull(sum(case when vehicle  = 	'CUSTOMER_SAT'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CUSTOMER_SAT_180D,
zeroifnull(sum(case when vehicle  = 	'CUSTOMER_SAT'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CUSTOMER_SAT_270D,
zeroifnull(sum(case when vehicle  = 	'CUSTOMER_SAT'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CUSTOMER_SAT_365D,
zeroifnull(sum(case when vehicle  = 	'CUSTOMER_SAT'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CUSTOMER_SAT_725D,
zeroifnull(sum(case when vehicle  = 	'DIRECT_MAIL'	and sel_recency < 91 THEN CPNS ELSE 0  end))	DIRECT_MAIL_090D,
zeroifnull(sum(case when vehicle  = 	'DIRECT_MAIL'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	DIRECT_MAIL_180D,
zeroifnull(sum(case when vehicle  = 	'DIRECT_MAIL'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	DIRECT_MAIL_270D,
zeroifnull(sum(case when vehicle  = 	'DIRECT_MAIL'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	DIRECT_MAIL_365D,
zeroifnull(sum(case when vehicle  = 	'DIRECT_MAIL'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	DIRECT_MAIL_725D,
zeroifnull(sum(case when vehicle  = 	'EMAIL'	and sel_recency < 91 THEN CPNS ELSE 0  end))	EMAIL_090D,
zeroifnull(sum(case when vehicle  = 	'EMAIL'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	EMAIL_180D,
zeroifnull(sum(case when vehicle  = 	'EMAIL'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	EMAIL_270D,
zeroifnull(sum(case when vehicle  = 	'EMAIL'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	EMAIL_365D,
zeroifnull(sum(case when vehicle  = 	'EMAIL'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	EMAIL_725D,
zeroifnull(sum(case when vehicle  = 	'EMAIL - TRIGGERS'	and sel_recency < 91 THEN CPNS ELSE 0  end))	EMAILTRIGGERS_090D,
zeroifnull(sum(case when vehicle  = 	'EMAIL - TRIGGERS'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	EMAILTRIGGERS_180D,
zeroifnull(sum(case when vehicle  = 	'EMAIL - TRIGGERS'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	EMAILTRIGGERS_270D,
zeroifnull(sum(case when vehicle  = 	'EMAIL - TRIGGERS'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	EMAILTRIGGERS_365D,
zeroifnull(sum(case when vehicle  = 	'EMAIL - TRIGGERS'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	EMAILTRIGGERS_725D,
zeroifnull(sum(case when vehicle  = 	'EMAIL-ONBOARDING'	and sel_recency < 91 THEN CPNS ELSE 0  end))	EMAIL_ONBORD_090D,
zeroifnull(sum(case when vehicle  = 	'EMAIL-ONBOARDING'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	EMAIL_ONBORD_180D,
zeroifnull(sum(case when vehicle  = 	'EMAIL-ONBOARDING'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	EMAIL_ONBORD_270D,
zeroifnull(sum(case when vehicle  = 	'EMAIL-ONBOARDING'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	EMAIL_ONBORD_365D,
zeroifnull(sum(case when vehicle  = 	'EMAIL-ONBOARDING'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	EMAIL_ONBORD_725D,
zeroifnull(sum(case when vehicle  = 	'FIXBUNDLE'	and sel_recency < 91 THEN CPNS ELSE 0  end))	FIXBUNDLE_090D,
zeroifnull(sum(case when vehicle  = 	'FIXBUNDLE'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	FIXBUNDLE_180D,
zeroifnull(sum(case when vehicle  = 	'FIXBUNDLE'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	FIXBUNDLE_270D,
zeroifnull(sum(case when vehicle  = 	'FIXBUNDLE'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	FIXBUNDLE_365D,
zeroifnull(sum(case when vehicle  = 	'FIXBUNDLE'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	FIXBUNDLE_725D,
zeroifnull(sum(case when vehicle  = 	'INSERT'	and sel_recency < 91 THEN CPNS ELSE 0  end))	INSERT_090D,
zeroifnull(sum(case when vehicle  = 	'INSERT'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	INSERT_180D,
zeroifnull(sum(case when vehicle  = 	'INSERT'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	INSERT_270D,
zeroifnull(sum(case when vehicle  = 	'INSERT'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	INSERT_365D,
zeroifnull(sum(case when vehicle  = 	'INSERT'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	INSERT_725D,
zeroifnull(sum(case when vehicle  = 	'INSTORE'	and sel_recency < 91 THEN CPNS ELSE 0  end))	INSTORE_090D,
zeroifnull(sum(case when vehicle  = 	'INSTORE'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	INSTORE_180D,
zeroifnull(sum(case when vehicle  = 	'INSTORE'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	INSTORE_270D,
zeroifnull(sum(case when vehicle  = 	'INSTORE'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	INSTORE_365D,
zeroifnull(sum(case when vehicle  = 	'INSTORE'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	INSTORE_725D,
zeroifnull(sum(case when vehicle  = 	'LOCAL_MARKETING'	and sel_recency < 91 THEN CPNS ELSE 0  end))	LOCAL_MARKETING_090D,
zeroifnull(sum(case when vehicle  = 	'LOCAL_MARKETING'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	LOCAL_MARKETING_180D,
zeroifnull(sum(case when vehicle  = 	'LOCAL_MARKETING'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	LOCAL_MARKETING_270D,
zeroifnull(sum(case when vehicle  = 	'LOCAL_MARKETING'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	LOCAL_MARKETING_365D,
zeroifnull(sum(case when vehicle  = 	'LOCAL_MARKETING'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	LOCAL_MARKETING_725D,
zeroifnull(sum(case when vehicle  = 	'LOYALTY'	and sel_recency < 91 THEN CPNS ELSE 0  end))	LOYALTY_090D,
zeroifnull(sum(case when vehicle  = 	'LOYALTY'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	LOYALTY_180D,
zeroifnull(sum(case when vehicle  = 	'LOYALTY'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	LOYALTY_270D,
zeroifnull(sum(case when vehicle  = 	'LOYALTY'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	LOYALTY_365D,
zeroifnull(sum(case when vehicle  = 	'LOYALTY'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	LOYALTY_725D,
zeroifnull(sum(case when vehicle  = 	'LOYALTY_GHOST'	and sel_recency < 91 THEN CPNS ELSE 0  end))	LOYALTY_GHOST_090D,
zeroifnull(sum(case when vehicle  = 	'LOYALTY_GHOST'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	LOYALTY_GHOST_180D,
zeroifnull(sum(case when vehicle  = 	'LOYALTY_GHOST'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	LOYALTY_GHOST_270D,
zeroifnull(sum(case when vehicle  = 	'LOYALTY_GHOST'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	LOYALTY_GHOST_365D,
zeroifnull(sum(case when vehicle  = 	'LOYALTY_GHOST'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	LOYALTY_GHOST_725D,
zeroifnull(sum(case when vehicle  = 	'MOBILE'	and sel_recency < 91 THEN CPNS ELSE 0  end))	MOBILE_090D,
zeroifnull(sum(case when vehicle  = 	'MOBILE'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	MOBILE_180D,
zeroifnull(sum(case when vehicle  = 	'MOBILE'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	MOBILE_270D,
zeroifnull(sum(case when vehicle  = 	'MOBILE'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	MOBILE_365D,
zeroifnull(sum(case when vehicle  = 	'MOBILE'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	MOBILE_725D,
zeroifnull(sum(case when vehicle  = 	'ONLINE'	and sel_recency < 91 THEN CPNS ELSE 0  end))	ONLINE_090D,
zeroifnull(sum(case when vehicle  = 	'ONLINE'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	ONLINE_180D,
zeroifnull(sum(case when vehicle  = 	'ONLINE'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	ONLINE_270D,
zeroifnull(sum(case when vehicle  = 	'ONLINE'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	ONLINE_365D,
zeroifnull(sum(case when vehicle  = 	'ONLINE'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	ONLINE_725D,
zeroifnull(sum(case when vehicle  = 	'ONLINE_A'	and sel_recency < 91 THEN CPNS ELSE 0  end))	ONLINE_A_090D,
zeroifnull(sum(case when vehicle  = 	'ONLINE_A'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	ONLINE_A_180D,
zeroifnull(sum(case when vehicle  = 	'ONLINE_A'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	ONLINE_A_270D,
zeroifnull(sum(case when vehicle  = 	'ONLINE_A'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	ONLINE_A_365D,
zeroifnull(sum(case when vehicle  = 	'ONLINE_A'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	ONLINE_A_725D,
zeroifnull(sum(case when vehicle  = 	'ONLINE_B'	and sel_recency < 91 THEN CPNS ELSE 0  end))	ONLINE_B_090D,
zeroifnull(sum(case when vehicle  = 	'ONLINE_B'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	ONLINE_B_180D,
zeroifnull(sum(case when vehicle  = 	'ONLINE_B'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	ONLINE_B_270D,
zeroifnull(sum(case when vehicle  = 	'ONLINE_B'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	ONLINE_B_365D,
zeroifnull(sum(case when vehicle  = 	'ONLINE_B'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	ONLINE_B_725D,
zeroifnull(sum(case when vehicle  = 	'ONLINE-WAP'	and sel_recency < 91 THEN CPNS ELSE 0  end))	ONLINE_WAP_090D,
zeroifnull(sum(case when vehicle  = 	'ONLINE-WAP'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	ONLINE_WAP_180D,
zeroifnull(sum(case when vehicle  = 	'ONLINE-WAP'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	ONLINE_WAP_270D,
zeroifnull(sum(case when vehicle  = 	'ONLINE-WAP'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	ONLINE_WAP_365D,
zeroifnull(sum(case when vehicle  = 	'ONLINE-WAP'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	ONLINE_WAP_725D,
zeroifnull(sum(case when vehicle  = 	'OTHER'	and sel_recency < 91 THEN CPNS ELSE 0  end))	OTHER_090D,
zeroifnull(sum(case when vehicle  = 	'OTHER'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	OTHER_180D,
zeroifnull(sum(case when vehicle  = 	'OTHER'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	OTHER_270D,
zeroifnull(sum(case when vehicle  = 	'OTHER'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	OTHER_365D,
zeroifnull(sum(case when vehicle  = 	'OTHER'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	OTHER_725D,
zeroifnull(sum(case when vehicle  = 	'OVERRIDES'	and sel_recency < 91 THEN CPNS ELSE 0  end))	OVERRIDES_090D,
zeroifnull(sum(case when vehicle  = 	'OVERRIDES'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	OVERRIDES_180D,
zeroifnull(sum(case when vehicle  = 	'OVERRIDES'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	OVERRIDES_270D,
zeroifnull(sum(case when vehicle  = 	'OVERRIDES'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	OVERRIDES_365D,
zeroifnull(sum(case when vehicle  = 	'OVERRIDES'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	OVERRIDES_725D,
zeroifnull(sum(case when vehicle  = 	'PERSONALIZED_POS_BB'	and sel_recency < 91 THEN CPNS ELSE 0  end))	PERSONALIZED_POS_BB_090D,
zeroifnull(sum(case when vehicle  = 	'PERSONALIZED_POS_BB'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	PERSONALIZED_POS_BB_180D,
zeroifnull(sum(case when vehicle  = 	'PERSONALIZED_POS_BB'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	PERSONALIZED_POS_BB_270D,
zeroifnull(sum(case when vehicle  = 	'PERSONALIZED_POS_BB'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	PERSONALIZED_POS_BB_365D,
zeroifnull(sum(case when vehicle  = 	'PERSONALIZED_POS_BB'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	PERSONALIZED_POS_BB_725D,
zeroifnull(sum(case when vehicle  = 	'PLAYBOOK'	and sel_recency < 91 THEN CPNS ELSE 0  end))	PLAYBOOK_090D,
zeroifnull(sum(case when vehicle  = 	'PLAYBOOK'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	PLAYBOOK_180D,
zeroifnull(sum(case when vehicle  = 	'PLAYBOOK'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	PLAYBOOK_270D,
zeroifnull(sum(case when vehicle  = 	'PLAYBOOK'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	PLAYBOOK_365D,
zeroifnull(sum(case when vehicle  = 	'PLAYBOOK'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	PLAYBOOK_725D,
zeroifnull(sum(case when vehicle  = 	'PRODUCT_PROTECTION'	and sel_recency < 91 THEN CPNS ELSE 0  end))	PRODUCT_PROTECTION_090D,
zeroifnull(sum(case when vehicle  = 	'PRODUCT_PROTECTION'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	PRODUCT_PROTECTION_180D,
zeroifnull(sum(case when vehicle  = 	'PRODUCT_PROTECTION'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	PRODUCT_PROTECTION_270D,
zeroifnull(sum(case when vehicle  = 	'PRODUCT_PROTECTION'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	PRODUCT_PROTECTION_365D,
zeroifnull(sum(case when vehicle  = 	'PRODUCT_PROTECTION'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	PRODUCT_PROTECTION_725D,
zeroifnull(sum(case when vehicle  = 	'RADIO'	and sel_recency < 91 THEN CPNS ELSE 0  end))	RADIO_090D,
zeroifnull(sum(case when vehicle  = 	'RADIO'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	RADIO_180D,
zeroifnull(sum(case when vehicle  = 	'RADIO'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	RADIO_270D,
zeroifnull(sum(case when vehicle  = 	'RADIO'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	RADIO_365D,
zeroifnull(sum(case when vehicle  = 	'RADIO'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	RADIO_725D,
zeroifnull(sum(case when vehicle  = 	'TRUEBUNDLE'	and sel_recency < 91 THEN CPNS ELSE 0  end))	TRUEBUNDLE_090D,
zeroifnull(sum(case when vehicle  = 	'TRUEBUNDLE'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	TRUEBUNDLE_180D,
zeroifnull(sum(case when vehicle  = 	'TRUEBUNDLE'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	TRUEBUNDLE_270D,
zeroifnull(sum(case when vehicle  = 	'TRUEBUNDLE'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	TRUEBUNDLE_365D,
zeroifnull(sum(case when vehicle  = 	'TRUEBUNDLE'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	TRUEBUNDLE_725D,
zeroifnull(sum(case when vehicle  = 	'UNKN'	and sel_recency < 91 THEN CPNS ELSE 0 end))	UNKN_090D,
zeroifnull(sum(case when vehicle  = 	'UNKN'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	UNKN_180D,
zeroifnull(sum(case when vehicle  = 	'UNKN'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	UNKN_270D,
zeroifnull(sum(case when vehicle  = 	'UNKN'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	UNKN_365D,
zeroifnull(sum(case when vehicle  = 	'UNKN'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	UNKN_725D,
zeroifnull(sum(case when vehicle  = 	'WEB_WELCOME_EMAIL'	and sel_recency < 91 THEN CPNS ELSE 0  end))	WEB_WELCOME_EMAIL_090D,
zeroifnull(sum(case when vehicle  = 	'WEB_WELCOME_EMAIL'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	WEB_WELCOME_EMAIL_180D,
zeroifnull(sum(case when vehicle  = 	'WEB_WELCOME_EMAIL'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	WEB_WELCOME_EMAIL_270D,
zeroifnull(sum(case when vehicle  = 	'WEB_WELCOME_EMAIL'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	WEB_WELCOME_EMAIL_365D,
zeroifnull(sum(case when vehicle  = 	'WEB_WELCOME_EMAIL'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	WEB_WELCOME_EMAIL_725D,
zeroifnull(sum(case when vehicle  = 	'WEB1'	and sel_recency < 91 THEN CPNS ELSE 0  end))	WEB1_090D,
zeroifnull(sum(case when vehicle  = 	'WEB1'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	WEB1_180D,
zeroifnull(sum(case when vehicle  = 	'WEB1'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	WEB1_270D,
zeroifnull(sum(case when vehicle  = 	'WEB1'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	WEB1_365D,
zeroifnull(sum(case when vehicle  = 	'WEB1'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	WEB1_725D,
zeroifnull(sum(case when vehicle  = 	'WEB2'	and sel_recency < 91 THEN CPNS ELSE 0  end))	WEB2_090D,
zeroifnull(sum(case when vehicle  = 	'WEB2'	and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	WEB2_180D,
zeroifnull(sum(case when vehicle  = 	'WEB2'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	WEB2_270D,
zeroifnull(sum(case when vehicle  = 	'WEB2'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	WEB2_365D,
zeroifnull(sum(case when vehicle  = 	'WEB2'	and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	WEB2_725D,
zeroifnull(sum(case when ALLOW_COMBINE_COUPON_IND      = 'Y' and sel_recency < 91 THEN CPNS ELSE 0 end))	STACK_090D,
zeroifnull(sum(case when ALLOW_COMBINE_COUPON_IND    = 	'Y' and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	STACK_180D,
zeroifnull(sum(case when ALLOW_COMBINE_COUPON_IND     = 'Y'	and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	STACK_270D,
zeroifnull(sum(case when ALLOW_COMBINE_COUPON_IND      = 'Y'	and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	STACK_365D,
zeroifnull(sum(case when ALLOW_COMBINE_COUPON_IND      =  'Y' and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	STACK_725D,

zeroifnull(sum(case when GC_CPN = 1 and sel_recency < 91 THEN CPNS ELSE 0  end))	GCN_090D,
zeroifnull(sum(case when GC_CPN = 1 and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	GCN_180D,
zeroifnull(sum(case when GC_CPN = 1 and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	GCN_270D,
zeroifnull(sum(case when GC_CPN = 1 and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	GCN_365D,
zeroifnull(sum(case when GC_CPN = 1 and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	GCN_725D,


zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '3'  and sel_recency < 91 THEN CPNS ELSE 0  end))	CPN_03_090D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '3'  and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CPN_03_180D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '3'  and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CPN_03_270D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '3'  and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CPN_03_365D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '3'  and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CPN_03_725D,

zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '2B'  and sel_recency < 91 THEN CPNS ELSE 0 end))	CPN_02B_090D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '2B'  and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CPN_02B_180D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '2B'  and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CPN_02B_270D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '2B'  and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CPN_02B_365D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '2B'  and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CPN_02B_725D,

zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '2A'  and sel_recency < 91 THEN CPNS ELSE 0 end))	CPN_02A_090D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '2A'  and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CPN_02A_180D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '2A'  and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CPN_02A_270D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '2A'  and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CPN_02A_365D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '2A'  and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CPN_02A_725D,

zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '1A'  and sel_recency < 91 THEN CPNS ELSE 0 end))	CPN_01A_090D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '1A'  and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CPN_01A_180D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '1A'  and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CPN_01A_270D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '1A'  and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CPN_01A_365D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '1A'  and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CPN_01A_725D,

zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '4'  and sel_recency < 91 THEN CPNS ELSE 0 end))	CPN_04_090D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '4'  and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CPN_04_180D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '4'  and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CPN_04_270D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '4'  and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CPN_04_365D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '4'  and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CPN_04_725D,

zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '7'  and sel_recency < 91 THEN CPNS ELSE 0 end))	CPN_07_090D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '7'  and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CPN_07_180D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '7'  and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CPN_07_270D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '7'  and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CPN_07_365D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '7'  and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CPN_07_725D,

zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '8B'  and sel_recency < 91 THEN CPNS ELSE 0 end))	CPN_08B_090D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '8B'  and sel_recency >  90 and sel_recency < 181  THEN CPNS ELSE 0 end))	CPN_08B_180D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '8B'  and sel_recency >  180 and sel_recency < 271  THEN CPNS ELSE 0 end))	CPN_08B_270D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '8B'  and sel_recency >  270 and sel_recency < 366  THEN CPNS ELSE 0 end))	CPN_08B_365D,
zeroifnull(sum(case when TRIM(PROMO_TYPE_CD) = '8B'  and sel_recency >  365 and sel_recency < 726  THEN CPNS ELSE 0 end)/4)	CPN_08B_725D


FROM 
(
sel agent_id, 
(select D001 from mw_dates)-order_date sel_recency,
PROMO_TYPE_CD, 
GC_CPN,
case when mkt_vehicle_nm is null then 'UNKN' else upper(mkt_vehicle_nm) end as vehicle,  
ALLOW_COMBINE_COUPON_IND      ,
count(distinct coupon_id) as cpns
from tempdb.mw_cpn_hist_CNT
group by 1,2,3,4,5,6
) DERV
GROUP BY 1
)
with data
on commit preserve rows;


collect stats on mw_coupon_vehicles_cnt index (agent_id);


create multiset table tempdb.mw_campaign_selection as
(
select 
distinct 
Z.agent_id,
b.campaign_id,
b.pull_dt,
c.fiscalweek_id
FROM MW_CH Z
LEFT JOIN 
 customer_v.campaign_contact_history a
ON Z.AGENT_ID = A.AGENT_ID
join
customer_v.campaign_dim b
on a.campaign_id = b.campaign_id 
join 
od.od_day c
on b.pull_dt= c.od_date
where b.pull_dt between (SELECT D725 FROM MW_DATES)
AND 
(SELECT d001 FROM MW_DATES)
and a.demo_cd <> 'Z'
and z.agent_id in (Sel agent_id from mw_ch)
)
WITH DATA;



create multiset table tempdb.mw_lpm_monthly_CNT as 
(
select 
parent_order_id, 
prom_id, 
max(a.agent_id) agent_id, 
PROM_TYPE_CD     ,      
min(order_dt) as order_dt,
count(distinct prom_id) as prom_cnt                  
from 
CUSTOMER_V.MKT_SALES_DETAIL_FACT     a
 join
customer_v.LOYALTY_ORDER_PROMOTION_FACT  b
 on a.parent_order_id = b.order_id
 and a.item_id = b.item_id
 where agent_id > 0
 AND ORDER_DT <= (SELECT D001 FROM MW_DATES)
AND ORDER_DT >= (SEL D725 FROM MW_DATES)
and agent_id in (sel agent_id from mw_ch)
and prom_id in (Sel prom_id from      gdw_v.LOY_PROMOTION   
where prom_issuance_frequency_cd = 'MONTHLY')
group by 1,2,4
) with data;

collect stats on  tempdb.mw_campaign_selection index (agent_id);

--drop table mw_loyal_monthly;


create multiset volatile table 
mw_loyal_monthly_cnt
as
(

sel agent_id, 
zeroifnull(sum(case when sel_recency < 91 then daily_prom_amt else 0 end))	LPM_090D,
zeroifnull(sum(case when sel_recency > 90 and sel_recency < 181 then daily_prom_amt else 0 end))	LPM_180D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 then daily_prom_amt else 0 end))	LPM_270D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 then daily_prom_amt else 0 end))	LPM_365D,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 then daily_prom_amt else 0 end))	LPM_725D
FROM
(
select agent_id, 
(select D001 from mw_dates)-order_dt sel_recency,
count(distinct prom_id) daily_prom_amt
from  tempdb.mw_lpm_monthly_cnt
group by 1,2
) DERIVED
GROUP BY 1

) 

with data on commit preserve rows;

collect stats on mw_loyal_monthly_cnt index (agent_id);

create multiset table tempdb.mw_lpm_daily_cnt as 
(
select 
parent_order_id, 
prom_id, 
max(a.agent_id) agent_id, 
PROM_TYPE_CD     ,      
min(order_dt) as order_dt,
count(distinct prom_id) as promo_cnt              
from 
CUSTOMER_V.MKT_SALES_DETAIL_FACT     a
 join
customer_v.LOYALTY_ORDER_PROMOTION_FACT  b
 on a.parent_order_id = b.order_id
 and a.item_id = b.item_id
 where agent_id > 0
 AND ORDER_DT <= (SELECT D001 FROM MW_DATES)
AND ORDER_DT >= (SEL D725 FROM MW_DATES)
and agent_id in (sel agent_id from mw_ch)
and prom_id in (Sel prom_id from      gdw_v.LOY_PROMOTION   
where prom_issuance_frequency_cd = 'daily')
group by 1,2,4
) with data;

collect stats on  tempdb.mw_lpm_daily_cnt  index (parent_order_id);

create multiset volatile table 
mw_loyal_daily_cnt
as
(

sel agent_id, 
zeroifnull(sum(case when sel_recency < 91 then daily_prom_amt else 0 end))	LPM_090D,
zeroifnull(sum(case when sel_recency > 90 and sel_recency < 181 then daily_prom_amt else 0 end))	LPM_180D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 then daily_prom_amt else 0 end))	LPM_270D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 then daily_prom_amt else 0 end))	LPM_365D,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 then daily_prom_amt else 0 end))	LPM_725D
FROM
(
select agent_id, 
(select D001 from mw_dates)-order_dt sel_recency,
sum(promo_cnt) daily_prom_amt
from  tempdb.mw_lpm_daily_cnt
group by 1,2
) DERIVED
GROUP BY 1

) 

with data on commit preserve rows;



create multiset  table tempdb.mw_odc_sessions as 
(
select 
d.session_id,
ENTRY_PAGE_TYPE_CD,
D.MARKETING_CHANNEL_NM ,
ITEM_OOS_FLG,
MARKETING_PLACEMENT_TXT,
E.DMA_DESC                      ,
E.WEB_APPLICATION_NM            ,
OPERATING_SYSTEM_DESC         ,
TIME_ZONE_DESC                ,
MOBILE_NETWORK_NM        ,     
CONNECTION_TYPE_NM        ,    
CONNECTION_SPEED_NM      ,     
MOBILE_DEVICE_IND             ,
MOBILE_DEVICE_NM              ,
DEVICE_MARKETING_NM       ,    
DEVICE_MODEL_NM               ,
max(agent_id) agent_id, 
max(cast(d.click_ts as date)) click_date,
max(buy_session_flg) as buysess, 
max(check_out_initiated_flg) as checkout,
  MAX
            ( CASE WHEN Z.ITEM_DEPARTMENT_ID IN (1,8,23)
                THEN 1
                ELSE 0
                END
            )
            BR_FURNITUR,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID IN 
(28,3)
                THEN 1
                ELSE 0 
                END
            ) BR_FILEBND,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID IN
(52)
                THEN 1
                ELSE 0 
                END
            ) BR_SPPAPER ,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in
(51,5)
AND  STANDARD_UOM_CD = 'CA'
AND Z.SKU_NUM<> '348037'
                THEN 1
                ELSE 0 
                END
            ) BR_CASEPAPER,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in
(51,5)
AND  STANDARD_UOM_CD = 'RM'
AND Z.SKU_NUM<> '348037'
                THEN 1
                ELSE 0 
                END
            ) BR_REAMPAPER,
 MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in
(51,5)
AND Z.SKU_NUM=  '348037'
                THEN 1
                ELSE 0 
                END
            )BR_REDPAPER,
MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in
(51,5)
AND Z.SKU_NUM<>  '348037'
AND   STANDARD_UOM_CD NOT IN ( 'CA','RM')
                THEN 1
                ELSE 0 
                END
            )BR_PAPEROTH,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 14 
                THEN 1
                ELSE 0 
                END) BR_INKJETC   ,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 65
                THEN 1
                ELSE 0 
END) BR_TONER,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 79
                THEN 1
                ELSE 0 
                END
            ) 
                BR_MANAGEDPRINT,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in (12,16,50)
                THEN 1
                ELSE 0 
                END
            ) 
                BR_PRINTERS,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID IN( 10,63)
                THEN 1
                ELSE 0 
                END
            ) 
                BR_COMPUTERS,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 9
                THEN 1
                ELSE 0 
                END
            ) BR_PCACC ,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in (25,27)
                THEN 1
                ELSE 0 
                END
            ) BR_SEATING ,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 71
                THEN 1
                ELSE 0 
                END
            ) BR_SOFTWARE,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in (21,74,69,62,61)
                THEN 1
                ELSE 0 
                END
            ) BR_STORGANDNETW,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in (26,48)
                THEN 1
                ELSE 0 
                END
            ) BR_MONOPROJO,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in (99,32,29)
                THEN 1
                ELSE 0 
                END
            ) BR_DIGPRINTDOC,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in (44,45)
                THEN 1
                ELSE 0 
                END
            ) BR_OFESSNTLS,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 7
                THEN 1
                ELSE 0 
                END
            ) BR_SCHOOLSPPL,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 86
                 THEN 1
                 ELSE 0 
                 END
            ) BR_BUSSERV,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 98
                 THEN 1
                 ELSE 0 
                 END
            ) BR_MAILSHIP,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID in (4,54)
                 THEN 1
                 ELSE 0 
                 END
            ) BR_DATEDG,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID IN (17,66,41,2)
                 THEN 1
                 ELSE 0 
                 END
            ) BR_BUSMACH,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 81
                    THEN 1
                    ELSE 0 
                    END
            ) BR_BAGSLUG,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 47
                THEN 1
                ELSE 0 
                END
            ) 
            BR_CLEANING ,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 43
                THEN 1 
                ELSE 0 
                END
            ) BR_BREAKROOM,
        MAX 
            (CASE WHEN Z.ITEM_DEPARTMENT_ID = 19
             THEN 1 
             ELSE 0 
             END
            ) BR_MOBILIT ,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID IN (58,24)
                 THEN 1
                ELSE 0 
                END
            ) BR_PRESENTACC,

        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID = 91
                THEN 1 
                ELSE 0 END) BR_POSTAGE  ,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID IN (42,49,6,85)
                THEN 1
                ELSE 0 END) BR_LBLSHIPSTOR,
        MAX 
            (
                CASE WHEN Z.ITEM_DEPARTMENT_ID IN (64,46,18)
                THEN 1 
                ELSE 0 END) 
    BR_PORTABLECE,
       
        MAX 
            (
                CASE WHEN Z.ITEM_GROUP_ID = 39
AND Z.item_desc NOT like 'Loyalty%'
                THEN 1
                ELSE 0 END) BR_BSD,
        MAX 
            (
                CASE WHEN Z.ITEM_GROUP_ID = 41
                THEN 1
                ELSE 0 END) BR_TECHSERV,
MAX
            (
                CASE WHEN  Z.item_desc like 'Loyalty%'
THEN 1
                ELSE 0 END) BR_LOYALACCRU

from GDW_VIEWS.CLICKSTREAM_DETAIL D
left join
GDW_VIEWS.CLICKSTREAM_GEOGRAPHY E
ON D.SESSION_ID = E.SESSION_ID
LEFT JOIN
customer_v.CS_SITE_PROMO_CLICK_DIR F
ON D.SESSION_ID = F.SESSION_ID
LEFT JOIN
customer_v.CS_TECH_PROPERTIES_DIR T
on D.session_id = T.session_id
left join
customer_v. ITEM_HIERARCHY_DIM    Z
on d.item_id = Z.item_id

--LEFT JOIN
--customer_v.CS_ELEMENT_DIR   H
--ON D.SESSION_ID = H.SESSION_ID 
    where agent_id > 0
     and agent_id in 
     (
         select agent_id
         from mw_ch
)
 
and 
cast(d.click_ts as date) >= (sel d725 from mw_dates)
  and cast(d.click_ts as date) <= (sel  d001 from mw_dates)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
  with data;
  
create multiset  table tempdb.mw_dept_dm_CNT
as
(
select 
a.parent_order_id, 
max(a.agent_id) agent_id,
min(order_dt) as ord_dt,
max(case when lob_id = 2 then 1 else 0 end) as ccenter,
max(case when lob_id in (3,63) then 1 else 0 end) as retail,
max(case when lob_id = 5 then 1 else 0 end) online,
max(0) as anon_shop,
count(distinct agent_id) as RELATED_AGENTS,
max(MARKET_APPL_ID) as MKT_APPL_ID,
MAX(CASE WHEN ITEM_SOURCE_CD =	'UT'	then 	1	else 0 end)	DLSC_UT	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'UR'	then 	1	else 0 end)	DLSC_UR	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'UN'	then 	1	else 0 end)	DLSC_UN	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'UD'	then 	1	else 0 end)	DLSC_UD	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'TD'	then 	1	else 0 end)	DLSC_TD	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'ST'	then 	1	else 0 end)	DLSC_ST	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'SR'	then 	1	else 0 end)	DLSC_SR	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'SP'	then 	1	else 0 end)	DLSC_SP	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'SD'	then 	1	else 0 end)	DLSC_SD	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'RB'	then 	1	else 0 end)	DLSC_RB	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'R2'	then 	1	else 0 end)	DLSC_R2	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'R1'	then 	1	else 0 end)	DLSC_R1	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'PR'	then 	1	else 0 end)	DLSC_PR	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'OT'	then 	1	else 0 end)	DLSC_OT	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'ND'	then 	1	else 0 end)	DLSC_ND	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'NC'	then 	1	else 0 end)	DLSC_NC	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'MF'	then 	1	else 0 end)	DLSC_MF	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'IM'	then 	1	else 0 end)	DLSC_IM	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'HC'	then 	1	else 0 end)	DLSC_HC	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'DV'	then 	1	else 0 end)	DLSC_DV	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'DS'	then 	1	else 0 end)	DLSC_DS	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'DL'	then 	1	else 0 end)	DLSC_DL	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'DF'	then 	1	else 0 end)	DLSC_DF	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'DE'	then 	1	else 0 end)	DLSC_DE	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'CW'	then 	1	else 0 end)	DLSC_CW	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'CT'	then 	1	else 0 end)	DLSC_CT	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'CS'	then 	1	else 0 end)	DLSC_CS	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'CR'	then 	1	else 0 end)	DLSC_CR	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'CF'	then 	1	else 0 end)	DLSC_CF	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'CD'	then 	1	else 0 end)	DLSC_CD	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'CA'	then 	1	else 0 end)	DLSC_CA	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'AD'	then 	1	else 0 end)	DLSC_AD	,
MAX(CASE WHEN ITEM_SOURCE_CD =	'00'	then 	1	else 0 end)	DLSC_00	,
MAX(CASE WHEN SALES_TYPE_CD = 	1	then 	1	else 0 end)	SLSTYP_GROSS,
MAX(CASE WHEN SALES_TYPE_CD = 	2	then 	1	else 0 end)	SLSTYP_RETRN,
MAX(CASE WHEN SALES_TYPE_CD = 	3	then 	1	else 0 end)	SLSTYP_MIREBATE,
MAX(CASE WHEN SALES_TYPE_CD = 	4	then 	1	else 0 end)	SLSTYP_VENDF,
MAX(CASE WHEN SALES_TYPE_CD = 	5	then 	1	else 0 end)	SLSTYP_MERCHD,
MAX(CASE WHEN SALES_TYPE_CD = 	6	then 	1	else 0 end)	SLSTYP_MKTD,
MAX(CASE WHEN SALES_TYPE_CD = 	7	then 	1	else 0 end)	SLSTYP_LOYALACC,
MAX(CASE WHEN SALES_TYPE_CD = 	8	then 	1	else 0 end)	SLSTYP_LOYALRED,
MAX(CASE WHEN SALES_TYPE_CD = 	31	then 	1	else 0 end)	SLSTYP_AMZ,
COUNT(DISTINCT ACCOUNT_ID) AS RELATED_ACCTS,
COUNT(DISTINCT DISCOUNT_ID) as DISCOUNTS,   
MAX(CASE WHEN DISCOUNT_ID > 0 THEN 1 ELSE 0 end) DISC_AMT,
SUM(EXT_ITEM_COST_USD_AMT) AS ITEM_COST,         
SUM(EXT_SELLING_PRICE_USD_AMT) AS NET_SALES_AMT,     
COUNT(DISTINCT a.ITEM_ID) as ITEMS                      ,
COUNT(DISTINCT LOYALTY_ID) as RELATED_LOY_IDS                 ,   
COUNT(DISTINCT CASE WHEN MARKET_APPL_ID IS NOT NULL THEN MARKET_APPL_ID ELSE 0 END) as mkt_apps      ,        
MAX(CASE WHEN PICKUP_DELIVERY_CD = 'D' then 1 else 0 end) DELIVERY_AMT, 
MAX(CASE WHEN PICKUP_DELIVERY_CD = 'P' then 1 else 0 end) PICKUP_AMT,   
COUNT(DISTINCT PRICE_PREFIX_CD) as SKU9DIGIT_QTY          ,     
MAX(CASE WHEN REG_PROM_CLEARANCE_CD =	1	then 	1	else 0 end)	REGPRO_CLRNCE,
MAX(CASE WHEN REG_PROM_CLEARANCE_CD =	2	then 	1	else 0 end)	REGPRO_MRCH_ROP,
MAX(CASE WHEN REG_PROM_CLEARANCE_CD =	3	then 	1	else 0 end)	REGPRO_MRCH_INST,
MAX(CASE WHEN REG_PROM_CLEARANCE_CD =	4	then 	1	else 0 end)	REGPRO_MRCH_INSTOR,
MAX(CASE WHEN REG_PROM_CLEARANCE_CD =	5	then 	1	else 0 end)	REGPRO_MRCH_MAILREB,
MAX(CASE WHEN REG_PROM_CLEARANCE_CD =	6	then 	1	else 0 end)	REGPRO_MRCH_CPN,
MAX(CASE WHEN REG_PROM_CLEARANCE_CD =	7	then 	1	else 0 end)	REGPRO_MRCH_MKT,
MAX(CASE WHEN REG_PROM_CLEARANCE_CD =	8	then 	1	else 0 end)	REGPRO_MRCH_REG,
SUM(SHIPPED_QTY) SHIPPED_QTY                  , 
MAX(CASE when trans_type_cd = '00' then 1 else 0 end)    TT_UNKN,
MAX(CASE when trans_type_cd = '02' then 1 else 0 end)    TT_SNONTAX,
MAX(CASE when trans_type_cd = '03' then 1 else 0 end)    EMP_STAX,
MAX(CASE when trans_type_cd = '04' then 1 else 0 end)    DEL_STAX,
MAX(CASE when trans_type_cd = '05' then 1 else 0 end)    DEL_PMT,
MAX(CASE when trans_type_cd = '06' then 1 else 0 end)    RTN_TAX,
MAX(CASE when trans_type_cd = '11' then 1 else 0 end)    PKUP_TAX,
MAX(CASE when trans_type_cd = '14' then 1 else 0 end)    DELRET_TAX,
MAX(CASE when trans_type_cd = '16' then 1 else 0 end)    CODRET_TAX,
MAX(CASE when trans_type_cd = '19' then 1 else 0 end)    SLS_RET_NOTAX,
MAX(CASE when trans_type_cd = '20' then 1 else 0 end)    DEL_RET_NOTAX,
MAX(CASE when trans_type_cd = '22' then 1 else 0 end)    COD_RET_NOTAX,
MAX(CASE when trans_type_cd = '23' then 1 else 0 end)    EMP_RET_TAX,
COUNT(DISTINCT INVENTORY_LOCATION_ID) AS INV_LOCS        ,
  MAX
            ( CASE WHEN B.ITEM_DEPARTMENT_ID IN (1,8,23)
                THEN 1
                ELSE 0
                END
            )
            FURNITUR,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID IN 
(28,3)
                THEN 1
                ELSE 0 
                END
            ) FILEBND,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID IN
(52)
                THEN 1
                ELSE 0 
                END
            ) SPPAPER ,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in
(51,5)
AND  STANDARD_UOM_CD = 'CA'
AND A.SKU_NUM <> '348037'
                THEN 1
                ELSE 0 
                END
            ) CASEPAPER,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in
(51,5)
AND  STANDARD_UOM_CD = 'RM'
AND A.SKU_NUM <> '348037'
                THEN 1
                ELSE 0 
                END
            ) REAMPAPER,
 MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in
(51,5)
AND A.SKU_NUM =  '348037'
                THEN 1
                ELSE 0 
                END
            )REDPAPER,
MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in
(51,5)
AND A.SKU_NUM <>  '348037'
AND   STANDARD_UOM_CD NOT IN ( 'CA','RM')
                THEN 1
                ELSE 0 
                END
            )PAPEROTH,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 14 
                THEN 1
                ELSE 0 
                END) INKJETC   ,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 65
                THEN 1
                ELSE 0 
END) TONER,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 79
                THEN 1
                ELSE 0 
                END
            ) 
                MANAGEDPRINT,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in (12,16,50)
                THEN 1
                ELSE 0 
                END
            ) 
                PRINTERS,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID IN( 10,63)
                THEN 1
                ELSE 0 
                END
            ) 
                COMPUTERS,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 9
                THEN 1
                ELSE 0 
                END
            ) PCACC ,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in (25,27)
                THEN 1
                ELSE 0 
                END
            ) SEATING ,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 71
                THEN 1
                ELSE 0 
                END
            ) SOFTWARE,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in (21,74,69,62,61)
                THEN 1
                ELSE 0 
                END
            ) STORGANDNETW,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in (26,48)
                THEN 1
                ELSE 0 
                END
            ) MONOPROJO,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in (99,32,29)
                THEN 1
                ELSE 0 
                END
            ) DIGPRINTDOC,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in (44,45)
                THEN 1
                ELSE 0 
                END
            ) OFESSNTLS,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 7
                THEN 1
                ELSE 0 
                END
            ) SCHOOLSPPL,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 86
                 THEN 1
                 ELSE 0 
                 END
            ) BUSSERV,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 98
                 THEN 1
                 ELSE 0 
                 END
            ) MAILSHIP,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID in (4,54)
                 THEN 1
                 ELSE 0 
                 END
            ) DATEDG,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID IN (17,66,41,2)
                 THEN 1
                 ELSE 0 
                 END
            ) BUSMACH,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 81
                    THEN 1
                    ELSE 0 
                    END
            ) BAGSLUG,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 47
                THEN 1
                ELSE 0 
                END
            ) 
            CLEANING ,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 43
                THEN 1 
                ELSE 0 
                END
            ) BREAKROOM,
        MAX 
            (CASE WHEN B.ITEM_DEPARTMENT_ID = 19
             THEN 1 
             ELSE 0 
             END
            ) MOBILIT ,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID IN (58,24)
                 THEN 1
                ELSE 0 
                END
            ) PRESENTACC,

        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID = 91
                THEN 1 
                ELSE 0 END) POSTAGE  ,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID IN (42,49,6,85)
                THEN 1
                ELSE 0 END) LBLSHIPSTOR,
        MAX 
            (
                CASE WHEN B.ITEM_DEPARTMENT_ID IN (64,46,18)
                THEN 1 
                ELSE 0 END) 
    PORTABLECE,
       
        MAX 
            (
                CASE WHEN B.ITEM_GROUP_ID = 39
AND b.item_desc NOT like 'Loyalty%'
                THEN 1
                ELSE 0 END) BSD,
        MAX 
            (
                CASE WHEN B.ITEM_GROUP_ID = 41
                THEN 1
                ELSE 0 END) TECHSERV,
MAX
            (
                CASE WHEN  b.item_desc like 'Loyalty%'
THEN 1
                ELSE 0 END) LOYALACCRU
FROM
CUSTOMER_V.MKT_SALES_DETAIL_FACT     a
join
customer_v. ITEM_HIERARCHY_DIM    b
on a.item_id = b.item_id
where agent_id > 0 


    
 and LOB_ID not in 
    (
        '01',
        '06',
        '10',
        '61'
    )

and parent_order_id in 
(
sel parent_order_id
from CUSTOMER_V.MKT_SALES_DETAIL_FACT 
where agent_id > 0 
and sales_type_cd = 1
and order_dt >= (sel D725 from mw_dates)
and order_dt <= (sel D001 from mw_dates)

)
and agent_id in (sel agent_id from mw_ch)
group by 1
) 

with data;




CREATE VOLATILE TABLE MW_SALES_CNT AS
(


sel 
AGENT_ID,
ZEROIFNULL(SUM(CASE when DLSC_DL > 0 		and sel_recency < 91 then 1 else 0 end))	DLSC_DL_090D,
ZEROIFNULL(SUM(CASE when SLSTYP_RETRN <0 		and sel_recency < 91 then 1 else 0 end))	SLSTYP_RETRN_090D,
zeroifnull(sum(case when FURNITUR > 0 		and sel_recency < 91 then 1 else 0 end))	FURNITUR_090D,
zeroifnull(sum(case when FILEBND > 0 		and sel_recency < 91 then 1 else 0 end))	FILEBND_090D,
zeroifnull(sum(case when SPPAPER > 0 		and sel_recency < 91 then 1 else 0 end))	SPPAPER_090D,
zeroifnull(sum(case when CASEPAPER > 0 		and sel_recency < 91 then 1 else 0 end))	CASEPAPER_090D,
zeroifnull(sum(case when REAMPAPER > 0 		and sel_recency < 91 then 1 else 0 end))	REAMPAPER_090D,
zeroifnull(sum(case when REDPAPER > 0 		and sel_recency < 91 then 1 else 0 end))	REDPAPER_090D,
zeroifnull(sum(case when PAPEROTH > 0 		and sel_recency < 91 then 1 else 0 end))	PAPEROTH_090D,
zeroifnull(sum(case when INKJETC > 0 		and sel_recency < 91 then 1 else 0 end))	INKJETC_090D,
zeroifnull(sum(case when TONER > 0 		and sel_recency < 91 then 1 else 0 end))	TONER_090D,
zeroifnull(sum(case when MANAGEDPRINT > 0 		and sel_recency < 91 then 1 else 0 end))	MANAGEDPRINT_090D,
zeroifnull(sum(case when PRINTERS > 0 		and sel_recency < 91 then 1 else 0 end))	PRINTERS_090D,
zeroifnull(sum(case when COMPUTERS > 0 		and sel_recency < 91 then 1 else 0 end))	COMPUTERS_090D,
zeroifnull(sum(case when PCACC > 0 		and sel_recency < 91 then 1 else 0 end))	PCACC_090D,
zeroifnull(sum(case when SEATING > 0 		and sel_recency < 91 then 1 else 0 end))	SEATING_090D,
zeroifnull(sum(case when SOFTWARE > 0 		and sel_recency < 91 then 1 else 0 end))	SOFTWARE_090D,
zeroifnull(sum(case when STORGANDNETW > 0 		and sel_recency < 91 then 1 else 0 end))	STORGANDNETW_090D,
zeroifnull(sum(case when MONOPROJO > 0 		and sel_recency < 91 then 1 else 0 end))	MONOPROJO_090D,
zeroifnull(sum(case when DIGPRINTDOC > 0 		and sel_recency < 91 then 1 else 0 end))	DIGPRINTDOC_090D,
zeroifnull(sum(case when OFESSNTLS > 0 		and sel_recency < 91 then 1 else 0 end))	OFESSNTLS_090D,
zeroifnull(sum(case when SCHOOLSPPL > 0 		and sel_recency < 91 then 1 else 0 end))	SCHOOLSPPL_090D,
zeroifnull(sum(case when BUSSERV > 0 		and sel_recency < 91 then 1 else 0 end))	BUSSERV_090D,
zeroifnull(sum(case when MAILSHIP > 0 		and sel_recency < 91 then 1 else 0 end))	MAILSHIP_090D,
zeroifnull(sum(case when DATEDG > 0 		and sel_recency < 91 then 1 else 0 end))	DATEDG_090D,
zeroifnull(sum(case when BUSMACH > 0 		and sel_recency < 91 then 1 else 0 end))	BUSMACH_090D,
zeroifnull(sum(case when BAGSLUG > 0 		and sel_recency < 91 then 1 else 0 end))	BAGSLUG_090D,
zeroifnull(sum(case when CLEANING > 0 		and sel_recency < 91 then 1 else 0 end))	CLEANING_090D,
zeroifnull(sum(case when BREAKROOM > 0 		and sel_recency < 91 then 1 else 0 end))	BREAKROOM_090D,
zeroifnull(sum(case when MOBILIT > 0 		and sel_recency < 91 then 1 else 0 end))	MOBILIT_090D,
zeroifnull(sum(case when PRESENTACC > 0 		and sel_recency < 91 then 1 else 0 end))	PRESENTACC_090D,
zeroifnull(sum(case when POSTAGE > 0 		and sel_recency < 91 then 1 else 0 end))	POSTAGE_090D,
zeroifnull(sum(case when LBLSHIPSTOR > 0 		and sel_recency < 91 then 1 else 0 end))	LBLSHIPSTOR_090D,
zeroifnull(sum(case when PORTABLECE > 0 		and sel_recency < 91 then 1 else 0 end))	PORTABLECE_090D,
zeroifnull(sum(case when BSD > 0 		and sel_recency < 91 then 1 else 0 end))	BSD_090D,
zeroifnull(sum(case when TECHSERV > 0 		and sel_recency < 91 then 1 else 0 end))	TECHSERV_090D,
zeroifnull(sum(case when ccenter > 0 		and sel_recency < 91 then 1 else 0 end))	ccenter_090D,
zeroifnull(sum(case when retail > 0 		and sel_recency < 91 then 1 else 0 end))	retail_090D,
zeroifnull(sum(case when online > 0 		and sel_recency < 91 then 1 else 0 end))	online_090D,
zeroifnull(sum(case when anon_shop > 0 		and sel_recency < 91 then 1 else 0 end))	anon_shop_090D,
zeroifnull(sum(case when DLSC_DL > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	DLSC_DL_180D,
zeroifnull(sum(case when SLSTYP_RETRN < 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SLSTYP_RETRN_180D,
zeroifnull(sum(case when FURNITUR > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	FURNITUR_180D,
zeroifnull(sum(case when FILEBND > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	FILEBND_180D,
zeroifnull(sum(case when SPPAPER > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SPPAPER_180D,
zeroifnull(sum(case when CASEPAPER > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	CASEPAPER_180D,
zeroifnull(sum(case when REAMPAPER > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	REAMPAPER_180D,
zeroifnull(sum(case when REDPAPER > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	REDPAPER_180D,
zeroifnull(sum(case when PAPEROTH > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	PAPEROTH_180D,
zeroifnull(sum(case when INKJETC > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	INKJETC_180D,
zeroifnull(sum(case when TONER > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	TONER_180D,
zeroifnull(sum(case when MANAGEDPRINT > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	MANAGEDPRINT_180D,
zeroifnull(sum(case when PRINTERS > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	PRINTERS_180D,
zeroifnull(sum(case when COMPUTERS > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	COMPUTERS_180D,
zeroifnull(sum(case when PCACC > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	PCACC_180D,
zeroifnull(sum(case when SEATING > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SEATING_180D,
zeroifnull(sum(case when SOFTWARE > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SOFTWARE_180D,
zeroifnull(sum(case when STORGANDNETW > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	STORGANDNETW_180D,
zeroifnull(sum(case when MONOPROJO > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	MONOPROJO_180D,
zeroifnull(sum(case when DIGPRINTDOC > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	DIGPRINTDOC_180D,
zeroifnull(sum(case when OFESSNTLS > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	OFESSNTLS_180D,
zeroifnull(sum(case when SCHOOLSPPL > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SCHOOLSPPL_180D,
zeroifnull(sum(case when BUSSERV > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BUSSERV_180D,
zeroifnull(sum(case when MAILSHIP > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	MAILSHIP_180D,
zeroifnull(sum(case when DATEDG > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	DATEDG_180D,
zeroifnull(sum(case when BUSMACH > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BUSMACH_180D,
zeroifnull(sum(case when BAGSLUG > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BAGSLUG_180D,
zeroifnull(sum(case when CLEANING > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	CLEANING_180D,
zeroifnull(sum(case when BREAKROOM > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BREAKROOM_180D,
zeroifnull(sum(case when MOBILIT > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	MOBILIT_180D,
zeroifnull(sum(case when PRESENTACC > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	PRESENTACC_180D,
zeroifnull(sum(case when POSTAGE > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	POSTAGE_180D,
zeroifnull(sum(case when LBLSHIPSTOR > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	LBLSHIPSTOR_180D,
zeroifnull(sum(case when PORTABLECE > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	PORTABLECE_180D,
zeroifnull(sum(case when BSD > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BSD_180D,
zeroifnull(sum(case when TECHSERV > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	TECHSERV_180D,
zeroifnull(sum(case when ccenter > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	ccenter_180D,
zeroifnull(sum(case when retail > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	retail_180D,
zeroifnull(sum(case when online > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	online_180D,
zeroifnull(sum(case when anon_shop > 0 		and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	anon_shop_180D,
zeroifnull(sum(case when DLSC_DL > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	DLSC_DL_270D,
zeroifnull(sum(case when SLSTYP_RETRN > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SLSTYP_RETRN_270D,
zeroifnull(sum(case when FURNITUR > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	FURNITUR_270D,
zeroifnull(sum(case when FILEBND > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	FILEBND_270D,
zeroifnull(sum(case when SPPAPER > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SPPAPER_270D,
zeroifnull(sum(case when CASEPAPER > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	CASEPAPER_270D,
zeroifnull(sum(case when REAMPAPER > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	REAMPAPER_270D,
zeroifnull(sum(case when REDPAPER > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	REDPAPER_270D,
zeroifnull(sum(case when PAPEROTH > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	PAPEROTH_270D,
zeroifnull(sum(case when INKJETC > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	INKJETC_270D,
zeroifnull(sum(case when TONER > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	TONER_270D,
zeroifnull(sum(case when MANAGEDPRINT > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	MANAGEDPRINT_270D,
zeroifnull(sum(case when PRINTERS > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	PRINTERS_270D,
zeroifnull(sum(case when COMPUTERS > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	COMPUTERS_270D,
zeroifnull(sum(case when PCACC > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	PCACC_270D,
zeroifnull(sum(case when SEATING > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SEATING_270D,
zeroifnull(sum(case when SOFTWARE > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SOFTWARE_270D,
zeroifnull(sum(case when STORGANDNETW > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	STORGANDNETW_270D,
zeroifnull(sum(case when MONOPROJO > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	MONOPROJO_270D,
zeroifnull(sum(case when DIGPRINTDOC > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	DIGPRINTDOC_270D,
zeroifnull(sum(case when OFESSNTLS > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	OFESSNTLS_270D,
zeroifnull(sum(case when SCHOOLSPPL > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SCHOOLSPPL_270D,
zeroifnull(sum(case when BUSSERV > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BUSSERV_270D,
zeroifnull(sum(case when MAILSHIP > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	MAILSHIP_270D,
zeroifnull(sum(case when DATEDG > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	DATEDG_270D,
zeroifnull(sum(case when BUSMACH > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BUSMACH_270D,
zeroifnull(sum(case when BAGSLUG > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BAGSLUG_270D,
zeroifnull(sum(case when CLEANING > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	CLEANING_270D,
zeroifnull(sum(case when BREAKROOM > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BREAKROOM_270D,
zeroifnull(sum(case when MOBILIT > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	MOBILIT_270D,
zeroifnull(sum(case when PRESENTACC > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	PRESENTACC_270D,
zeroifnull(sum(case when POSTAGE > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	POSTAGE_270D,
zeroifnull(sum(case when LBLSHIPSTOR > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	LBLSHIPSTOR_270D,
zeroifnull(sum(case when PORTABLECE > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	PORTABLECE_270D,
zeroifnull(sum(case when BSD > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BSD_270D,
zeroifnull(sum(case when TECHSERV > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	TECHSERV_270D,
zeroifnull(sum(case when ccenter > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	ccenter_270D,
zeroifnull(sum(case when retail > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	retail_270D,
zeroifnull(sum(case when online > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	online_270D,
zeroifnull(sum(case when anon_shop > 0 		and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	anon_shop_270D,
zeroifnull(sum(case when DLSC_DL > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	DLSC_DL_365D,
zeroifnull(sum(case when SLSTYP_RETRN < 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SLSTYP_RETRN_365D,
zeroifnull(sum(case when FURNITUR > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	FURNITUR_365D,
zeroifnull(sum(case when FILEBND > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	FILEBND_365D,
zeroifnull(sum(case when SPPAPER > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SPPAPER_365D,
zeroifnull(sum(case when CASEPAPER > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	CASEPAPER_365D,
zeroifnull(sum(case when REAMPAPER > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	REAMPAPER_365D,
zeroifnull(sum(case when REDPAPER > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	REDPAPER_365D,
zeroifnull(sum(case when PAPEROTH > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	PAPEROTH_365D,
zeroifnull(sum(case when INKJETC > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	INKJETC_365D,
zeroifnull(sum(case when TONER > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	TONER_365D,
zeroifnull(sum(case when MANAGEDPRINT > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	MANAGEDPRINT_365D,
zeroifnull(sum(case when PRINTERS > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	PRINTERS_365D,
zeroifnull(sum(case when COMPUTERS > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	COMPUTERS_365D,
zeroifnull(sum(case when PCACC > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	PCACC_365D,
zeroifnull(sum(case when SEATING > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SEATING_365D,
zeroifnull(sum(case when SOFTWARE > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SOFTWARE_365D,
zeroifnull(sum(case when STORGANDNETW > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	STORGANDNETW_365D,
zeroifnull(sum(case when MONOPROJO > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	MONOPROJO_365D,
zeroifnull(sum(case when DIGPRINTDOC > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	DIGPRINTDOC_365D,
zeroifnull(sum(case when OFESSNTLS > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	OFESSNTLS_365D,
zeroifnull(sum(case when SCHOOLSPPL > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SCHOOLSPPL_365D,
zeroifnull(sum(case when BUSSERV > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BUSSERV_365D,
zeroifnull(sum(case when MAILSHIP > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	MAILSHIP_365D,
zeroifnull(sum(case when DATEDG > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	DATEDG_365D,
zeroifnull(sum(case when BUSMACH > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BUSMACH_365D,
zeroifnull(sum(case when BAGSLUG > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BAGSLUG_365D,
zeroifnull(sum(case when CLEANING > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	CLEANING_365D,
zeroifnull(sum(case when BREAKROOM > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BREAKROOM_365D,
zeroifnull(sum(case when MOBILIT > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	MOBILIT_365D,
zeroifnull(sum(case when PRESENTACC > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	PRESENTACC_365D,
zeroifnull(sum(case when POSTAGE > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	POSTAGE_365D,
zeroifnull(sum(case when LBLSHIPSTOR > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	LBLSHIPSTOR_365D,
zeroifnull(sum(case when PORTABLECE > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	PORTABLECE_365D,
zeroifnull(sum(case when BSD > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BSD_365D,
zeroifnull(sum(case when TECHSERV > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	TECHSERV_365D,
zeroifnull(sum(case when ccenter > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	ccenter_365D,
zeroifnull(sum(case when retail > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	retail_365D,
zeroifnull(sum(case when online > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	online_365D,
zeroifnull(sum(case when anon_shop > 0 		and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	anon_shop_365D,
zeroifnull(sum(case when DLSC_DL > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	DLSC_DL_725D,
zeroifnull(sum(case when SLSTYP_RETRN < 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	SLSTYP_RETRN_725D,
zeroifnull(sum(case when FURNITUR > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	FURNITUR_725D,
zeroifnull(sum(case when FILEBND > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	FILEBND_725D,
zeroifnull(sum(case when SPPAPER > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	SPPAPER_725D,
zeroifnull(sum(case when CASEPAPER > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	CASEPAPER_725D,
zeroifnull(sum(case when REAMPAPER > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	REAMPAPER_725D,
zeroifnull(sum(case when REDPAPER > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	REDPAPER_725D,
zeroifnull(sum(case when PAPEROTH > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	PAPEROTH_725D,
zeroifnull(sum(case when INKJETC > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	INKJETC_725D,
zeroifnull(sum(case when TONER > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	TONER_725D,
zeroifnull(sum(case when MANAGEDPRINT > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	MANAGEDPRINT_725D,
zeroifnull(sum(case when PRINTERS > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	PRINTERS_725D,
zeroifnull(sum(case when COMPUTERS > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	COMPUTERS_725D,
zeroifnull(sum(case when PCACC > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	PCACC_725D,
zeroifnull(sum(case when SEATING > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	SEATING_725D,
zeroifnull(sum(case when SOFTWARE > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	SOFTWARE_725D,
zeroifnull(sum(case when STORGANDNETW > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	STORGANDNETW_725D,
zeroifnull(sum(case when MONOPROJO > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	MONOPROJO_725D,
zeroifnull(sum(case when DIGPRINTDOC > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	DIGPRINTDOC_725D,
zeroifnull(sum(case when OFESSNTLS > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	OFESSNTLS_725D,
zeroifnull(sum(case when SCHOOLSPPL > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	SCHOOLSPPL_725D,
zeroifnull(sum(case when BUSSERV > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	BUSSERV_725D,
zeroifnull(sum(case when MAILSHIP > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	MAILSHIP_725D,
zeroifnull(sum(case when DATEDG > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	DATEDG_725D,
zeroifnull(sum(case when BUSMACH > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	BUSMACH_725D,
zeroifnull(sum(case when BAGSLUG > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	BAGSLUG_725D,
zeroifnull(sum(case when CLEANING > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	CLEANING_725D,
zeroifnull(sum(case when BREAKROOM > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	BREAKROOM_725D,
zeroifnull(sum(case when MOBILIT > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	MOBILIT_725D,
zeroifnull(sum(case when PRESENTACC > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	PRESENTACC_725D,
zeroifnull(sum(case when POSTAGE > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	POSTAGE_725D,
zeroifnull(sum(case when LBLSHIPSTOR > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	LBLSHIPSTOR_725D,
zeroifnull(sum(case when PORTABLECE > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	PORTABLECE_725D,
zeroifnull(sum(case when BSD > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	BSD_725D,
zeroifnull(sum(case when TECHSERV > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	TECHSERV_725D,
zeroifnull(sum(case when ccenter > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	ccenter_725D,
zeroifnull(sum(case when retail > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	retail_725D,
zeroifnull(sum(case when online > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	online_725D,
zeroifnull(sum(case when anon_shop > 0 		and sel_recency >  365 and sel_recency < 726  then 1 else 0 end))	anon_shop_725D,

						
zeroifnull(sum(case when	SLSTYP_LOYALACC	>	0			and sel_recency < 91 then SLSTYP_LOYALACC else 0 end))    SLSTYP_LOYALACC_090D,
zeroifnull(sum(case when	SLSTYP_LOYALACC	>	0			and sel_recency > 90 and sel_recency <181 then SLSTYP_LOYALACC else 0 end))    SLSTYP_LOYALACC_180D,
zeroifnull(sum(case when	SLSTYP_LOYALACC	>	0			and sel_recency > 180 and sel_recency <271 then SLSTYP_LOYALACC else 0 end))    SLSTYP_LOYALACC_270D,
zeroifnull(sum(case when	SLSTYP_LOYALACC	>	0			and sel_recency > 270 and sel_recency <366 then SLSTYP_LOYALACC else 0 end))    SLSTYP_LOYALACC_365D,
zeroifnull(sum(case when	SLSTYP_LOYALACC	>	0			and sel_recency > 365 and sel_recency <726 then SLSTYP_LOYALACC else 0 end))    SLSTYP_LOYALACC_725D,
						
zeroifnull(sum(case when	SLSTYP_LOYALRED	>	0			and sel_recency < 91 then SLSTYP_LOYALRED else 0 end))    SLSTYP_LOYALRED_090D,
zeroifnull(sum(case when	SLSTYP_LOYALRED	>	0			and sel_recency > 90 and sel_recency <181 then SLSTYP_LOYALRED else 0 end))    SLSTYP_LOYALRED_180D,
zeroifnull(sum(case when	SLSTYP_LOYALRED	>	0			and sel_recency > 180 and sel_recency <271 then SLSTYP_LOYALRED else 0 end))    SLSTYP_LOYALRED_270D,
zeroifnull(sum(case when	SLSTYP_LOYALRED	>	0			and sel_recency > 270 and sel_recency <366 then SLSTYP_LOYALRED else 0 end))    SLSTYP_LOYALRED_365D,
zeroifnull(sum(case when	SLSTYP_LOYALRED	>	0			and sel_recency > 365 and sel_recency <726 then SLSTYP_LOYALRED else 0 end))    SLSTYP_LOYALRED_725D,
						
zeroifnull(sum(case when	DELIVERY_AMT	>	0			and sel_recency < 91 then DELIVERY_AMT else 0 end))    DELIVERY_AMT_090D,
zeroifnull(sum(case when	DELIVERY_AMT	>	0			and sel_recency > 90 and sel_recency <181 then DELIVERY_AMT else 0 end))    DELIVERY_AMT_180D,
zeroifnull(sum(case when	DELIVERY_AMT	>	0			and sel_recency > 180 and sel_recency <271 then DELIVERY_AMT else 0 end))    DELIVERY_AMT_270D,
zeroifnull(sum(case when	DELIVERY_AMT	>	0			and sel_recency > 270 and sel_recency <366 then DELIVERY_AMT else 0 end))    DELIVERY_AMT_365D,
zeroifnull(sum(case when	DELIVERY_AMT	>	0			and sel_recency > 365 and sel_recency <726 then DELIVERY_AMT else 0 end))    DELIVERY_AMT_725D,
						
zeroifnull(sum(case when	PICKUP_AMT	>	0			and sel_recency < 91 then PICKUP_AMT else 0 end))    PICKUP_AMT_090D,
zeroifnull(sum(case when	PICKUP_AMT	>	0			and sel_recency > 90 and sel_recency <181 then PICKUP_AMT else 0 end))    PICKUP_AMT_180D,
zeroifnull(sum(case when	PICKUP_AMT	>	0			and sel_recency > 180 and sel_recency <271 then PICKUP_AMT else 0 end))    PICKUP_AMT_270D,
zeroifnull(sum(case when	PICKUP_AMT	>	0			and sel_recency > 270 and sel_recency <366 then PICKUP_AMT else 0 end))    PICKUP_AMT_365D,
zeroifnull(sum(case when	PICKUP_AMT	>	0			and sel_recency > 365 and sel_recency <726 then PICKUP_AMT else 0 end))    PICKUP_AMT_725D

from 
(


SEL
AGENT_ID, 
(select D001 from mw_dates)-ord_dt sel_recency,
sum(DLSC_DL)	as 	DLSC_DL	,
sum(SLSTYP_RETRN)	as 	SLSTYP_RETRN	,
sum(FURNITUR)	as 	FURNITUR	,
sum(FILEBND)	as 	FILEBND	,
sum(SPPAPER)	as 	SPPAPER	,
sum(CASEPAPER)	as 	CASEPAPER	,
sum(REAMPAPER)	as 	REAMPAPER	,
sum(REDPAPER)	as 	REDPAPER	,
sum(PAPEROTH)	as 	PAPEROTH	,
sum(INKJETC)	as 	INKJETC	,
sum(TONER)	as 	TONER	,
sum(MANAGEDPRINT)	as 	MANAGEDPRINT	,
sum(PRINTERS)	as 	PRINTERS	,
sum(COMPUTERS)	as 	COMPUTERS	,
sum(PCACC)	as 	PCACC	,
sum(SEATING)	as 	SEATING	,
sum(SOFTWARE)	as 	SOFTWARE	,
sum(STORGANDNETW)	as 	STORGANDNETW	,
sum(MONOPROJO)	as 	MONOPROJO	,
sum(DIGPRINTDOC)	as 	DIGPRINTDOC	,
sum(OFESSNTLS)	as 	OFESSNTLS	,
sum(SCHOOLSPPL)	as 	SCHOOLSPPL	,
sum(BUSSERV)	as 	BUSSERV	,
sum(MAILSHIP)	as 	MAILSHIP	,
sum(DATEDG)	as 	DATEDG	,
sum(BUSMACH)	as 	BUSMACH	,
sum(BAGSLUG)	as 	BAGSLUG	,
sum(CLEANING)	as 	CLEANING	,
sum(BREAKROOM)	as 	BREAKROOM	,
sum(MOBILIT)	as 	MOBILIT	,
sum(PRESENTACC)	as 	PRESENTACC	,
sum(POSTAGE)	as 	POSTAGE	,
sum(LBLSHIPSTOR)	as 	LBLSHIPSTOR	,
sum(PORTABLECE)	as 	PORTABLECE	,
sum(BSD)	as 	BSD	,
sum(TECHSERV)	as 	TECHSERV	,
sum(ccenter)	as 	ccenter	,
sum(retail)	as 	retail	,
sum(online)	as 	online	,
sum(anon_shop)	as 	anon_shop	,
SUM(SLSTYP_LOYALACC) SLSTYP_LOYALACC,
sum(SLSTYP_LOYALRED) SLSTYP_LOYALRED,
sum(SLSTYP_AMZ) SLSTYP_AMZ,
SUM(DELIVERY_AMT) DELIVERY_AMT, 
SUM(PICKUP_AMT) PICKUP_AMT  
FROM  tempdb.mw_dept_dm_CNT
GROUP BY 1,2


) derived
GROUP BY 1 )
WITH DATA
ON COMMIT PRESERVE ROWS;


create multiset volatile table 
mw_browse_not_buy
as
(
select agent_id, 
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'DIRECT LOAD' then browsed else 0 end))	CM_DL_BROWSED_090D,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'DIRECT LOAD' then bought else 0 end))	CM_DL_BOUGHT_090D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'DIRECT LOAD' then browsed else 0 end))	CM_DL_BROWSED_180D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'DIRECT LOAD' then bought else 0 end))	CM_DL_BOUGHT_180D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'DIRECT LOAD' then browsed else 0 end))	CM_DL_BROWSED_270D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'DIRECT LOAD' then bought else 0 end))	CM_DL_BOUGHT_270D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'DIRECT LOAD' then browsed else 0 end))	CM_DL_BROWSED_365D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'DIRECT LOAD' then bought else 0 end))	CM_DL_BOUGHT_365D,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'DIRECT LOAD' then browsed else 0 end))	CM_DL_BROWSED_725,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'DIRECT LOAD' then bought else 0 end))	CM_DL_BOUGHT_725,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'PAID SEARCH' then browsed else 0 end))	CM_PS_BROWSED_090D,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'PAID SEARCH' then bought else 0 end))	CM_PS_BOUGHT_090D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'PAID SEARCH' then browsed else 0 end))	CM_PS_BROWSED_180D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'PAID SEARCH' then bought else 0 end))	CM_PS_BOUGHT_180D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'PAID SEARCH' then browsed else 0 end))	CM_PS_BROWSED_270D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'PAID SEARCH' then bought else 0 end))	CM_PS_BOUGHT_270D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'PAID SEARCH' then browsed else 0 end))	CM_PS_BROWSED_365D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'PAID SEARCH' then bought else 0 end))	CM_PS_BOUGHT_365D,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'PAID SEARCH' then browsed else 0 end))	CM_PS_BROWSED_725,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'PAID SEARCH' then bought else 0 end))	CM_PS_BOUGHT_725,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'PLA' then browsed else 0 end))	CM_PLA_BROWSED_090D,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'PLA' then bought else 0 end))	CM_PLA_BOUGHT_090D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'PLA' then browsed else 0 end))	CM_PLA_BROWSED_180D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'PLA' then bought else 0 end))	CM_PLA_BOUGHT_180D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'PLA' then browsed else 0 end))	CM_PLA_BROWSED_270D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'PLA' then bought else 0 end))	CM_PLA_BOUGHT_270D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'PLA' then browsed else 0 end))	CM_PLA_BROWSED_365D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'PLA' then bought else 0 end))	CM_PLA_BOUGHT_365D,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'PLA' then browsed else 0 end))	CM_PLA_BROWSED_725,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'PLA' then bought else 0 end))	CM_PLA_BOUGHT_725,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'NATURAL SEARCH' then browsed else 0 end))	CM_NS_BROWSED_090D,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'NATURAL SEARCH' then bought else 0 end))	CM_NS_BOUGHT_090D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'NATURAL SEARCH' then browsed else 0 end))	CM_NS_BROWSED_180D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'NATURAL SEARCH' then bought else 0 end))	CM_NS_BOUGHT_180D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'NATURAL SEARCH' then browsed else 0 end))	CM_NS_BROWSED_270D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'NATURAL SEARCH' then bought else 0 end))	CM_NS_BOUGHT_270D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'NATURAL SEARCH' then browsed else 0 end))	CM_NS_BROWSED_365D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'NATURAL SEARCH' then bought else 0 end))	CM_NS_BOUGHT_365D,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'NATURAL SEARCH' then browsed else 0 end))	CM_NS_BROWSED_725,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'NATURAL SEARCH' then bought else 0 end))	CM_NS_BOUGHT_725,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'AFFILIATES' then browsed else 0 end))	CM_AF_BROWSED_090D,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'AFFILIATES' then bought else 0 end))	CM_AF_BOUGHT_090D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'AFFILIATES' then browsed else 0 end))	CM_AF_BROWSED_180D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'AFFILIATES' then bought else 0 end))	CM_AF_BOUGHT_180D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'AFFILIATES' then browsed else 0 end))	CM_AF_BROWSED_270D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'AFFILIATES' then bought else 0 end))	CM_AF_BOUGHT_270D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'AFFILIATES' then browsed else 0 end))	CM_AF_BROWSED_365D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'AFFILIATES' then bought else 0 end))	CM_AF_BOUGHT_365D,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'AFFILIATES' then browsed else 0 end))	CM_AF_BROWSED_725,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'AFFILIATES' then bought else 0 end))	CM_AF_BOUGHT_725,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'EMAIL' then browsed else 0 end))	CM_EM_BROWSED_090D,
zeroifnull(sum(case when sel_recency < 91 and trim(marketing_channel_nm) = 'EMAIL' then bought else 0 end))	CM_EM_BOUGHT_090D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'EMAIL' then browsed else 0 end))	CM_EM_BROWSED_180D,
zeroifnull(sum(case when Sel_recency > 90 and sel_recency < 181 and trim(marketing_channel_nm) = 'EMAIL' then bought else 0 end))	CM_EM_BOUGHT_180D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'EMAIL' then browsed else 0 end))	CM_EM_BROWSED_270D,
zeroifnull(sum(case when sel_recency > 180 and sel_recency < 271 and trim(marketing_channel_nm) = 'EMAIL' then bought else 0 end))	CM_EM_BOUGHT_270D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'EMAIL' then browsed else 0 end))	CM_EM_BROWSED_365D,
zeroifnull(sum(case when sel_recency > 270 and sel_recency < 366 and trim(marketing_channel_nm) = 'EMAIL' then bought else 0 end))	CM_EM_BOUGHT_365D,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'EMAIL' then browsed else 0 end))	CM_EM_BROWSED_725,
zeroifnull(sum(case when sel_recency > 365 and sel_recency < 726 and trim(marketing_channel_nm) = 'EMAIL' then bought else 0 end))	CM_EM_BOUGHT_725,
zeroifnull(sum(case when sel_recency < 91 and 	BR_FURNITUR	> 0 then 1 else 0 end))	BR_FURNITUR_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_FILEBND	> 0 then 1 else 0 end))	BR_FILEBND_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_SPPAPER	> 0 then 1 else 0 end))	BR_SPPAPER_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_CASEPAPER	> 0 then 1 else 0 end))	BR_CASEPAPER_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_REAMPAPER	> 0 then 1 else 0 end))	BR_REAMPAPER_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_REDPAPER	> 0 then 1 else 0 end))	BR_REDPAPER_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_PAPEROTH	> 0 then 1 else 0 end))	BR_PAPEROTH_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_INKJETC	> 0 then 1 else 0 end))	BR_INKJETC_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_TONER	> 0 then 1 else 0 end))	BR_TONER_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_MANAGEDPRINT	> 0 then 1 else 0 end))	BR_MANAGEDPRINT_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_PRINTERS	> 0 then 1 else 0 end))	BR_PRINTERS_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_COMPUTERS	> 0 then 1 else 0 end))	BR_COMPUTERS_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_PCACC	> 0 then 1 else 0 end))	BR_PCACC_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_SEATING	> 0 then 1 else 0 end))	BR_SEATING_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_SOFTWARE	> 0 then 1 else 0 end))	BR_SOFTWARE_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_STORGANDNETW	> 0 then 1 else 0 end))	BR_STORGANDNETW_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_MONOPROJO	> 0 then 1 else 0 end))	BR_MONOPROJO_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_DIGPRINTDOC	> 0 then 1 else 0 end))	BR_DIGPRINTDOC_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_OFESSNTLS	> 0 then 1 else 0 end))	BR_OFESSNTLS_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_SCHOOLSPPL	> 0 then 1 else 0 end))	BR_SCHOOLSPPL_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_BUSSERV	> 0 then 1 else 0 end))	BR_BUSSERV_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_MAILSHIP	> 0 then 1 else 0 end))	BR_MAILSHIP_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_DATEDG	> 0 then 1 else 0 end))	BR_DATEDG_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_BUSMACH	> 0 then 1 else 0 end))	BR_BUSMACH_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_BAGSLUG	> 0 then 1 else 0 end))	BR_BAGSLUG_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_CLEANING	> 0 then 1 else 0 end))	BR_CLEANING_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_BREAKROOM	> 0 then 1 else 0 end))	BR_BREAKROOM_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_MOBILIT	> 0 then 1 else 0 end))	BR_MOBILIT_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_PRESENTACC	> 0 then 1 else 0 end))	BR_PRESENTACC_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_POSTAGE	> 0 then 1 else 0 end))	BR_POSTAGE_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_LBLSHIPSTOR	> 0 then 1 else 0 end))	BR_LBLSHIPSTOR_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_PORTABLECE	> 0 then 1 else 0 end))	BR_PORTABLECE_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_BSD	> 0 then 1 else 0 end))	BR_BSD_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_TECHSERV	> 0 then 1 else 0 end))	BR_TECHSERV_090D,
zeroifnull(sum(case when sel_recency < 91 and 	BR_LOYALACCRU	> 0 then 1 else 0 end))	BR_LOYALACCRU_090D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_FURNITUR	> 0 then 1 else 0 end))	BR_FURNITUR_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_FILEBND	> 0 then 1 else 0 end))	BR_FILEBND_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_SPPAPER	> 0 then 1 else 0 end))	BR_SPPAPER_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_CASEPAPER	> 0 then 1 else 0 end))	BR_CASEPAPER_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_REAMPAPER	> 0 then 1 else 0 end))	BR_REAMPAPER_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_REDPAPER	> 0 then 1 else 0 end))	BR_REDPAPER_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_PAPEROTH	> 0 then 1 else 0 end))	BR_PAPEROTH_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_INKJETC	> 0 then 1 else 0 end))	BR_INKJETC_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_TONER	> 0 then 1 else 0 end))	BR_TONER_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_MANAGEDPRINT	> 0 then 1 else 0 end))	BR_MANAGEDPRINT_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_PRINTERS	> 0 then 1 else 0 end))	BR_PRINTERS_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_COMPUTERS	> 0 then 1 else 0 end))	BR_COMPUTERS_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_PCACC	> 0 then 1 else 0 end))	BR_PCACC_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_SEATING	> 0 then 1 else 0 end))	BR_SEATING_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_SOFTWARE	> 0 then 1 else 0 end))	BR_SOFTWARE_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_STORGANDNETW	> 0 then 1 else 0 end))	BR_STORGANDNETW_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_MONOPROJO	> 0 then 1 else 0 end))	BR_MONOPROJO_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_DIGPRINTDOC	> 0 then 1 else 0 end))	BR_DIGPRINTDOC_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_OFESSNTLS	> 0 then 1 else 0 end))	BR_OFESSNTLS_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_SCHOOLSPPL	> 0 then 1 else 0 end))	BR_SCHOOLSPPL_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_BUSSERV	> 0 then 1 else 0 end))	BR_BUSSERV_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_MAILSHIP	> 0 then 1 else 0 end))	BR_MAILSHIP_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_DATEDG	> 0 then 1 else 0 end))	BR_DATEDG_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_BUSMACH	> 0 then 1 else 0 end))	BR_BUSMACH_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_BAGSLUG	> 0 then 1 else 0 end))	BR_BAGSLUG_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_CLEANING	> 0 then 1 else 0 end))	BR_CLEANING_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_BREAKROOM	> 0 then 1 else 0 end))	BR_BREAKROOM_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_MOBILIT	> 0 then 1 else 0 end))	BR_MOBILIT_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_PRESENTACC	> 0 then 1 else 0 end))	BR_PRESENTACC_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_POSTAGE	> 0 then 1 else 0 end))	BR_POSTAGE_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_LBLSHIPSTOR	> 0 then 1 else 0 end))	BR_LBLSHIPSTOR_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_PORTABLECE	> 0 then 1 else 0 end))	BR_PORTABLECE_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_BSD	> 0 then 1 else 0 end))	BR_BSD_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_TECHSERV	> 0 then 1 else 0 end))	BR_TECHSERV_270D,
zeroifnull(sum(case when  sel_recency > 180 and sel_recency < 271 and 	BR_LOYALACCRU	> 0 then 1 else 0 end))	BR_LOYALACCRU_270D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_FURNITUR	> 0 then 1 else 0 end))	BR_FURNITUR_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_FILEBND	> 0 then 1 else 0 end))	BR_FILEBND_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_SPPAPER	> 0 then 1 else 0 end))	BR_SPPAPER_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_CASEPAPER	> 0 then 1 else 0 end))	BR_CASEPAPER_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_REAMPAPER	> 0 then 1 else 0 end))	BR_REAMPAPER_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_REDPAPER	> 0 then 1 else 0 end))	BR_REDPAPER_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_PAPEROTH	> 0 then 1 else 0 end))	BR_PAPEROTH_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_INKJETC	> 0 then 1 else 0 end))	BR_INKJETC_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_TONER	> 0 then 1 else 0 end))	BR_TONER_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_MANAGEDPRINT	> 0 then 1 else 0 end))	BR_MANAGEDPRINT_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_PRINTERS	> 0 then 1 else 0 end))	BR_PRINTERS_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_COMPUTERS	> 0 then 1 else 0 end))	BR_COMPUTERS_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_PCACC	> 0 then 1 else 0 end))	BR_PCACC_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_SEATING	> 0 then 1 else 0 end))	BR_SEATING_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_SOFTWARE	> 0 then 1 else 0 end))	BR_SOFTWARE_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_STORGANDNETW	> 0 then 1 else 0 end))	BR_STORGANDNETW_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_MONOPROJO	> 0 then 1 else 0 end))	BR_MONOPROJO_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_DIGPRINTDOC	> 0 then 1 else 0 end))	BR_DIGPRINTDOC_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_OFESSNTLS	> 0 then 1 else 0 end))	BR_OFESSNTLS_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_SCHOOLSPPL	> 0 then 1 else 0 end))	BR_SCHOOLSPPL_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_BUSSERV	> 0 then 1 else 0 end))	BR_BUSSERV_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_MAILSHIP	> 0 then 1 else 0 end))	BR_MAILSHIP_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_DATEDG	> 0 then 1 else 0 end))	BR_DATEDG_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_BUSMACH	> 0 then 1 else 0 end))	BR_BUSMACH_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_BAGSLUG	> 0 then 1 else 0 end))	BR_BAGSLUG_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_CLEANING	> 0 then 1 else 0 end))	BR_CLEANING_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_BREAKROOM	> 0 then 1 else 0 end))	BR_BREAKROOM_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_MOBILIT	> 0 then 1 else 0 end))	BR_MOBILIT_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_PRESENTACC	> 0 then 1 else 0 end))	BR_PRESENTACC_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_POSTAGE	> 0 then 1 else 0 end))	BR_POSTAGE_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_LBLSHIPSTOR	> 0 then 1 else 0 end))	BR_LBLSHIPSTOR_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_PORTABLECE	> 0 then 1 else 0 end))	BR_PORTABLECE_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_BSD	> 0 then 1 else 0 end))	BR_BSD_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_TECHSERV	> 0 then 1 else 0 end))	BR_TECHSERV_180D,
zeroifnull(sum(case when  sel_recency > 90 and sel_recency < 181 and 	BR_LOYALACCRU	> 0 then 1 else 0 end))	BR_LOYALACCRU_180D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_FURNITUR	> 0 then 1 else 0 end))	BR_FURNITUR_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_FILEBND	> 0 then 1 else 0 end))	BR_FILEBND_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_SPPAPER	> 0 then 1 else 0 end))	BR_SPPAPER_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_CASEPAPER	> 0 then 1 else 0 end))	BR_CASEPAPER_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_REAMPAPER	> 0 then 1 else 0 end))	BR_REAMPAPER_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_REDPAPER	> 0 then 1 else 0 end))	BR_REDPAPER_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_PAPEROTH	> 0 then 1 else 0 end))	BR_PAPEROTH_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_INKJETC	> 0 then 1 else 0 end))	BR_INKJETC_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_TONER	> 0 then 1 else 0 end))	BR_TONER_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_MANAGEDPRINT	> 0 then 1 else 0 end))	BR_MANAGEDPRINT_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_PRINTERS	> 0 then 1 else 0 end))	BR_PRINTERS_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_COMPUTERS	> 0 then 1 else 0 end))	BR_COMPUTERS_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_PCACC	> 0 then 1 else 0 end))	BR_PCACC_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_SEATING	> 0 then 1 else 0 end))	BR_SEATING_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_SOFTWARE	> 0 then 1 else 0 end))	BR_SOFTWARE_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_STORGANDNETW	> 0 then 1 else 0 end))	BR_STORGANDNETW_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_MONOPROJO	> 0 then 1 else 0 end))	BR_MONOPROJO_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_DIGPRINTDOC	> 0 then 1 else 0 end))	BR_DIGPRINTDOC_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_OFESSNTLS	> 0 then 1 else 0 end))	BR_OFESSNTLS_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_SCHOOLSPPL	> 0 then 1 else 0 end))	BR_SCHOOLSPPL_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_BUSSERV	> 0 then 1 else 0 end))	BR_BUSSERV_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_MAILSHIP	> 0 then 1 else 0 end))	BR_MAILSHIP_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_DATEDG	> 0 then 1 else 0 end))	BR_DATEDG_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_BUSMACH	> 0 then 1 else 0 end))	BR_BUSMACH_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_BAGSLUG	> 0 then 1 else 0 end))	BR_BAGSLUG_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_CLEANING	> 0 then 1 else 0 end))	BR_CLEANING_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_BREAKROOM	> 0 then 1 else 0 end))	BR_BREAKROOM_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_MOBILIT	> 0 then 1 else 0 end))	BR_MOBILIT_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_PRESENTACC	> 0 then 1 else 0 end))	BR_PRESENTACC_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_POSTAGE	> 0 then 1 else 0 end))	BR_POSTAGE_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_LBLSHIPSTOR	> 0 then 1 else 0 end))	BR_LBLSHIPSTOR_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_PORTABLECE	> 0 then 1 else 0 end))	BR_PORTABLECE_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_BSD	> 0 then 1 else 0 end))	BR_BSD_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_TECHSERV	> 0 then 1 else 0 end))	BR_TECHSERV_365D,
zeroifnull(sum(case when  sel_recency > 270 and sel_recency < 366 and 	BR_LOYALACCRU	> 0 then 1 else 0 end))	BR_LOYALACCRU_365D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_FURNITUR	> 0 then 1 else 0 end))	BR_FURNITUR_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_FILEBND	> 0 then 1 else 0 end))	BR_FILEBND_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_SPPAPER	> 0 then 1 else 0 end))	BR_SPPAPER_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_CASEPAPER	> 0 then 1 else 0 end))	BR_CASEPAPER_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_REAMPAPER	> 0 then 1 else 0 end))	BR_REAMPAPER_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_REDPAPER	> 0 then 1 else 0 end))	BR_REDPAPER_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_PAPEROTH	> 0 then 1 else 0 end))	BR_PAPEROTH_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_INKJETC	> 0 then 1 else 0 end))	BR_INKJETC_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_TONER	> 0 then 1 else 0 end))	BR_TONER_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_MANAGEDPRINT	> 0 then 1 else 0 end))	BR_MANAGEDPRINT_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_PRINTERS	> 0 then 1 else 0 end))	BR_PRINTERS_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_COMPUTERS	> 0 then 1 else 0 end))	BR_COMPUTERS_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_PCACC	> 0 then 1 else 0 end))	BR_PCACC_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_SEATING	> 0 then 1 else 0 end))	BR_SEATING_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_SOFTWARE	> 0 then 1 else 0 end))	BR_SOFTWARE_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_STORGANDNETW	> 0 then 1 else 0 end))	BR_STORGANDNETW_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_MONOPROJO	> 0 then 1 else 0 end))	BR_MONOPROJO_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_DIGPRINTDOC	> 0 then 1 else 0 end))	BR_DIGPRINTDOC_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_OFESSNTLS	> 0 then 1 else 0 end))	BR_OFESSNTLS_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_SCHOOLSPPL	> 0 then 1 else 0 end))	BR_SCHOOLSPPL_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_BUSSERV	> 0 then 1 else 0 end))	BR_BUSSERV_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_MAILSHIP	> 0 then 1 else 0 end))	BR_MAILSHIP_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_DATEDG	> 0 then 1 else 0 end))	BR_DATEDG_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_BUSMACH	> 0 then 1 else 0 end))	BR_BUSMACH_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_BAGSLUG	> 0 then 1 else 0 end))	BR_BAGSLUG_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_CLEANING	> 0 then 1 else 0 end))	BR_CLEANING_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_BREAKROOM	> 0 then 1 else 0 end))	BR_BREAKROOM_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_MOBILIT	> 0 then 1 else 0 end))	BR_MOBILIT_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_PRESENTACC	> 0 then 1 else 0 end))	BR_PRESENTACC_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_POSTAGE	> 0 then 1 else 0 end))	BR_POSTAGE_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_LBLSHIPSTOR	> 0 then 1 else 0 end))	BR_LBLSHIPSTOR_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_PORTABLECE	> 0 then 1 else 0 end))	BR_PORTABLECE_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_BSD	> 0 then 1 else 0 end))	BR_BSD_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_TECHSERV	> 0 then 1 else 0 end))	BR_TECHSERV_725D,
zeroifnull(sum(case when  sel_recency > 365 and sel_recency < 726 and 	BR_LOYALACCRU	> 0 then 1 else 0 end))	BR_LOYALACCRU_725D


from

(

sel agent_id, 
marketing_channel_nm,
(select D001 from mw_dates)-click_date sel_recency,
max(case when session_id is not null then 1 else 0 end) as browsed,
max(case when buysess = 1 then 1 else 0 end) as bought,
ZEROIFNULL(max(	BR_FURNITUR	) )	as	BR_FURNITUR	,
ZEROIFNULL(max(	BR_FILEBND	) )	as	BR_FILEBND	,
ZEROIFNULL(max(	BR_SPPAPER	) )	as	BR_SPPAPER	,
ZEROIFNULL(max(	BR_CASEPAPER	) )	as	BR_CASEPAPER	,
ZEROIFNULL(max(	BR_REAMPAPER	) )	as	BR_REAMPAPER	,
ZEROIFNULL(max(	BR_REDPAPER	) )	as	BR_REDPAPER	,
ZEROIFNULL(max(	BR_PAPEROTH	) )	as	BR_PAPEROTH	,
ZEROIFNULL(max(	BR_INKJETC	) )	as	BR_INKJETC	,
ZEROIFNULL(max(	BR_TONER	) )	as	BR_TONER	,
ZEROIFNULL(max(	BR_MANAGEDPRINT	) )	as	BR_MANAGEDPRINT	,
ZEROIFNULL(max(	BR_PRINTERS	) )	as	BR_PRINTERS	,
ZEROIFNULL(max(	BR_COMPUTERS	) )	as	BR_COMPUTERS	,
ZEROIFNULL(max(	BR_PCACC	) )	as	BR_PCACC	,
ZEROIFNULL(max(	BR_SEATING	) )	as	BR_SEATING	,
ZEROIFNULL(max(	BR_SOFTWARE	) )	as	BR_SOFTWARE	,
ZEROIFNULL(max(	BR_STORGANDNETW	) )	as	BR_STORGANDNETW	,
ZEROIFNULL(max(	BR_MONOPROJO	) )	as	BR_MONOPROJO	,
ZEROIFNULL(max(	BR_DIGPRINTDOC	) )	as	BR_DIGPRINTDOC	,
ZEROIFNULL(max(	BR_OFESSNTLS	) )	as	BR_OFESSNTLS	,
ZEROIFNULL(max(	BR_SCHOOLSPPL	) )	as	BR_SCHOOLSPPL	,
ZEROIFNULL(max(	BR_BUSSERV	) )	as	BR_BUSSERV	,
ZEROIFNULL(max(	BR_MAILSHIP	) )	as	BR_MAILSHIP	,
ZEROIFNULL(max(	BR_DATEDG	) )	as	BR_DATEDG	,
ZEROIFNULL(max(	BR_BUSMACH	) )	as	BR_BUSMACH	,
ZEROIFNULL(max(	BR_BAGSLUG	) )	as	BR_BAGSLUG	,
ZEROIFNULL(max(	BR_CLEANING	) )	as	BR_CLEANING	,
ZEROIFNULL(max(	BR_BREAKROOM	) )	as	BR_BREAKROOM	,
ZEROIFNULL(max(	BR_MOBILIT	) )	as	BR_MOBILIT	,
ZEROIFNULL(max(	BR_PRESENTACC	) )	as	BR_PRESENTACC	,
ZEROIFNULL(max(	BR_POSTAGE	) )	as	BR_POSTAGE	,
ZEROIFNULL(max(	BR_LBLSHIPSTOR	) )	as	BR_LBLSHIPSTOR	,
ZEROIFNULL(max(	BR_PORTABLECE	) )	as	BR_PORTABLECE	,
ZEROIFNULL(max(	BR_BSD	) )	as	BR_BSD	,
ZEROIFNULL(max(	BR_TECHSERV	) )	as	BR_TECHSERV	,
ZEROIFNULL(max(	BR_LOYALACCRU	) )	as	BR_LOYALACCRU	
from 
 tempdb.mw_odc_sessions 
 group by 1,2,3
 ) der
 GROUP BY 1
 )WITH DATA
 ON COMMIT PRESERVE ROWS;


create multiset volatile table mw_contact_hist as
(sel agent_id, 
zeroifnull(sum(case when camp_type = 	'CD'	and sel_recency < 91 then 1 else 0 end))	CD_090D,
zeroifnull(sum(case when camp_type = 	'CA'	and sel_recency < 91 then 1 else 0 end))	CA_090D,
zeroifnull(sum(case when camp_type = 	'SS'	and sel_recency < 91 then 1 else 0 end))	SS_090D,
zeroifnull(sum(case when camp_type = 	'PD'	and sel_recency < 91 then 1 else 0 end))	PD_090D,
zeroifnull(sum(case when camp_type = 	'SP'	and sel_recency < 91 then 1 else 0 end))	SP_090D,
zeroifnull(sum(case when camp_type = 	'SD'	and sel_recency < 91 then 1 else 0 end))	SD_090D,
zeroifnull(sum(case when camp_type = 	'RW'	and sel_recency < 91 then 1 else 0 end))	RW_090D,
zeroifnull(sum(case when camp_type = 	'PC'	and sel_recency < 91 then 1 else 0 end))	PC_090D,
zeroifnull(sum(case when camp_type = 	'DM'	and sel_recency < 91 then 1 else 0 end))	DM_090D,
zeroifnull(sum(case when camp_type = 	'CP'	and sel_recency < 91 then 1 else 0 end))	CP_090D,
zeroifnull(sum(case when camp_type = 	'PS'	and sel_recency < 91 then 1 else 0 end))	PS_090D,
zeroifnull(sum(case when camp_type = 	'FS'	and sel_recency < 91 then 1 else 0 end))	FS_090D,
zeroifnull(sum(case when camp_type = 	'FU'	and sel_recency < 91 then 1 else 0 end))	FU_090D,
zeroifnull(sum(case when camp_type = 	'BD'	and sel_recency < 91 then 1 else 0 end))	BD_090D,
zeroifnull(sum(case when camp_type = 	'PT'	and sel_recency < 91 then 1 else 0 end))	PT_090D,
zeroifnull(sum(case when camp_type = 	'PB'	and sel_recency < 91 then 1 else 0 end))	PB_090D,
zeroifnull(sum(case when camp_type = 	'DG'	and sel_recency < 91 then 1 else 0 end))	DG_090D,
zeroifnull(sum(case when camp_type = 	'GD'	and sel_recency < 91 then 1 else 0 end))	GD_090D,
zeroifnull(sum(case when camp_type = 	'CU'	and sel_recency < 91 then 1 else 0 end))	CU_090D,
zeroifnull(sum(case when camp_type = 	'HP'	and sel_recency < 91 then 1 else 0 end))	HP_090D,
zeroifnull(sum(case when camp_type = 	'ET'	and sel_recency < 91 then 1 else 0 end))	ET_090D,
zeroifnull(sum(case when camp_type = 	'BM'	and sel_recency < 91 then 1 else 0 end))	BM_090D,
zeroifnull(sum(case when camp_type = 	'BB'	and sel_recency < 91 then 1 else 0 end))	BB_090D,
zeroifnull(sum(case when camp_type = 	'TC'	and sel_recency < 91 then 1 else 0 end))	TC_090D,
zeroifnull(sum(case when camp_type = 	'TZ'	and sel_recency < 91 then 1 else 0 end))	TZ_090D,
zeroifnull(sum(case when camp_type = 	'BF'	and sel_recency < 91 then 1 else 0 end))	BF_090D,
zeroifnull(sum(case when camp_type = 	'BS'	and sel_recency < 91 then 1 else 0 end))	BS_090D,
zeroifnull(sum(case when camp_type = 	'OD'	and sel_recency < 91 then 1 else 0 end))	OD_090D,
zeroifnull(sum(case when camp_type = 	'GO'	and sel_recency < 91 then 1 else 0 end))	GO_090D,
zeroifnull(sum(case when camp_type = 	'BI'	and sel_recency < 91 then 1 else 0 end))	BI_090D,
zeroifnull(sum(case when camp_type = 	'SB'	and sel_recency < 91 then 1 else 0 end))	SB_090D,
zeroifnull(sum(case when camp_type = 	'IT'	and sel_recency < 91 then 1 else 0 end))	IT_090D,
zeroifnull(sum(case when camp_type = 	'SZ'	and sel_recency < 91 then 1 else 0 end))	SZ_090D,
zeroifnull(sum(case when camp_type = 	'SO'	and sel_recency < 91 then 1 else 0 end))	SO_090D,
zeroifnull(sum(case when camp_type = 	'TD'	and sel_recency < 91 then 1 else 0 end))	TD_090D,
zeroifnull(sum(case when camp_type = 	'CD'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	CD_180D,
zeroifnull(sum(case when camp_type = 	'CA'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	CA_180D,
zeroifnull(sum(case when camp_type = 	'SS'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SS_180D,
zeroifnull(sum(case when camp_type = 	'PD'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	PD_180D,
zeroifnull(sum(case when camp_type = 	'SP'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SP_180D,
zeroifnull(sum(case when camp_type = 	'SD'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SD_180D,
zeroifnull(sum(case when camp_type = 	'RW'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	RW_180D,
zeroifnull(sum(case when camp_type = 	'PC'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	PC_180D,
zeroifnull(sum(case when camp_type = 	'DM'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	DM_180D,
zeroifnull(sum(case when camp_type = 	'CP'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	CP_180D,
zeroifnull(sum(case when camp_type = 	'PS'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	PS_180D,
zeroifnull(sum(case when camp_type = 	'FS'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	FS_180D,
zeroifnull(sum(case when camp_type = 	'FU'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	FU_180D,
zeroifnull(sum(case when camp_type = 	'BD'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BD_180D,
zeroifnull(sum(case when camp_type = 	'PT'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	PT_180D,
zeroifnull(sum(case when camp_type = 	'PB'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	PB_180D,
zeroifnull(sum(case when camp_type = 	'DG'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	DG_180D,
zeroifnull(sum(case when camp_type = 	'GD'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	GD_180D,
zeroifnull(sum(case when camp_type = 	'CU'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	CU_180D,
zeroifnull(sum(case when camp_type = 	'HP'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	HP_180D,
zeroifnull(sum(case when camp_type = 	'ET'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	ET_180D,
zeroifnull(sum(case when camp_type = 	'BM'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BM_180D,
zeroifnull(sum(case when camp_type = 	'BB'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BB_180D,
zeroifnull(sum(case when camp_type = 	'TC'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	TC_180D,
zeroifnull(sum(case when camp_type = 	'TZ'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	TZ_180D,
zeroifnull(sum(case when camp_type = 	'BF'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BF_180D,
zeroifnull(sum(case when camp_type = 	'BS'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BS_180D,
zeroifnull(sum(case when camp_type = 	'OD'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	OD_180D,
zeroifnull(sum(case when camp_type = 	'GO'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	GO_180D,
zeroifnull(sum(case when camp_type = 	'BI'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	BI_180D,
zeroifnull(sum(case when camp_type = 	'SB'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SB_180D,
zeroifnull(sum(case when camp_type = 	'IT'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	IT_180D,
zeroifnull(sum(case when camp_type = 	'SZ'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SZ_180D,
zeroifnull(sum(case when camp_type = 	'SO'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	SO_180D,
zeroifnull(sum(case when camp_type = 	'TD'	and sel_recency >  90 and sel_recency < 181  then 1 else 0 end))	TD_180D,
zeroifnull(sum(case when camp_type = 	'CD'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	CD_270D,
zeroifnull(sum(case when camp_type = 	'CA'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	CA_270D,
zeroifnull(sum(case when camp_type = 	'SS'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SS_270D,
zeroifnull(sum(case when camp_type = 	'PD'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	PD_270D,
zeroifnull(sum(case when camp_type = 	'SP'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SP_270D,
zeroifnull(sum(case when camp_type = 	'SD'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SD_270D,
zeroifnull(sum(case when camp_type = 	'RW'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	RW_270D,
zeroifnull(sum(case when camp_type = 	'PC'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	PC_270D,
zeroifnull(sum(case when camp_type = 	'DM'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	DM_270D,
zeroifnull(sum(case when camp_type = 	'CP'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	CP_270D,
zeroifnull(sum(case when camp_type = 	'PS'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	PS_270D,
zeroifnull(sum(case when camp_type = 	'FS'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	FS_270D,
zeroifnull(sum(case when camp_type = 	'FU'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	FU_270D,
zeroifnull(sum(case when camp_type = 	'BD'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BD_270D,
zeroifnull(sum(case when camp_type = 	'PT'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	PT_270D,
zeroifnull(sum(case when camp_type = 	'PB'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	PB_270D,
zeroifnull(sum(case when camp_type = 	'DG'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	DG_270D,
zeroifnull(sum(case when camp_type = 	'GD'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	GD_270D,
zeroifnull(sum(case when camp_type = 	'CU'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	CU_270D,
zeroifnull(sum(case when camp_type = 	'HP'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	HP_270D,
zeroifnull(sum(case when camp_type = 	'ET'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	ET_270D,
zeroifnull(sum(case when camp_type = 	'BM'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BM_270D,
zeroifnull(sum(case when camp_type = 	'BB'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BB_270D,
zeroifnull(sum(case when camp_type = 	'TC'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	TC_270D,
zeroifnull(sum(case when camp_type = 	'TZ'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	TZ_270D,
zeroifnull(sum(case when camp_type = 	'BF'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BF_270D,
zeroifnull(sum(case when camp_type = 	'BS'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BS_270D,
zeroifnull(sum(case when camp_type = 	'OD'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	OD_270D,
zeroifnull(sum(case when camp_type = 	'GO'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	GO_270D,
zeroifnull(sum(case when camp_type = 	'BI'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	BI_270D,
zeroifnull(sum(case when camp_type = 	'SB'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SB_270D,
zeroifnull(sum(case when camp_type = 	'IT'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	IT_270D,
zeroifnull(sum(case when camp_type = 	'SZ'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SZ_270D,
zeroifnull(sum(case when camp_type = 	'SO'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	SO_270D,
zeroifnull(sum(case when camp_type = 	'TD'	and sel_recency >  180 and sel_recency < 271  then 1 else 0 end))	TD_270D,
zeroifnull(sum(case when camp_type = 	'CD'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	CD_365D,
zeroifnull(sum(case when camp_type = 	'CA'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	CA_365D,
zeroifnull(sum(case when camp_type = 	'SS'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SS_365D,
zeroifnull(sum(case when camp_type = 	'PD'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	PD_365D,
zeroifnull(sum(case when camp_type = 	'SP'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SP_365D,
zeroifnull(sum(case when camp_type = 	'SD'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SD_365D,
zeroifnull(sum(case when camp_type = 	'RW'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	RW_365D,
zeroifnull(sum(case when camp_type = 	'PC'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	PC_365D,
zeroifnull(sum(case when camp_type = 	'DM'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	DM_365D,
zeroifnull(sum(case when camp_type = 	'CP'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	CP_365D,
zeroifnull(sum(case when camp_type = 	'PS'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	PS_365D,
zeroifnull(sum(case when camp_type = 	'FS'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	FS_365D,
zeroifnull(sum(case when camp_type = 	'FU'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	FU_365D,
zeroifnull(sum(case when camp_type = 	'BD'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BD_365D,
zeroifnull(sum(case when camp_type = 	'PT'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	PT_365D,
zeroifnull(sum(case when camp_type = 	'PB'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	PB_365D,
zeroifnull(sum(case when camp_type = 	'DG'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	DG_365D,
zeroifnull(sum(case when camp_type = 	'GD'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	GD_365D,
zeroifnull(sum(case when camp_type = 	'CU'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	CU_365D,
zeroifnull(sum(case when camp_type = 	'HP'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	HP_365D,
zeroifnull(sum(case when camp_type = 	'ET'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	ET_365D,
zeroifnull(sum(case when camp_type = 	'BM'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BM_365D,
zeroifnull(sum(case when camp_type = 	'BB'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BB_365D,
zeroifnull(sum(case when camp_type = 	'TC'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	TC_365D,
zeroifnull(sum(case when camp_type = 	'TZ'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	TZ_365D,
zeroifnull(sum(case when camp_type = 	'BF'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BF_365D,
zeroifnull(sum(case when camp_type = 	'BS'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BS_365D,
zeroifnull(sum(case when camp_type = 	'OD'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	OD_365D,
zeroifnull(sum(case when camp_type = 	'GO'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	GO_365D,
zeroifnull(sum(case when camp_type = 	'BI'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	BI_365D,
zeroifnull(sum(case when camp_type = 	'SB'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SB_365D,
zeroifnull(sum(case when camp_type = 	'IT'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	IT_365D,
zeroifnull(sum(case when camp_type = 	'SZ'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SZ_365D,
zeroifnull(sum(case when camp_type = 	'SO'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	SO_365D,
zeroifnull(sum(case when camp_type = 	'TD'	and sel_recency >  270 and sel_recency < 366  then 1 else 0 end))	TD_365D,
zeroifnull(sum(case when camp_type = 	'CD'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	CD_725D,
zeroifnull(sum(case when camp_type = 	'CA'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	CA_725D,
zeroifnull(sum(case when camp_type = 	'SS'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	SS_725D,
zeroifnull(sum(case when camp_type = 	'PD'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	PD_725D,
zeroifnull(sum(case when camp_type = 	'SP'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	SP_725D,
zeroifnull(sum(case when camp_type = 	'SD'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	SD_725D,
zeroifnull(sum(case when camp_type = 	'RW'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	RW_725D,
zeroifnull(sum(case when camp_type = 	'PC'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	PC_725D,
zeroifnull(sum(case when camp_type = 	'DM'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	DM_725D,
zeroifnull(sum(case when camp_type = 	'CP'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	CP_725D,
zeroifnull(sum(case when camp_type = 	'PS'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	PS_725D,
zeroifnull(sum(case when camp_type = 	'FS'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	FS_725D,
zeroifnull(sum(case when camp_type = 	'FU'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	FU_725D,
zeroifnull(sum(case when camp_type = 	'BD'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	BD_725D,
zeroifnull(sum(case when camp_type = 	'PT'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	PT_725D,
zeroifnull(sum(case when camp_type = 	'PB'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	PB_725D,
zeroifnull(sum(case when camp_type = 	'DG'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	DG_725D,
zeroifnull(sum(case when camp_type = 	'GD'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	GD_725D,
zeroifnull(sum(case when camp_type = 	'CU'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	CU_725D,
zeroifnull(sum(case when camp_type = 	'HP'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	HP_725D,
zeroifnull(sum(case when camp_type = 	'ET'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	ET_725D,
zeroifnull(sum(case when camp_type = 	'BM'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	BM_725D,
zeroifnull(sum(case when camp_type = 	'BB'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	BB_725D,
zeroifnull(sum(case when camp_type = 	'TC'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	TC_725D,
zeroifnull(sum(case when camp_type = 	'TZ'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	TZ_725D,
zeroifnull(sum(case when camp_type = 	'BF'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	BF_725D,
zeroifnull(sum(case when camp_type = 	'BS'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	BS_725D,
zeroifnull(sum(case when camp_type = 	'OD'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	OD_725D,
zeroifnull(sum(case when camp_type = 	'GO'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	GO_725D,
zeroifnull(sum(case when camp_type = 	'BI'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	BI_725D,
zeroifnull(sum(case when camp_type = 	'SB'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	SB_725D,
zeroifnull(sum(case when camp_type = 	'IT'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	IT_725D,
zeroifnull(sum(case when camp_type = 	'SZ'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	SZ_725D,
zeroifnull(sum(case when camp_type = 	'SO'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	SO_725D,
zeroifnull(sum(case when camp_type = 	'TD'	and sel_recency >  365 and sel_recency < 726  then 1 else 0 end)/4)	TD_725D

from 
(
select agent_id, 
substr(trim(campaign_id),1,2) as camp_type, 
(select D001 from mw_dates)-pull_dt sel_recency
from   tempdb.mw_campaign_selection 
) DER
GROUP BY 1
) 
with data
on commit preserve rows;

drop table tempdb.mw_train_5000_COUNTS;

create table tempdb.mw_TRAIN_5000_COUNTS
 as 
 (
 select a.agent_id, 
 a.ctype, 
 a.lcycle,
 A.CPREF,
 a.segment,
 zeroifnull(CD_090D	)	CD_090D	,
zeroifnull(CA_090D	)	CA_090D	,
zeroifnull(SS_090D	)	SS_090D	,
zeroifnull(PD_090D	)	PD_090D	,
zeroifnull(SP_090D	)	SP_090D	,
zeroifnull(SD_090D	)	SD_090D	,
zeroifnull(RW_090D	)	RW_090D	,
zeroifnull(PC_090D	)	PC_090D	,
zeroifnull(DM_090D	)	DM_090D	,
zeroifnull(CP_090D	)	CP_090D	,
zeroifnull(PS_090D	)	PS_090D	,
zeroifnull(FS_090D	)	FS_090D	,
zeroifnull(FU_090D	)	FU_090D	,
zeroifnull(BD_090D	)	BD_090D	,
zeroifnull(PT_090D	)	PT_090D	,
zeroifnull(PB_090D	)	PB_090D	,
zeroifnull(DG_090D	)	DG_090D	,
zeroifnull(GD_090D	)	GD_090D	,
zeroifnull(CU_090D	)	CU_090D	,
zeroifnull(HP_090D	)	HP_090D	,
zeroifnull(ET_090D	)	ET_090D	,
zeroifnull(BM_090D	)	BM_090D	,
zeroifnull(BB_090D	)	BB_090D	,
zeroifnull(TC_090D	)	TC_090D	,
zeroifnull(TZ_090D	)	TZ_090D	,
zeroifnull(BF_090D	)	BF_090D	,
zeroifnull(BS_090D	)	BS_090D	,
zeroifnull(OD_090D	)	OD_090D	,
zeroifnull(GO_090D	)	GO_090D	,
zeroifnull(BI_090D	)	BI_090D	,
zeroifnull(SB_090D	)	SB_090D	,
zeroifnull(IT_090D	)	IT_090D	,
zeroifnull(SZ_090D	)	SZ_090D	,
zeroifnull(SO_090D	)	SO_090D	,
zeroifnull(TD_090D	)	TD_090D	,
zeroifnull(CD_180D	)	CD_180D	,
zeroifnull(CA_180D	)	CA_180D	,
zeroifnull(SS_180D	)	SS_180D	,
zeroifnull(PD_180D	)	PD_180D	,
zeroifnull(SP_180D	)	SP_180D	,
zeroifnull(SD_180D	)	SD_180D	,
zeroifnull(RW_180D	)	RW_180D	,
zeroifnull(PC_180D	)	PC_180D	,
zeroifnull(DM_180D	)	DM_180D	,
zeroifnull(CP_180D	)	CP_180D	,
zeroifnull(PS_180D	)	PS_180D	,
zeroifnull(FS_180D	)	FS_180D	,
zeroifnull(FU_180D	)	FU_180D	,
zeroifnull(BD_180D	)	BD_180D	,
zeroifnull(PT_180D	)	PT_180D	,
zeroifnull(PB_180D	)	PB_180D	,
zeroifnull(DG_180D	)	DG_180D	,
zeroifnull(GD_180D	)	GD_180D	,
zeroifnull(CU_180D	)	CU_180D	,
zeroifnull(HP_180D	)	HP_180D	,
zeroifnull(ET_180D	)	ET_180D	,
zeroifnull(BM_180D	)	BM_180D	,
zeroifnull(BB_180D	)	BB_180D	,
zeroifnull(TC_180D	)	TC_180D	,
zeroifnull(TZ_180D	)	TZ_180D	,
zeroifnull(BF_180D	)	BF_180D	,
zeroifnull(BS_180D	)	BS_180D	,
zeroifnull(OD_180D	)	OD_180D	,
zeroifnull(GO_180D	)	GO_180D	,
zeroifnull(BI_180D	)	BI_180D	,
zeroifnull(SB_180D	)	SB_180D	,
zeroifnull(IT_180D	)	IT_180D	,
zeroifnull(SZ_180D	)	SZ_180D	,
zeroifnull(SO_180D	)	SO_180D	,
zeroifnull(TD_180D	)	TD_180D	,
zeroifnull(CD_270D	)	CD_270D	,
zeroifnull(CA_270D	)	CA_270D	,
zeroifnull(SS_270D	)	SS_270D	,
zeroifnull(PD_270D	)	PD_270D	,
zeroifnull(SP_270D	)	SP_270D	,
zeroifnull(SD_270D	)	SD_270D	,
zeroifnull(RW_270D	)	RW_270D	,
zeroifnull(PC_270D	)	PC_270D	,
zeroifnull(DM_270D	)	DM_270D	,
zeroifnull(CP_270D	)	CP_270D	,
zeroifnull(PS_270D	)	PS_270D	,
zeroifnull(FS_270D	)	FS_270D	,
zeroifnull(FU_270D	)	FU_270D	,
zeroifnull(BD_270D	)	BD_270D	,
zeroifnull(PT_270D	)	PT_270D	,
zeroifnull(PB_270D	)	PB_270D	,
zeroifnull(DG_270D	)	DG_270D	,
zeroifnull(GD_270D	)	GD_270D	,
zeroifnull(CU_270D	)	CU_270D	,
zeroifnull(HP_270D	)	HP_270D	,
zeroifnull(ET_270D	)	ET_270D	,
zeroifnull(BM_270D	)	BM_270D	,
zeroifnull(BB_270D	)	BB_270D	,
zeroifnull(TC_270D	)	TC_270D	,
zeroifnull(TZ_270D	)	TZ_270D	,
zeroifnull(BF_270D	)	BF_270D	,
zeroifnull(BS_270D	)	BS_270D	,
zeroifnull(OD_270D	)	OD_270D	,
zeroifnull(GO_270D	)	GO_270D	,
zeroifnull(BI_270D	)	BI_270D	,
zeroifnull(SB_270D	)	SB_270D	,
zeroifnull(IT_270D	)	IT_270D	,
zeroifnull(SZ_270D	)	SZ_270D	,
zeroifnull(SO_270D	)	SO_270D	,
zeroifnull(TD_270D	)	TD_270D	,
zeroifnull(CD_365D	)	CD_365D	,
zeroifnull(CA_365D	)	CA_365D	,
zeroifnull(SS_365D	)	SS_365D	,
zeroifnull(PD_365D	)	PD_365D	,
zeroifnull(SP_365D	)	SP_365D	,
zeroifnull(SD_365D	)	SD_365D	,
zeroifnull(RW_365D	)	RW_365D	,
zeroifnull(PC_365D	)	PC_365D	,
zeroifnull(DM_365D	)	DM_365D	,
zeroifnull(CP_365D	)	CP_365D	,
zeroifnull(PS_365D	)	PS_365D	,
zeroifnull(FS_365D	)	FS_365D	,
zeroifnull(FU_365D	)	FU_365D	,
zeroifnull(BD_365D	)	BD_365D	,
zeroifnull(PT_365D	)	PT_365D	,
zeroifnull(PB_365D	)	PB_365D	,
zeroifnull(DG_365D	)	DG_365D	,
zeroifnull(GD_365D	)	GD_365D	,
zeroifnull(CU_365D	)	CU_365D	,
zeroifnull(HP_365D	)	HP_365D	,
zeroifnull(ET_365D	)	ET_365D	,
zeroifnull(BM_365D	)	BM_365D	,
zeroifnull(BB_365D	)	BB_365D	,
zeroifnull(TC_365D	)	TC_365D	,
zeroifnull(TZ_365D	)	TZ_365D	,
zeroifnull(BF_365D	)	BF_365D	,
zeroifnull(BS_365D	)	BS_365D	,
zeroifnull(OD_365D	)	OD_365D	,
zeroifnull(GO_365D	)	GO_365D	,
zeroifnull(BI_365D	)	BI_365D	,
zeroifnull(SB_365D	)	SB_365D	,
zeroifnull(IT_365D	)	IT_365D	,
zeroifnull(SZ_365D	)	SZ_365D	,
zeroifnull(SO_365D	)	SO_365D	,
zeroifnull(TD_365D	)	TD_365D	,
zeroifnull(CD_725D	)	CD_725D	,
zeroifnull(CA_725D	)	CA_725D	,
zeroifnull(SS_725D	)	SS_725D	,
zeroifnull(PD_725D	)	PD_725D	,
zeroifnull(SP_725D	)	SP_725D	,
zeroifnull(SD_725D	)	SD_725D	,
zeroifnull(RW_725D	)	RW_725D	,
zeroifnull(PC_725D	)	PC_725D	,
zeroifnull(DM_725D	)	DM_725D	,
zeroifnull(CP_725D	)	CP_725D	,
zeroifnull(PS_725D	)	PS_725D	,
zeroifnull(FS_725D	)	FS_725D	,
zeroifnull(FU_725D	)	FU_725D	,
zeroifnull(BD_725D	)	BD_725D	,
zeroifnull(PT_725D	)	PT_725D	,
zeroifnull(PB_725D	)	PB_725D	,
zeroifnull(DG_725D	)	DG_725D	,
zeroifnull(GD_725D	)	GD_725D	,
zeroifnull(CU_725D	)	CU_725D	,
zeroifnull(HP_725D	)	HP_725D	,
zeroifnull(ET_725D	)	ET_725D	,
zeroifnull(BM_725D	)	BM_725D	,
zeroifnull(BB_725D	)	BB_725D	,
zeroifnull(TC_725D	)	TC_725D	,
zeroifnull(TZ_725D	)	TZ_725D	,
zeroifnull(BF_725D	)	BF_725D	,
zeroifnull(BS_725D	)	BS_725D	,
zeroifnull(OD_725D	)	OD_725D	,
zeroifnull(GO_725D	)	GO_725D	,
zeroifnull(BI_725D	)	BI_725D	,
zeroifnull(SB_725D	)	SB_725D	,
zeroifnull(IT_725D	)	IT_725D	,
zeroifnull(SZ_725D	)	SZ_725D	,
zeroifnull(SO_725D	)	SO_725D	,
zeroifnull(TD_725D	)	TD_725D	,
zeroifnull(AFFILIATES_090D	)	AFFILIATES_090D	,
zeroifnull(AFFILIATES_180D	)	AFFILIATES_180D	,
zeroifnull(AFFILIATES_270D	)	AFFILIATES_270D	,
zeroifnull(AFFILIATES_365D	)	AFFILIATES_365D	,
zeroifnull(AFFILIATES_725D	)	AFFILIATES_725D	,
zeroifnull(BIG_EVENT_090D	)	BIG_EVENT_090D	,
zeroifnull(BIG_EVENT_180D	)	BIG_EVENT_180D	,
zeroifnull(BIG_EVENT_270D	)	BIG_EVENT_270D	,
zeroifnull(BIG_EVENT_365D	)	BIG_EVENT_365D	,
zeroifnull(BIG_EVENT_725D	)	BIG_EVENT_725D	,
zeroifnull(CATALOG_090D	)	CATALOG_090D	,
zeroifnull(CATALOG_180D	)	CATALOG_180D	,
zeroifnull(CATALOG_270D	)	CATALOG_270D	,
zeroifnull(CATALOG_365D	)	CATALOG_365D	,
zeroifnull(CATALOG_725D	)	CATALOG_725D	,
zeroifnull(CONTRACT_090D	)	CONTRACT_090D	,
zeroifnull(CONTRACT_180D	)	CONTRACT_180D	,
zeroifnull(CONTRACT_270D	)	CONTRACT_270D	,
zeroifnull(CONTRACT_365D	)	CONTRACT_365D	,
zeroifnull(CONTRACT_725D	)	CONTRACT_725D	,
zeroifnull(CONTRACT_DM_090D	)	CONTRACT_DM_090D	,
zeroifnull(CONTRACT_DM_180D	)	CONTRACT_DM_180D	,
zeroifnull(CONTRACT_DM_270D	)	CONTRACT_DM_270D	,
zeroifnull(CONTRACT_DM_365D	)	CONTRACT_DM_365D	,
zeroifnull(CONTRACT_DM_725D	)	CONTRACT_DM_725D	,
zeroifnull(CONTRACT_EMAIL_090D	)	CONTRACT_EMAIL_090D	,
zeroifnull(CONTRACT_EMAIL_180D	)	CONTRACT_EMAIL_180D	,
zeroifnull(CONTRACT_EMAIL_270D	)	CONTRACT_EMAIL_270D	,
zeroifnull(CONTRACT_EMAIL_365D	)	CONTRACT_EMAIL_365D	,
zeroifnull(CONTRACT_EMAIL_725D	)	CONTRACT_EMAIL_725D	,
zeroifnull(CONTRACT_ONLINE_090D	)	CONTRACT_ONLINE_090D	,
zeroifnull(CONTRACT_ONLINE_180D	)	CONTRACT_ONLINE_180D	,
zeroifnull(CONTRACT_ONLINE_270D	)	CONTRACT_ONLINE_270D	,
zeroifnull(CONTRACT_ONLINE_365D	)	CONTRACT_ONLINE_365D	,
zeroifnull(CONTRACT_ONLINE_725D	)	CONTRACT_ONLINE_725D	,
zeroifnull(CONTRACT_OTHER_090D	)	CONTRACT_OTHER_090D	,
zeroifnull(CONTRACT_OTHER_180D	)	CONTRACT_OTHER_180D	,
zeroifnull(CONTRACT_OTHER_270D	)	CONTRACT_OTHER_270D	,
zeroifnull(CONTRACT_OTHER_365D	)	CONTRACT_OTHER_365D	,
zeroifnull(CONTRACT_OTHER_725D	)	CONTRACT_OTHER_725D	,
zeroifnull(CPD_090D	)	CPD_090D	,
zeroifnull(CPD_180D	)	CPD_180D	,
zeroifnull(CPD_270D	)	CPD_270D	,
zeroifnull(CPD_365D	)	CPD_365D	,
zeroifnull(CPD_725D	)	CPD_725D	,
zeroifnull(CREDIT_OFFER_090D	)	CREDIT_OFFER_090D	,
zeroifnull(CREDIT_OFFER_180D	)	CREDIT_OFFER_180D	,
zeroifnull(CREDIT_OFFER_270D	)	CREDIT_OFFER_270D	,
zeroifnull(CREDIT_OFFER_365D	)	CREDIT_OFFER_365D	,
zeroifnull(CREDIT_OFFER_725D	)	CREDIT_OFFER_725D	,
zeroifnull(CUSTOMER_SAT_090D	)	CUSTOMER_SAT_090D	,
zeroifnull(CUSTOMER_SAT_180D	)	CUSTOMER_SAT_180D	,
zeroifnull(CUSTOMER_SAT_270D	)	CUSTOMER_SAT_270D	,
zeroifnull(CUSTOMER_SAT_365D	)	CUSTOMER_SAT_365D	,
zeroifnull(CUSTOMER_SAT_725D	)	CUSTOMER_SAT_725D	,
zeroifnull(DIRECT_MAIL_090D	)	DIRECT_MAIL_090D	,
zeroifnull(DIRECT_MAIL_180D	)	DIRECT_MAIL_180D	,
zeroifnull(DIRECT_MAIL_270D	)	DIRECT_MAIL_270D	,
zeroifnull(DIRECT_MAIL_365D	)	DIRECT_MAIL_365D	,
zeroifnull(DIRECT_MAIL_725D	)	DIRECT_MAIL_725D	,
zeroifnull(EMAIL_090D	)	EMAIL_090D	,
zeroifnull(EMAIL_180D	)	EMAIL_180D	,
zeroifnull(EMAIL_270D	)	EMAIL_270D	,
zeroifnull(EMAIL_365D	)	EMAIL_365D	,
zeroifnull(EMAIL_725D	)	EMAIL_725D	,
zeroifnull(EMAILTRIGGERS_090D	)	EMAILTRIGGERS_090D	,
zeroifnull(EMAILTRIGGERS_180D	)	EMAILTRIGGERS_180D	,
zeroifnull(EMAILTRIGGERS_270D	)	EMAILTRIGGERS_270D	,
zeroifnull(EMAILTRIGGERS_365D	)	EMAILTRIGGERS_365D	,
zeroifnull(EMAILTRIGGERS_725D	)	EMAILTRIGGERS_725D	,
zeroifnull(EMAIL_ONBORD_090D	)	EMAIL_ONBORD_090D	,
zeroifnull(EMAIL_ONBORD_180D	)	EMAIL_ONBORD_180D	,
zeroifnull(EMAIL_ONBORD_270D	)	EMAIL_ONBORD_270D	,
zeroifnull(EMAIL_ONBORD_365D	)	EMAIL_ONBORD_365D	,
zeroifnull(EMAIL_ONBORD_725D	)	EMAIL_ONBORD_725D	,
zeroifnull(FIXBUNDLE_090D	)	FIXBUNDLE_090D	,
zeroifnull(FIXBUNDLE_180D	)	FIXBUNDLE_180D	,
zeroifnull(FIXBUNDLE_270D	)	FIXBUNDLE_270D	,
zeroifnull(FIXBUNDLE_365D	)	FIXBUNDLE_365D	,
zeroifnull(FIXBUNDLE_725D	)	FIXBUNDLE_725D	,
zeroifnull(INSERT_090D	)	INSERT_090D	,
zeroifnull(INSERT_180D	)	INSERT_180D	,
zeroifnull(INSERT_270D	)	INSERT_270D	,
zeroifnull(INSERT_365D	)	INSERT_365D	,
zeroifnull(INSERT_725D	)	INSERT_725D	,
zeroifnull(INSTORE_090D	)	INSTORE_090D	,
zeroifnull(INSTORE_180D	)	INSTORE_180D	,
zeroifnull(INSTORE_270D	)	INSTORE_270D	,
zeroifnull(INSTORE_365D	)	INSTORE_365D	,
zeroifnull(INSTORE_725D	)	INSTORE_725D	,
zeroifnull(LOCAL_MARKETING_090D	)	LOCAL_MARKETING_090D	,
zeroifnull(LOCAL_MARKETING_180D	)	LOCAL_MARKETING_180D	,
zeroifnull(LOCAL_MARKETING_270D	)	LOCAL_MARKETING_270D	,
zeroifnull(LOCAL_MARKETING_365D	)	LOCAL_MARKETING_365D	,
zeroifnull(LOCAL_MARKETING_725D	)	LOCAL_MARKETING_725D	,
zeroifnull(LOYALTY_090D	)	LOYALTY_090D	,
zeroifnull(LOYALTY_180D	)	LOYALTY_180D	,
zeroifnull(LOYALTY_270D	)	LOYALTY_270D	,
zeroifnull(LOYALTY_365D	)	LOYALTY_365D	,
zeroifnull(LOYALTY_725D	)	LOYALTY_725D	,
zeroifnull(LOYALTY_GHOST_090D	)	LOYALTY_GHOST_090D	,
zeroifnull(LOYALTY_GHOST_180D	)	LOYALTY_GHOST_180D	,
zeroifnull(LOYALTY_GHOST_270D	)	LOYALTY_GHOST_270D	,
zeroifnull(LOYALTY_GHOST_365D	)	LOYALTY_GHOST_365D	,
zeroifnull(LOYALTY_GHOST_725D	)	LOYALTY_GHOST_725D	,
zeroifnull(MOBILE_090D	)	MOBILE_090D	,
zeroifnull(MOBILE_180D	)	MOBILE_180D	,
zeroifnull(MOBILE_270D	)	MOBILE_270D	,
zeroifnull(MOBILE_365D	)	MOBILE_365D	,
zeroifnull(MOBILE_725D	)	MOBILE_725D	,
zeroifnull(c.ONLINE_090D	)	ONLINE_090D	,
zeroifnull(c.ONLINE_180D	)	ONLINE_180D	,
zeroifnull(c.ONLINE_270D	)	ONLINE_270D	,
zeroifnull(c.ONLINE_365D	)	ONLINE_365D	,
zeroifnull(c.ONLINE_725D	)	ONLINE_725D	,
zeroifnull(ONLINE_A_090D	)	ONLINE_A_090D	,
zeroifnull(ONLINE_A_180D	)	ONLINE_A_180D	,
zeroifnull(ONLINE_A_270D	)	ONLINE_A_270D	,
zeroifnull(ONLINE_A_365D	)	ONLINE_A_365D	,
zeroifnull(ONLINE_A_725D	)	ONLINE_A_725D	,
zeroifnull(ONLINE_B_090D	)	ONLINE_B_090D	,
zeroifnull(ONLINE_B_180D	)	ONLINE_B_180D	,
zeroifnull(ONLINE_B_270D	)	ONLINE_B_270D	,
zeroifnull(ONLINE_B_365D	)	ONLINE_B_365D	,
zeroifnull(ONLINE_B_725D	)	ONLINE_B_725D	,
zeroifnull(ONLINE_WAP_090D	)	ONLINE_WAP_090D	,
zeroifnull(ONLINE_WAP_180D	)	ONLINE_WAP_180D	,
zeroifnull(ONLINE_WAP_270D	)	ONLINE_WAP_270D	,
zeroifnull(ONLINE_WAP_365D	)	ONLINE_WAP_365D	,
zeroifnull(ONLINE_WAP_725D	)	ONLINE_WAP_725D	,
zeroifnull(OTHER_090D	)	OTHER_090D	,
zeroifnull(OTHER_180D	)	OTHER_180D	,
zeroifnull(OTHER_270D	)	OTHER_270D	,
zeroifnull(OTHER_365D	)	OTHER_365D	,
zeroifnull(OTHER_725D	)	OTHER_725D	,
zeroifnull(OVERRIDES_090D	)	OVERRIDES_090D	,
zeroifnull(OVERRIDES_180D	)	OVERRIDES_180D	,
zeroifnull(OVERRIDES_270D	)	OVERRIDES_270D	,
zeroifnull(OVERRIDES_365D	)	OVERRIDES_365D	,
zeroifnull(OVERRIDES_725D	)	OVERRIDES_725D	,
zeroifnull(PERSONALIZED_POS_BB_090D	)	PERSONALIZED_POS_BB_090D	,
zeroifnull(PERSONALIZED_POS_BB_180D	)	PERSONALIZED_POS_BB_180D	,
zeroifnull(PERSONALIZED_POS_BB_270D	)	PERSONALIZED_POS_BB_270D	,
zeroifnull(PERSONALIZED_POS_BB_365D	)	PERSONALIZED_POS_BB_365D	,
zeroifnull(PERSONALIZED_POS_BB_725D	)	PERSONALIZED_POS_BB_725D	,
zeroifnull(PLAYBOOK_090D	)	PLAYBOOK_090D	,
zeroifnull(PLAYBOOK_180D	)	PLAYBOOK_180D	,
zeroifnull(PLAYBOOK_270D	)	PLAYBOOK_270D	,
zeroifnull(PLAYBOOK_365D	)	PLAYBOOK_365D	,
zeroifnull(PLAYBOOK_725D	)	PLAYBOOK_725D	,
zeroifnull(PRODUCT_PROTECTION_090D	)	PRODUCT_PROTECTION_090D	,
zeroifnull(PRODUCT_PROTECTION_180D	)	PRODUCT_PROTECTION_180D	,
zeroifnull(PRODUCT_PROTECTION_270D	)	PRODUCT_PROTECTION_270D	,
zeroifnull(PRODUCT_PROTECTION_365D	)	PRODUCT_PROTECTION_365D	,
zeroifnull(PRODUCT_PROTECTION_725D	)	PRODUCT_PROTECTION_725D	,
zeroifnull(RADIO_090D	)	RADIO_090D	,
zeroifnull(RADIO_180D	)	RADIO_180D	,
zeroifnull(RADIO_270D	)	RADIO_270D	,
zeroifnull(RADIO_365D	)	RADIO_365D	,
zeroifnull(RADIO_725D	)	RADIO_725D	,
zeroifnull(TRUEBUNDLE_090D	)	TRUEBUNDLE_090D	,
zeroifnull(TRUEBUNDLE_180D	)	TRUEBUNDLE_180D	,
zeroifnull(TRUEBUNDLE_270D	)	TRUEBUNDLE_270D	,
zeroifnull(TRUEBUNDLE_365D	)	TRUEBUNDLE_365D	,
zeroifnull(TRUEBUNDLE_725D	)	TRUEBUNDLE_725D	,
zeroifnull(UNKN_090D	)	UNKN_090D	,
zeroifnull(UNKN_180D	)	UNKN_180D	,
zeroifnull(UNKN_270D	)	UNKN_270D	,
zeroifnull(UNKN_365D	)	UNKN_365D	,
zeroifnull(UNKN_725D	)	UNKN_725D	,
zeroifnull(WEB_WELCOME_EMAIL_090D	)	WEB_WELCOME_EMAIL_090D	,
zeroifnull(WEB_WELCOME_EMAIL_180D	)	WEB_WELCOME_EMAIL_180D	,
zeroifnull(WEB_WELCOME_EMAIL_270D	)	WEB_WELCOME_EMAIL_270D	,
zeroifnull(WEB_WELCOME_EMAIL_365D	)	WEB_WELCOME_EMAIL_365D	,
zeroifnull(WEB_WELCOME_EMAIL_725D	)	WEB_WELCOME_EMAIL_725D	,
zeroifnull(WEB1_090D	)	WEB1_090D	,
zeroifnull(WEB1_180D	)	WEB1_180D	,
zeroifnull(WEB1_270D	)	WEB1_270D	,
zeroifnull(WEB1_365D	)	WEB1_365D	,
zeroifnull(WEB1_725D	)	WEB1_725D	,
zeroifnull(WEB2_090D	)	WEB2_090D	,
zeroifnull(WEB2_180D	)	WEB2_180D	,
zeroifnull(WEB2_270D	)	WEB2_270D	,
zeroifnull(WEB2_365D	)	WEB2_365D	,
zeroifnull(WEB2_725D	)	WEB2_725D	,
zeroifnull(D.LPM_090D   )    LPD_090D            ,   
zeroifnull(D.LPM_180D    )                  LPD_180D,
zeroifnull(D.LPM_270D     )                 LPD_270D,
zeroifnull(D.LPM_365D      )                LPD_365D,
zeroifnull(D.LPM_725D       )               LPD_725D,
zeroifnull(LM.LPM_090D   )    LPM_090D            ,   
zeroifnull(LM.LPM_180D    )                  LPM_180D,
zeroifnull(LM.LPM_270D     )                 LPM_270D,
zeroifnull(LM.LPM_365D      )                LPM_365D,
zeroifnull(LM.LPM_725D       )               LPM_725D,
ZEROIFNULL((CM_DL_BROWSED_090D)) CM_DL_BROWSED_090D,
ZEROIFNULL((CM_DL_BOUGHT_090D)) CM_DL_BOUGHT_090D,
ZEROIFNULL((CM_DL_BROWSED_180D)) CM_DL_BROWSED_180D,
ZEROIFNULL((CM_DL_BOUGHT_180D)) CM_DL_BOUGHT_180D,
ZEROIFNULL((CM_DL_BROWSED_270D)) CM_DL_BROWSED_270D,
ZEROIFNULL((CM_DL_BOUGHT_270D)) CM_DL_BOUGHT_270D,
ZEROIFNULL((CM_DL_BROWSED_365D)) CM_DL_BROWSED_365D,
ZEROIFNULL((CM_DL_BOUGHT_365D)) CM_DL_BOUGHT_365D,
ZEROIFNULL((CM_DL_BROWSED_725)) CM_DL_BROWSED_725,
ZEROIFNULL((CM_DL_BOUGHT_725)) CM_DL_BOUGHT_725,
ZEROIFNULL((CM_PS_BROWSED_090D)) CM_PS_BROWSED_090D,
ZEROIFNULL((CM_PS_BOUGHT_090D)) CM_PS_BOUGHT_090D,
ZEROIFNULL((CM_PS_BROWSED_180D)) CM_PS_BROWSED_180D,
ZEROIFNULL((CM_PS_BOUGHT_180D)) CM_PS_BOUGHT_180D,
ZEROIFNULL((CM_PS_BROWSED_270D)) CM_PS_BROWSED_270D,
ZEROIFNULL((CM_PS_BOUGHT_270D)) CM_PS_BOUGHT_270D,
ZEROIFNULL((CM_PS_BROWSED_365D)) CM_PS_BROWSED_365D,
ZEROIFNULL((CM_PS_BOUGHT_365D)) CM_PS_BOUGHT_365D,
ZEROIFNULL((CM_PS_BROWSED_725)) CM_PS_BROWSED_725,
ZEROIFNULL((CM_PS_BOUGHT_725)) CM_PS_BOUGHT_725,
ZEROIFNULL((CM_PLA_BROWSED_090D)) CM_PLA_BROWSED_090D,
ZEROIFNULL((CM_PLA_BOUGHT_090D)) CM_PLA_BOUGHT_090D,
ZEROIFNULL((CM_PLA_BROWSED_180D)) CM_PLA_BROWSED_180D,
ZEROIFNULL((CM_PLA_BOUGHT_180D)) CM_PLA_BOUGHT_180D,
ZEROIFNULL((CM_PLA_BROWSED_270D)) CM_PLA_BROWSED_270D,
ZEROIFNULL((CM_PLA_BOUGHT_270D)) CM_PLA_BOUGHT_270D,
ZEROIFNULL((CM_PLA_BROWSED_365D)) CM_PLA_BROWSED_365D,
ZEROIFNULL((CM_PLA_BOUGHT_365D)) CM_PLA_BOUGHT_365D,
ZEROIFNULL((CM_PLA_BROWSED_725)) CM_PLA_BROWSED_725,
ZEROIFNULL((CM_PLA_BOUGHT_725)) CM_PLA_BOUGHT_725,
ZEROIFNULL((CM_NS_BROWSED_090D)) CM_NS_BROWSED_090D,
ZEROIFNULL((CM_NS_BOUGHT_090D)) CM_NS_BOUGHT_090D,
ZEROIFNULL((CM_NS_BROWSED_180D)) CM_NS_BROWSED_180D,
ZEROIFNULL((CM_NS_BOUGHT_180D)) CM_NS_BOUGHT_180D,
ZEROIFNULL((CM_NS_BROWSED_270D)) CM_NS_BROWSED_270D,
ZEROIFNULL((CM_NS_BOUGHT_270D)) CM_NS_BOUGHT_270D,
ZEROIFNULL((CM_NS_BROWSED_365D)) CM_NS_BROWSED_365D,
ZEROIFNULL((CM_NS_BOUGHT_365D)) CM_NS_BOUGHT_365D,
ZEROIFNULL((CM_NS_BROWSED_725)) CM_NS_BROWSED_725,
ZEROIFNULL((CM_NS_BOUGHT_725)) CM_NS_BOUGHT_725,
ZEROIFNULL((CM_AF_BROWSED_090D)) CM_AF_BROWSED_090D,
ZEROIFNULL((CM_AF_BOUGHT_090D)) CM_AF_BOUGHT_090D,
ZEROIFNULL((CM_AF_BROWSED_180D)) CM_AF_BROWSED_180D,
ZEROIFNULL((CM_AF_BOUGHT_180D)) CM_AF_BOUGHT_180D,
ZEROIFNULL((CM_AF_BROWSED_270D)) CM_AF_BROWSED_270D,
ZEROIFNULL((CM_AF_BOUGHT_270D)) CM_AF_BOUGHT_270D,
ZEROIFNULL((CM_AF_BROWSED_365D)) CM_AF_BROWSED_365D,
ZEROIFNULL((CM_AF_BOUGHT_365D)) CM_AF_BOUGHT_365D,
ZEROIFNULL((CM_AF_BROWSED_725)) CM_AF_BROWSED_725,
ZEROIFNULL((CM_AF_BOUGHT_725)) CM_AF_BOUGHT_725,
ZEROIFNULL((CM_EM_BROWSED_090D)) CM_EM_BROWSED_090D,
ZEROIFNULL((CM_EM_BOUGHT_090D)) CM_EM_BOUGHT_090D,
ZEROIFNULL((CM_EM_BROWSED_180D)) CM_EM_BROWSED_180D,
ZEROIFNULL((CM_EM_BOUGHT_180D)) CM_EM_BOUGHT_180D,
ZEROIFNULL((CM_EM_BROWSED_270D)) CM_EM_BROWSED_270D,
ZEROIFNULL((CM_EM_BOUGHT_270D)) CM_EM_BOUGHT_270D,
ZEROIFNULL((CM_EM_BROWSED_365D)) CM_EM_BROWSED_365D,
ZEROIFNULL((CM_EM_BOUGHT_365D)) CM_EM_BOUGHT_365D,
ZEROIFNULL((CM_EM_BROWSED_725)) CM_EM_BROWSED_725,
ZEROIFNULL((CM_EM_BOUGHT_725)) CM_EM_BOUGHT_725,
ZEROIFNULL(	DLSC_DL_090D	)	DLSC_DL_090D	,
ZEROIFNULL(	SLSTYP_RETRN_090D	)	SLSTYP_RETRN_090D	,
ZEROIFNULL(	FURNITUR_090D	)	FURNITUR_090D	,
ZEROIFNULL(	FILEBND_090D	)	FILEBND_090D	,
ZEROIFNULL(	SPPAPER_090D	)	SPPAPER_090D	,
ZEROIFNULL(	CASEPAPER_090D	)	CASEPAPER_090D	,
ZEROIFNULL(	REAMPAPER_090D	)	REAMPAPER_090D	,
ZEROIFNULL(	REDPAPER_090D	)	REDPAPER_090D	,
ZEROIFNULL(	PAPEROTH_090D	)	PAPEROTH_090D	,
ZEROIFNULL(	INKJETC_090D	)	INKJETC_090D	,
ZEROIFNULL(	TONER_090D	)	TONER_090D	,
ZEROIFNULL(	MANAGEDPRINT_090D	)	MANAGEDPRINT_090D	,
ZEROIFNULL(	PRINTERS_090D	)	PRINTERS_090D	,
ZEROIFNULL(	COMPUTERS_090D	)	COMPUTERS_090D	,
ZEROIFNULL(	PCACC_090D	)	PCACC_090D	,
ZEROIFNULL(	SEATING_090D	)	SEATING_090D	,
ZEROIFNULL(	SOFTWARE_090D	)	SOFTWARE_090D	,
ZEROIFNULL(	STORGANDNETW_090D	)	STORGANDNETW_090D	,
ZEROIFNULL(	MONOPROJO_090D	)	MONOPROJO_090D	,
ZEROIFNULL(	DIGPRINTDOC_090D	)	DIGPRINTDOC_090D	,
ZEROIFNULL(	OFESSNTLS_090D	)	OFESSNTLS_090D	,
ZEROIFNULL(	SCHOOLSPPL_090D	)	SCHOOLSPPL_090D	,
ZEROIFNULL(	BUSSERV_090D	)	BUSSERV_090D	,
ZEROIFNULL(	MAILSHIP_090D	)	MAILSHIP_090D	,
ZEROIFNULL(	DATEDG_090D	)	DATEDG_090D	,
ZEROIFNULL(	BUSMACH_090D	)	BUSMACH_090D	,
ZEROIFNULL(	BAGSLUG_090D	)	BAGSLUG_090D	,
ZEROIFNULL(	CLEANING_090D	)	CLEANING_090D	,
ZEROIFNULL(	BREAKROOM_090D	)	BREAKROOM_090D	,
ZEROIFNULL(	MOBILIT_090D	)	MOBILIT_090D	,
ZEROIFNULL(	PRESENTACC_090D	)	PRESENTACC_090D	,
ZEROIFNULL(	POSTAGE_090D	)	POSTAGE_090D	,
ZEROIFNULL(	LBLSHIPSTOR_090D	)	LBLSHIPSTOR_090D	,
ZEROIFNULL(	PORTABLECE_090D	)	PORTABLECE_090D	,
ZEROIFNULL(	BSD_090D	)	BSD_090D	,
ZEROIFNULL(	TECHSERV_090D	)	TECHSERV_090D	,
ZEROIFNULL(	ccenter_090D	)	ccenter_090D	,
ZEROIFNULL(	retail_090D	)	retail_090D	,
ZEROIFNULL(	S.online_090D	)	online_s090D	,
ZEROIFNULL(	anon_shop_090D	)	anon_shop_090D	,
ZEROIFNULL(	DLSC_DL_180D	)	DLSC_DL_180D	,
ZEROIFNULL(	SLSTYP_RETRN_180D	)	SLSTYP_RETRN_180D	,
ZEROIFNULL(	FURNITUR_180D	)	FURNITUR_180D	,
ZEROIFNULL(	FILEBND_180D	)	FILEBND_180D	,
ZEROIFNULL(	SPPAPER_180D	)	SPPAPER_180D	,
ZEROIFNULL(	CASEPAPER_180D	)	CASEPAPER_180D	,
ZEROIFNULL(	REAMPAPER_180D	)	REAMPAPER_180D	,
ZEROIFNULL(	REDPAPER_180D	)	REDPAPER_180D	,
ZEROIFNULL(	PAPEROTH_180D	)	PAPEROTH_180D	,
ZEROIFNULL(	INKJETC_180D	)	INKJETC_180D	,
ZEROIFNULL(	TONER_180D	)	TONER_180D	,
ZEROIFNULL(	MANAGEDPRINT_180D	)	MANAGEDPRINT_180D	,
ZEROIFNULL(	PRINTERS_180D	)	PRINTERS_180D	,
ZEROIFNULL(	COMPUTERS_180D	)	COMPUTERS_180D	,
ZEROIFNULL(	PCACC_180D	)	PCACC_180D	,
ZEROIFNULL(	SEATING_180D	)	SEATING_180D	,
ZEROIFNULL(	SOFTWARE_180D	)	SOFTWARE_180D	,
ZEROIFNULL(	STORGANDNETW_180D	)	STORGANDNETW_180D	,
ZEROIFNULL(	MONOPROJO_180D	)	MONOPROJO_180D	,
ZEROIFNULL(	DIGPRINTDOC_180D	)	DIGPRINTDOC_180D	,
ZEROIFNULL(	OFESSNTLS_180D	)	OFESSNTLS_180D	,
ZEROIFNULL(	SCHOOLSPPL_180D	)	SCHOOLSPPL_180D	,
ZEROIFNULL(	BUSSERV_180D	)	BUSSERV_180D	,
ZEROIFNULL(	MAILSHIP_180D	)	MAILSHIP_180D	,
ZEROIFNULL(	DATEDG_180D	)	DATEDG_180D	,
ZEROIFNULL(	BUSMACH_180D	)	BUSMACH_180D	,
ZEROIFNULL(	BAGSLUG_180D	)	BAGSLUG_180D	,
ZEROIFNULL(	CLEANING_180D	)	CLEANING_180D	,
ZEROIFNULL(	BREAKROOM_180D	)	BREAKROOM_180D	,
ZEROIFNULL(	MOBILIT_180D	)	MOBILIT_180D	,
ZEROIFNULL(	PRESENTACC_180D	)	PRESENTACC_180D	,
ZEROIFNULL(	POSTAGE_180D	)	POSTAGE_180D	,
ZEROIFNULL(	LBLSHIPSTOR_180D	)	LBLSHIPSTOR_180D	,
ZEROIFNULL(	PORTABLECE_180D	)	PORTABLECE_180D	,
ZEROIFNULL(	BSD_180D	)	BSD_180D	,
ZEROIFNULL(	TECHSERV_180D	)	TECHSERV_180D	,
ZEROIFNULL(	ccenter_180D	)	ccenter_180D	,
ZEROIFNULL(	retail_180D	)	retail_180D	,
ZEROIFNULL(	s.online_180D	)	sonline_180D	,
ZEROIFNULL(	anon_shop_180D	)	anon_shop_180D	,
ZEROIFNULL(	DLSC_DL_270D	)	DLSC_DL_270D	,
ZEROIFNULL(	SLSTYP_RETRN_270D	)	SLSTYP_RETRN_270D	,
ZEROIFNULL(	FURNITUR_270D	)	FURNITUR_270D	,
ZEROIFNULL(	FILEBND_270D	)	FILEBND_270D	,
ZEROIFNULL(	SPPAPER_270D	)	SPPAPER_270D	,
ZEROIFNULL(	CASEPAPER_270D	)	CASEPAPER_270D	,
ZEROIFNULL(	REAMPAPER_270D	)	REAMPAPER_270D	,
ZEROIFNULL(	REDPAPER_270D	)	REDPAPER_270D	,
ZEROIFNULL(	PAPEROTH_270D	)	PAPEROTH_270D	,
ZEROIFNULL(	INKJETC_270D	)	INKJETC_270D	,
ZEROIFNULL(	TONER_270D	)	TONER_270D	,
ZEROIFNULL(	MANAGEDPRINT_270D	)	MANAGEDPRINT_270D	,
ZEROIFNULL(	PRINTERS_270D	)	PRINTERS_270D	,
ZEROIFNULL(	COMPUTERS_270D	)	COMPUTERS_270D	,
ZEROIFNULL(	PCACC_270D	)	PCACC_270D	,
ZEROIFNULL(	SEATING_270D	)	SEATING_270D	,
ZEROIFNULL(	SOFTWARE_270D	)	SOFTWARE_270D	,
ZEROIFNULL(	STORGANDNETW_270D	)	STORGANDNETW_270D	,
ZEROIFNULL(	MONOPROJO_270D	)	MONOPROJO_270D	,
ZEROIFNULL(	DIGPRINTDOC_270D	)	DIGPRINTDOC_270D	,
ZEROIFNULL(	OFESSNTLS_270D	)	OFESSNTLS_270D	,
ZEROIFNULL(	SCHOOLSPPL_270D	)	SCHOOLSPPL_270D	,
ZEROIFNULL(	BUSSERV_270D	)	BUSSERV_270D	,
ZEROIFNULL(	MAILSHIP_270D	)	MAILSHIP_270D	,
ZEROIFNULL(	DATEDG_270D	)	DATEDG_270D	,
ZEROIFNULL(	BUSMACH_270D	)	BUSMACH_270D	,
ZEROIFNULL(	BAGSLUG_270D	)	BAGSLUG_270D	,
ZEROIFNULL(	CLEANING_270D	)	CLEANING_270D	,
ZEROIFNULL(	BREAKROOM_270D	)	BREAKROOM_270D	,
ZEROIFNULL(	MOBILIT_270D	)	MOBILIT_270D	,
ZEROIFNULL(	PRESENTACC_270D	)	PRESENTACC_270D	,
ZEROIFNULL(	POSTAGE_270D	)	POSTAGE_270D	,
ZEROIFNULL(	LBLSHIPSTOR_270D	)	LBLSHIPSTOR_270D	,
ZEROIFNULL(	PORTABLECE_270D	)	PORTABLECE_270D	,
ZEROIFNULL(	BSD_270D	)	BSD_270D	,
ZEROIFNULL(	TECHSERV_270D	)	TECHSERV_270D	,
ZEROIFNULL(	ccenter_270D	)	ccenter_270D	,
ZEROIFNULL(	retail_270D	)	retail_270D	,
ZEROIFNULL(	S.online_270D	)	sonline_270D	,
ZEROIFNULL(	anon_shop_270D	)	anon_shop_270D	,
ZEROIFNULL(	DLSC_DL_365D	)	DLSC_DL_365D	,
ZEROIFNULL(	SLSTYP_RETRN_365D	)	SLSTYP_RETRN_365D	,
ZEROIFNULL(	FURNITUR_365D	)	FURNITUR_365D	,
ZEROIFNULL(	FILEBND_365D	)	FILEBND_365D	,
ZEROIFNULL(	SPPAPER_365D	)	SPPAPER_365D	,
ZEROIFNULL(	CASEPAPER_365D	)	CASEPAPER_365D	,
ZEROIFNULL(	REAMPAPER_365D	)	REAMPAPER_365D	,
ZEROIFNULL(	REDPAPER_365D	)	REDPAPER_365D	,
ZEROIFNULL(	PAPEROTH_365D	)	PAPEROTH_365D	,
ZEROIFNULL(	INKJETC_365D	)	INKJETC_365D	,
ZEROIFNULL(	TONER_365D	)	TONER_365D	,
ZEROIFNULL(	MANAGEDPRINT_365D	)	MANAGEDPRINT_365D	,
ZEROIFNULL(	PRINTERS_365D	)	PRINTERS_365D	,
ZEROIFNULL(	COMPUTERS_365D	)	COMPUTERS_365D	,
ZEROIFNULL(	PCACC_365D	)	PCACC_365D	,
ZEROIFNULL(	SEATING_365D	)	SEATING_365D	,
ZEROIFNULL(	SOFTWARE_365D	)	SOFTWARE_365D	,
ZEROIFNULL(	STORGANDNETW_365D	)	STORGANDNETW_365D	,
ZEROIFNULL(	MONOPROJO_365D	)	MONOPROJO_365D	,
ZEROIFNULL(	DIGPRINTDOC_365D	)	DIGPRINTDOC_365D	,
ZEROIFNULL(	OFESSNTLS_365D	)	OFESSNTLS_365D	,
ZEROIFNULL(	SCHOOLSPPL_365D	)	SCHOOLSPPL_365D	,
ZEROIFNULL(	BUSSERV_365D	)	BUSSERV_365D	,
ZEROIFNULL(	MAILSHIP_365D	)	MAILSHIP_365D	,
ZEROIFNULL(	DATEDG_365D	)	DATEDG_365D	,
ZEROIFNULL(	BUSMACH_365D	)	BUSMACH_365D	,
ZEROIFNULL(	BAGSLUG_365D	)	BAGSLUG_365D	,
ZEROIFNULL(	CLEANING_365D	)	CLEANING_365D	,
ZEROIFNULL(	BREAKROOM_365D	)	BREAKROOM_365D	,
ZEROIFNULL(	MOBILIT_365D	)	MOBILIT_365D	,
ZEROIFNULL(	PRESENTACC_365D	)	PRESENTACC_365D	,
ZEROIFNULL(	POSTAGE_365D	)	POSTAGE_365D	,
ZEROIFNULL(	LBLSHIPSTOR_365D	)	LBLSHIPSTOR_365D	,
ZEROIFNULL(	PORTABLECE_365D	)	PORTABLECE_365D	,
ZEROIFNULL(	BSD_365D	)	BSD_365D	,
ZEROIFNULL(	TECHSERV_365D	)	TECHSERV_365D	,
ZEROIFNULL(	ccenter_365D	)	ccenter_365D	,
ZEROIFNULL(	retail_365D	)	retail_365D	,
ZEROIFNULL(	S.online_365D	)	sonline_365D	,
ZEROIFNULL(	anon_shop_365D	)	anon_shop_365D	,
ZEROIFNULL(	DLSC_DL_725D	)	DLSC_DL_725D	,
ZEROIFNULL(	SLSTYP_RETRN_725D	)	SLSTYP_RETRN_725D	,
ZEROIFNULL(	FURNITUR_725D	)	FURNITUR_725D	,
ZEROIFNULL(	FILEBND_725D	)	FILEBND_725D	,
ZEROIFNULL(	SPPAPER_725D	)	SPPAPER_725D	,
ZEROIFNULL(	CASEPAPER_725D	)	CASEPAPER_725D	,
ZEROIFNULL(	REAMPAPER_725D	)	REAMPAPER_725D	,
ZEROIFNULL(	REDPAPER_725D	)	REDPAPER_725D	,
ZEROIFNULL(	PAPEROTH_725D	)	PAPEROTH_725D	,
ZEROIFNULL(	INKJETC_725D	)	INKJETC_725D	,
ZEROIFNULL(	TONER_725D	)	TONER_725D	,
ZEROIFNULL(	MANAGEDPRINT_725D	)	MANAGEDPRINT_725D	,
ZEROIFNULL(	PRINTERS_725D	)	PRINTERS_725D	,
ZEROIFNULL(	COMPUTERS_725D	)	COMPUTERS_725D	,
ZEROIFNULL(	PCACC_725D	)	PCACC_725D	,
ZEROIFNULL(	SEATING_725D	)	SEATING_725D	,
ZEROIFNULL(	SOFTWARE_725D	)	SOFTWARE_725D	,
ZEROIFNULL(	STORGANDNETW_725D	)	STORGANDNETW_725D	,
ZEROIFNULL(	MONOPROJO_725D	)	MONOPROJO_725D	,
ZEROIFNULL(	DIGPRINTDOC_725D	)	DIGPRINTDOC_725D	,
ZEROIFNULL(	OFESSNTLS_725D	)	OFESSNTLS_725D	,
ZEROIFNULL(	SCHOOLSPPL_725D	)	SCHOOLSPPL_725D	,
ZEROIFNULL(	BUSSERV_725D	)	BUSSERV_725D	,
ZEROIFNULL(	MAILSHIP_725D	)	MAILSHIP_725D	,
ZEROIFNULL(	DATEDG_725D	)	DATEDG_725D	,
ZEROIFNULL(	BUSMACH_725D	)	BUSMACH_725D	,
ZEROIFNULL(	BAGSLUG_725D	)	BAGSLUG_725D	,
ZEROIFNULL(	CLEANING_725D	)	CLEANING_725D	,
ZEROIFNULL(	BREAKROOM_725D	)	BREAKROOM_725D	,
ZEROIFNULL(	MOBILIT_725D	)	MOBILIT_725D	,
ZEROIFNULL(	PRESENTACC_725D	)	PRESENTACC_725D	,
ZEROIFNULL(	POSTAGE_725D	)	POSTAGE_725D	,
ZEROIFNULL(	LBLSHIPSTOR_725D	)	LBLSHIPSTOR_725D	,
ZEROIFNULL(	PORTABLECE_725D	)	PORTABLECE_725D	,
ZEROIFNULL(	BSD_725D	)	BSD_725D	,
ZEROIFNULL(	TECHSERV_725D	)	TECHSERV_725D	,
ZEROIFNULL(	ccenter_725D	)	ccenter_725D	,
ZEROIFNULL(	retail_725D	)	retail_725D	,
ZEROIFNULL(	S.online_725D	)	sonline_725D	,
ZEROIFNULL(	anon_shop_725D	)	anon_shop_725D	,
ZEROIFNULL(	SLSTYP_LOYALACC_090D          	)	SLSTYP_LOYALACC_090D	,
ZEROIFNULL(	SLSTYP_LOYALACC_180D          	)	SLSTYP_LOYALACC_180D	,
ZEROIFNULL(	SLSTYP_LOYALACC_270D          	)	SLSTYP_LOYALACC_270D	,
ZEROIFNULL(	SLSTYP_LOYALACC_365D          	)	SLSTYP_LOYALACC_365D	,
ZEROIFNULL(	SLSTYP_LOYALACC_725D          	)	SLSTYP_LOYALACC_725D	,
ZEROIFNULL(	SLSTYP_LOYALRED_090D          	)	SLSTYP_LOYALRED_090D	,
ZEROIFNULL(	SLSTYP_LOYALRED_180D          	)	SLSTYP_LOYALRED_180D	,
ZEROIFNULL(	SLSTYP_LOYALRED_270D          	)	SLSTYP_LOYALRED_270D	,
ZEROIFNULL(	SLSTYP_LOYALRED_365D          	)	SLSTYP_LOYALRED_365D	,
ZEROIFNULL(	SLSTYP_LOYALRED_725D          	)	SLSTYP_LOYALRED_725D	,
ZEROIFNULL(	DELIVERY_AMT_090D             	)	DELIVERY_AMT_090D	,
ZEROIFNULL(	DELIVERY_AMT_180D             	)	DELIVERY_AMT_180D	,
ZEROIFNULL(	DELIVERY_AMT_270D             	)	DELIVERY_AMT_270D	,
ZEROIFNULL(	DELIVERY_AMT_365D             	)	DELIVERY_AMT_365D	,
ZEROIFNULL(	DELIVERY_AMT_725D             	)	DELIVERY_AMT_725D	,
ZEROIFNULL(	PICKUP_AMT_090D               	)	PICKUP_AMT_090D	,
ZEROIFNULL(	PICKUP_AMT_180D               	)	PICKUP_AMT_180D	,
ZEROIFNULL(	PICKUP_AMT_270D               	)	PICKUP_AMT_270D	,
ZEROIFNULL(	PICKUP_AMT_365D               	)	PICKUP_AMT_365D	,
ZEROIFNULL(	PICKUP_AMT_725D               	)	PICKUP_AMT_725D	,
zeroifnull(	C.STACK_090D	)	STACK_090D,
zeroifnull(	C.STACK_180D	)	STACK_180D,
zeroifnull(	C.STACK_270D	)	STACK_270D,
zeroifnull(	C.STACK_365D	)	STACK_365D,
zeroifnull(	C.STACK_725D	)	STACK_725D,
zeroifnull(	C.GCN_090D	)	GCN_090D,
zeroifnull(	C.GCN_180D	)	GCN_180D,
zeroifnull(	C.GCN_270D	)	GCN_270D,
zeroifnull(	C.GCN_365D	)	GCN_365D,
zeroifnull(	C.GCN_725D	)	GCN_725D,
zeroifnull(	C.CPN_03_090D	)	CPN_03_090D,
zeroifnull(	C.CPN_03_180D	)	CPN_03_180D,
zeroifnull(	C.CPN_03_270D	)	CPN_03_270D,
zeroifnull(	C.CPN_03_365D	)	CPN_03_365D,
zeroifnull(	C.CPN_03_725D	)	CPN_03_725D,
ZEROIFNULL((	BR_FURNITUR_090D              	) )	as	BR_FURNITUR_090D              	,
ZEROIFNULL((	BR_FILEBND_090D               	) )	as	BR_FILEBND_090D               	,
ZEROIFNULL((	BR_SPPAPER_090D               	) )	as	BR_SPPAPER_090D               	,
ZEROIFNULL((	BR_CASEPAPER_090D             	) )	as	BR_CASEPAPER_090D             	,
ZEROIFNULL((	BR_REAMPAPER_090D             	) )	as	BR_REAMPAPER_090D             	,
ZEROIFNULL((	BR_REDPAPER_090D              	) )	as	BR_REDPAPER_090D              	,
ZEROIFNULL((	BR_PAPEROTH_090D              	) )	as	BR_PAPEROTH_090D              	,
ZEROIFNULL((	BR_INKJETC_090D               	) )	as	BR_INKJETC_090D               	,
ZEROIFNULL((	BR_TONER_090D                 	) )	as	BR_TONER_090D                 	,
ZEROIFNULL((	BR_MANAGEDPRINT_090D          	) )	as	BR_MANAGEDPRINT_090D          	,
ZEROIFNULL((	BR_PRINTERS_090D              	) )	as	BR_PRINTERS_090D              	,
ZEROIFNULL((	BR_COMPUTERS_090D             	) )	as	BR_COMPUTERS_090D             	,
ZEROIFNULL((	BR_PCACC_090D                 	) )	as	BR_PCACC_090D                 	,
ZEROIFNULL((	BR_SEATING_090D               	) )	as	BR_SEATING_090D               	,
ZEROIFNULL((	BR_SOFTWARE_090D              	) )	as	BR_SOFTWARE_090D              	,
ZEROIFNULL((	BR_STORGANDNETW_090D          	) )	as	BR_STORGANDNETW_090D          	,
ZEROIFNULL((	BR_MONOPROJO_090D             	) )	as	BR_MONOPROJO_090D             	,
ZEROIFNULL((	BR_DIGPRINTDOC_090D           	) )	as	BR_DIGPRINTDOC_090D           	,
ZEROIFNULL((	BR_OFESSNTLS_090D             	) )	as	BR_OFESSNTLS_090D             	,
ZEROIFNULL((	BR_SCHOOLSPPL_090D            	) )	as	BR_SCHOOLSPPL_090D            	,
ZEROIFNULL((	BR_BUSSERV_090D               	) )	as	BR_BUSSERV_090D               	,
ZEROIFNULL((	BR_MAILSHIP_090D              	) )	as	BR_MAILSHIP_090D              	,
ZEROIFNULL((	BR_DATEDG_090D                	) )	as	BR_DATEDG_090D                	,
ZEROIFNULL((	BR_BUSMACH_090D               	) )	as	BR_BUSMACH_090D               	,
ZEROIFNULL((	BR_BAGSLUG_090D               	) )	as	BR_BAGSLUG_090D               	,
ZEROIFNULL((	BR_CLEANING_090D              	) )	as	BR_CLEANING_090D              	,
ZEROIFNULL((	BR_BREAKROOM_090D             	) )	as	BR_BREAKROOM_090D             	,
ZEROIFNULL((	BR_MOBILIT_090D               	) )	as	BR_MOBILIT_090D               	,
ZEROIFNULL((	BR_PRESENTACC_090D            	) )	as	BR_PRESENTACC_090D            	,
ZEROIFNULL((	BR_POSTAGE_090D               	) )	as	BR_POSTAGE_090D               	,
ZEROIFNULL((	BR_LBLSHIPSTOR_090D           	) )	as	BR_LBLSHIPSTOR_090D           	,
ZEROIFNULL((	BR_PORTABLECE_090D            	) )	as	BR_PORTABLECE_090D            	,
ZEROIFNULL((	BR_BSD_090D                   	) )	as	BR_BSD_090D                   	,
ZEROIFNULL((	BR_TECHSERV_090D              	) )	as	BR_TECHSERV_090D              	,
ZEROIFNULL((	BR_LOYALACCRU_090D            	) )	as	BR_LOYALACCRU_090D            	,
ZEROIFNULL((	BR_FURNITUR_270D              	) )	as	BR_FURNITUR_270D              	,
ZEROIFNULL((	BR_FILEBND_270D               	) )	as	BR_FILEBND_270D               	,
ZEROIFNULL((	BR_SPPAPER_270D               	) )	as	BR_SPPAPER_270D               	,
ZEROIFNULL((	BR_CASEPAPER_270D             	) )	as	BR_CASEPAPER_270D             	,
ZEROIFNULL((	BR_REAMPAPER_270D             	) )	as	BR_REAMPAPER_270D             	,
ZEROIFNULL((	BR_REDPAPER_270D              	) )	as	BR_REDPAPER_270D              	,
ZEROIFNULL((	BR_PAPEROTH_270D              	) )	as	BR_PAPEROTH_270D              	,
ZEROIFNULL((	BR_INKJETC_270D               	) )	as	BR_INKJETC_270D               	,
ZEROIFNULL((	BR_TONER_270D                 	) )	as	BR_TONER_270D                 	,
ZEROIFNULL((	BR_MANAGEDPRINT_270D          	) )	as	BR_MANAGEDPRINT_270D          	,
ZEROIFNULL((	BR_PRINTERS_270D              	) )	as	BR_PRINTERS_270D              	,
ZEROIFNULL((	BR_COMPUTERS_270D             	) )	as	BR_COMPUTERS_270D             	,
ZEROIFNULL((	BR_PCACC_270D                 	) )	as	BR_PCACC_270D                 	,
ZEROIFNULL((	BR_SEATING_270D               	) )	as	BR_SEATING_270D               	,
ZEROIFNULL((	BR_SOFTWARE_270D              	) )	as	BR_SOFTWARE_270D              	,
ZEROIFNULL((	BR_STORGANDNETW_270D          	) )	as	BR_STORGANDNETW_270D          	,
ZEROIFNULL((	BR_MONOPROJO_270D             	) )	as	BR_MONOPROJO_270D             	,
ZEROIFNULL((	BR_DIGPRINTDOC_270D           	) )	as	BR_DIGPRINTDOC_270D           	,
ZEROIFNULL((	BR_OFESSNTLS_270D             	) )	as	BR_OFESSNTLS_270D             	,
ZEROIFNULL((	BR_SCHOOLSPPL_270D            	) )	as	BR_SCHOOLSPPL_270D            	,
ZEROIFNULL((	BR_BUSSERV_270D               	) )	as	BR_BUSSERV_270D               	,
ZEROIFNULL((	BR_MAILSHIP_270D              	) )	as	BR_MAILSHIP_270D              	,
ZEROIFNULL((	BR_DATEDG_270D                	) )	as	BR_DATEDG_270D                	,
ZEROIFNULL((	BR_BUSMACH_270D               	) )	as	BR_BUSMACH_270D               	,
ZEROIFNULL((	BR_BAGSLUG_270D               	) )	as	BR_BAGSLUG_270D               	,
ZEROIFNULL((	BR_CLEANING_270D              	) )	as	BR_CLEANING_270D              	,
ZEROIFNULL((	BR_BREAKROOM_270D             	) )	as	BR_BREAKROOM_270D             	,
ZEROIFNULL((	BR_MOBILIT_270D               	) )	as	BR_MOBILIT_270D               	,
ZEROIFNULL((	BR_PRESENTACC_270D            	) )	as	BR_PRESENTACC_270D            	,
ZEROIFNULL((	BR_POSTAGE_270D               	) )	as	BR_POSTAGE_270D               	,
ZEROIFNULL((	BR_LBLSHIPSTOR_270D           	) )	as	BR_LBLSHIPSTOR_270D           	,
ZEROIFNULL((	BR_PORTABLECE_270D            	) )	as	BR_PORTABLECE_270D            	,
ZEROIFNULL((	BR_BSD_270D                   	) )	as	BR_BSD_270D                   	,
ZEROIFNULL((	BR_TECHSERV_270D              	) )	as	BR_TECHSERV_270D              	,
ZEROIFNULL((	BR_LOYALACCRU_270D            	) )	as	BR_LOYALACCRU_270D            	,
ZEROIFNULL((	BR_FURNITUR_180D              	) )	as	BR_FURNITUR_180D              	,
ZEROIFNULL((	BR_FILEBND_180D               	) )	as	BR_FILEBND_180D               	,
ZEROIFNULL((	BR_SPPAPER_180D               	) )	as	BR_SPPAPER_180D               	,
ZEROIFNULL((	BR_CASEPAPER_180D             	) )	as	BR_CASEPAPER_180D             	,
ZEROIFNULL((	BR_REAMPAPER_180D             	) )	as	BR_REAMPAPER_180D             	,
ZEROIFNULL((	BR_REDPAPER_180D              	) )	as	BR_REDPAPER_180D              	,
ZEROIFNULL((	BR_PAPEROTH_180D              	) )	as	BR_PAPEROTH_180D              	,
ZEROIFNULL((	BR_INKJETC_180D               	) )	as	BR_INKJETC_180D               	,
ZEROIFNULL((	BR_TONER_180D                 	) )	as	BR_TONER_180D                 	,
ZEROIFNULL((	BR_MANAGEDPRINT_180D          	) )	as	BR_MANAGEDPRINT_180D          	,
ZEROIFNULL((	BR_PRINTERS_180D              	) )	as	BR_PRINTERS_180D              	,
ZEROIFNULL((	BR_COMPUTERS_180D             	) )	as	BR_COMPUTERS_180D             	,
ZEROIFNULL((	BR_PCACC_180D                 	) )	as	BR_PCACC_180D                 	,
ZEROIFNULL((	BR_SEATING_180D               	) )	as	BR_SEATING_180D               	,
ZEROIFNULL((	BR_SOFTWARE_180D              	) )	as	BR_SOFTWARE_180D              	,
ZEROIFNULL((	BR_STORGANDNETW_180D          	) )	as	BR_STORGANDNETW_180D          	,
ZEROIFNULL((	BR_MONOPROJO_180D             	) )	as	BR_MONOPROJO_180D             	,
ZEROIFNULL((	BR_DIGPRINTDOC_180D           	) )	as	BR_DIGPRINTDOC_180D           	,
ZEROIFNULL((	BR_OFESSNTLS_180D             	) )	as	BR_OFESSNTLS_180D             	,
ZEROIFNULL((	BR_SCHOOLSPPL_180D            	) )	as	BR_SCHOOLSPPL_180D            	,
ZEROIFNULL((	BR_BUSSERV_180D               	) )	as	BR_BUSSERV_180D               	,
ZEROIFNULL((	BR_MAILSHIP_180D              	) )	as	BR_MAILSHIP_180D              	,
ZEROIFNULL((	BR_DATEDG_180D                	) )	as	BR_DATEDG_180D                	,
ZEROIFNULL((	BR_BUSMACH_180D               	) )	as	BR_BUSMACH_180D               	,
ZEROIFNULL((	BR_BAGSLUG_180D               	) )	as	BR_BAGSLUG_180D               	,
ZEROIFNULL((	BR_CLEANING_180D              	) )	as	BR_CLEANING_180D              	,
ZEROIFNULL((	BR_BREAKROOM_180D             	) )	as	BR_BREAKROOM_180D             	,
ZEROIFNULL((	BR_MOBILIT_180D               	) )	as	BR_MOBILIT_180D               	,
ZEROIFNULL((	BR_PRESENTACC_180D            	) )	as	BR_PRESENTACC_180D            	,
ZEROIFNULL((	BR_POSTAGE_180D               	) )	as	BR_POSTAGE_180D               	,
ZEROIFNULL((	BR_LBLSHIPSTOR_180D           	) )	as	BR_LBLSHIPSTOR_180D           	,
ZEROIFNULL((	BR_PORTABLECE_180D            	) )	as	BR_PORTABLECE_180D            	,
ZEROIFNULL((	BR_BSD_180D                   	) )	as	BR_BSD_180D                   	,
ZEROIFNULL((	BR_TECHSERV_180D              	) )	as	BR_TECHSERV_180D              	,
ZEROIFNULL((	BR_LOYALACCRU_180D            	) )	as	BR_LOYALACCRU_180D            	,
ZEROIFNULL((	BR_FURNITUR_365D              	) )	as	BR_FURNITUR_365D              	,
ZEROIFNULL((	BR_FILEBND_365D               	) )	as	BR_FILEBND_365D               	,
ZEROIFNULL((	BR_SPPAPER_365D               	) )	as	BR_SPPAPER_365D               	,
ZEROIFNULL((	BR_CASEPAPER_365D             	) )	as	BR_CASEPAPER_365D             	,
ZEROIFNULL((	BR_REAMPAPER_365D             	) )	as	BR_REAMPAPER_365D             	,
ZEROIFNULL((	BR_REDPAPER_365D              	) )	as	BR_REDPAPER_365D              	,
ZEROIFNULL((	BR_PAPEROTH_365D              	) )	as	BR_PAPEROTH_365D              	,
ZEROIFNULL((	BR_INKJETC_365D               	) )	as	BR_INKJETC_365D               	,
ZEROIFNULL((	BR_TONER_365D                 	) )	as	BR_TONER_365D                 	,
ZEROIFNULL((	BR_MANAGEDPRINT_365D          	) )	as	BR_MANAGEDPRINT_365D          	,
ZEROIFNULL((	BR_PRINTERS_365D              	) )	as	BR_PRINTERS_365D              	,
ZEROIFNULL((	BR_COMPUTERS_365D             	) )	as	BR_COMPUTERS_365D             	,
ZEROIFNULL((	BR_PCACC_365D                 	) )	as	BR_PCACC_365D                 	,
ZEROIFNULL((	BR_SEATING_365D               	) )	as	BR_SEATING_365D               	,
ZEROIFNULL((	BR_SOFTWARE_365D              	) )	as	BR_SOFTWARE_365D              	,
ZEROIFNULL((	BR_STORGANDNETW_365D          	) )	as	BR_STORGANDNETW_365D          	,
ZEROIFNULL((	BR_MONOPROJO_365D             	) )	as	BR_MONOPROJO_365D             	,
ZEROIFNULL((	BR_DIGPRINTDOC_365D           	) )	as	BR_DIGPRINTDOC_365D           	,
ZEROIFNULL((	BR_OFESSNTLS_365D             	) )	as	BR_OFESSNTLS_365D             	,
ZEROIFNULL((	BR_SCHOOLSPPL_365D            	) )	as	BR_SCHOOLSPPL_365D            	,
ZEROIFNULL((	BR_BUSSERV_365D               	) )	as	BR_BUSSERV_365D               	,
ZEROIFNULL((	BR_MAILSHIP_365D              	) )	as	BR_MAILSHIP_365D              	,
ZEROIFNULL((	BR_DATEDG_365D                	) )	as	BR_DATEDG_365D                	,
ZEROIFNULL((	BR_BUSMACH_365D               	) )	as	BR_BUSMACH_365D               	,
ZEROIFNULL((	BR_BAGSLUG_365D               	) )	as	BR_BAGSLUG_365D               	,
ZEROIFNULL((	BR_CLEANING_365D              	) )	as	BR_CLEANING_365D              	,
ZEROIFNULL((	BR_BREAKROOM_365D             	) )	as	BR_BREAKROOM_365D             	,
ZEROIFNULL((	BR_MOBILIT_365D               	) )	as	BR_MOBILIT_365D               	,
ZEROIFNULL((	BR_PRESENTACC_365D            	) )	as	BR_PRESENTACC_365D            	,
ZEROIFNULL((	BR_POSTAGE_365D               	) )	as	BR_POSTAGE_365D               	,
ZEROIFNULL((	BR_LBLSHIPSTOR_365D           	) )	as	BR_LBLSHIPSTOR_365D           	,
ZEROIFNULL((	BR_PORTABLECE_365D            	) )	as	BR_PORTABLECE_365D            	,
ZEROIFNULL((	BR_BSD_365D                   	) )	as	BR_BSD_365D                   	,
ZEROIFNULL((	BR_TECHSERV_365D              	) )	as	BR_TECHSERV_365D              	,
ZEROIFNULL((	BR_LOYALACCRU_365D            	) )	as	BR_LOYALACCRU_365D            	,
ZEROIFNULL((	BR_FURNITUR_725D              	) )	as	BR_FURNITUR_725D              	,
ZEROIFNULL((	BR_FILEBND_725D               	) )	as	BR_FILEBND_725D               	,
ZEROIFNULL((	BR_SPPAPER_725D               	) )	as	BR_SPPAPER_725D               	,
ZEROIFNULL((	BR_CASEPAPER_725D             	) )	as	BR_CASEPAPER_725D             	,
ZEROIFNULL((	BR_REAMPAPER_725D             	) )	as	BR_REAMPAPER_725D             	,
ZEROIFNULL((	BR_REDPAPER_725D              	) )	as	BR_REDPAPER_725D              	,
ZEROIFNULL((	BR_PAPEROTH_725D              	) )	as	BR_PAPEROTH_725D              	,
ZEROIFNULL((	BR_INKJETC_725D               	) )	as	BR_INKJETC_725D               	,
ZEROIFNULL((	BR_TONER_725D                 	) )	as	BR_TONER_725D                 	,
ZEROIFNULL((	BR_MANAGEDPRINT_725D          	) )	as	BR_MANAGEDPRINT_725D          	,
ZEROIFNULL((	BR_PRINTERS_725D              	) )	as	BR_PRINTERS_725D              	,
ZEROIFNULL((	BR_COMPUTERS_725D             	) )	as	BR_COMPUTERS_725D             	,
ZEROIFNULL((	BR_PCACC_725D                 	) )	as	BR_PCACC_725D                 	,
ZEROIFNULL((	BR_SEATING_725D               	) )	as	BR_SEATING_725D               	,
ZEROIFNULL((	BR_SOFTWARE_725D              	) )	as	BR_SOFTWARE_725D              	,
ZEROIFNULL((	BR_STORGANDNETW_725D          	) )	as	BR_STORGANDNETW_725D          	,
ZEROIFNULL((	BR_MONOPROJO_725D             	) )	as	BR_MONOPROJO_725D             	,
ZEROIFNULL((	BR_DIGPRINTDOC_725D           	) )	as	BR_DIGPRINTDOC_725D           	,
ZEROIFNULL((	BR_OFESSNTLS_725D             	) )	as	BR_OFESSNTLS_725D             	,
ZEROIFNULL((	BR_SCHOOLSPPL_725D            	) )	as	BR_SCHOOLSPPL_725D            	,
ZEROIFNULL((	BR_BUSSERV_725D               	) )	as	BR_BUSSERV_725D               	,
ZEROIFNULL((	BR_MAILSHIP_725D              	) )	as	BR_MAILSHIP_725D              	,
ZEROIFNULL((	BR_DATEDG_725D                	) )	as	BR_DATEDG_725D                	,
ZEROIFNULL((	BR_BUSMACH_725D               	) )	as	BR_BUSMACH_725D               	,
ZEROIFNULL((	BR_BAGSLUG_725D               	) )	as	BR_BAGSLUG_725D               	,
ZEROIFNULL((	BR_CLEANING_725D              	) )	as	BR_CLEANING_725D              	,
ZEROIFNULL((	BR_BREAKROOM_725D             	) )	as	BR_BREAKROOM_725D             	,
ZEROIFNULL((	BR_MOBILIT_725D               	) )	as	BR_MOBILIT_725D               	,
ZEROIFNULL((	BR_PRESENTACC_725D            	) )	as	BR_PRESENTACC_725D            	,
ZEROIFNULL((	BR_POSTAGE_725D               	) )	as	BR_POSTAGE_725D               	,
ZEROIFNULL((	BR_LBLSHIPSTOR_725D           	) )	as	BR_LBLSHIPSTOR_725D           	,
ZEROIFNULL((	BR_PORTABLECE_725D            	) )	as	BR_PORTABLECE_725D            	,
ZEROIFNULL((	BR_BSD_725D                   	) )	as	BR_BSD_725D                   	,
ZEROIFNULL((	BR_TECHSERV_725D              	) )	as	BR_TECHSERV_725D              	,
ZEROIFNULL((	BR_LOYALACCRU_725D            	) )	as	BR_LOYALACCRU_725D            	,

0 as tgt_event

from mw_ch a  --check
left join 
mw_contact_hist b -- check
on a.agent_id = b.agent_id
left join
mw_coupon_vehicles_CNT c --check
on a.agent_id = c.agent_id
left join
mw_loyal_daily_cnt d 
on a.agent_id = d.agent_id
LEFT JOIN
MW_BROWSE_NOT_BUY W
ON A.AGENT_ID = W.AGENT_ID
LEFT JOIN
MW_LOYAL_MONTHLY_CNT LM  --part 1 of 2
			--
ON A.AGENT_ID = LM.AGENT_ID
LEFT JOIN
MW_SALES_CNT S
ON A.AGENT_ID = S.AGENT_ID
) with data;

--SET TARGET VARIABLE


create volatile table  mw_camp_counts
as (
sel agent_id, 
sum(net_sales_amt) 
as camp_amt
from  customer_v.campaign_response_history
where campaign_id = 'DM1017'
GROUP BY 1)
with data on commit preserve rows;

update tempdb.mw_train_5000_COUNTS
set tgt_event = 1
where agent_id in (sel agent_id
from  mw_camp_counts
where camp_amt > 29.99);


--CREATE GREAT CIRCLE HAVERSINE DISTANCE FOR CUSTOMER TO STORE

drop table  TEMPDB.MW_LOC_geo;

CREATE TABLE TEMPDB.MW_LOC_geo AS (

sel 
a.LOC_ID, 
A.ADDRESS_ID, 
LONGITUDE, 
LATITUDE from 
od.LOC_X_COMM_ADDR  A
JOIN
CUSTOMER_V.AGENT_ADDRESS_DIM         B
ON A.ADDRESS_ID = B.ADDRESS_ID
GROUP BY 1,2,3,4
) WITH DATA;

drop table TEMPDB.MW_AGENT_X_DIST ;

CREATE TABLE TEMPDB.MW_AGENT_X_DIST AS 


(

SELECT A.AGENT_ID, 
A.LATITUDE, 
A.LONGITUDE, 
B.NS_LAT, 
B.NS_LONG,
(radians(ns_LAT)-RADIANS(LATITUDE)) DLAT,
(RADIANS(Ns_LONG)-RADIANS(LONGITUDE)) DLONG,
ZEROIFNULL
(
(2*asin(sqrt(
SIN((radians(ns_LAT)-RADIANS(LATITUDE))/2)*sin((radians(ns_LAT)-RADIANS(LATITUDE))/2)
+sin((RADIANS(Ns_LONG)-RADIANS(LONGITUDE)) /2)*SIN((RADIANS(Ns_LONG)-RADIANS(LONGITUDE)) )
*COS(RADIANS(LATITUDE))*COS(RADIANS(DLAT)))))*6372.8) as RADDIST
FROM 
customer_v.AGENT_ADDRESS_DIM   A
JOIN
(
SEL  
--B.address_id,
A.NEARESTSTORELOC_ID            , 
b.loc_id, 
b.LATITUDE AS NS_LAT,
B.LONGITUDE AS NS_LONG
FROM  cERESSYS_V.CRS_MAIL_OUT   A
JOIN
TEMPDB.MW_LOC_GEO B
ON A.NEARESTSTORELOC_ID             = B.LOC_ID
GROUP BY 1,2,3,4
)
B
ON A.NEAREST_OD_NUM                 = b.NEARESTSTORELOC_ID
WHERE A.AGENT_ID IN 
(SEL AGENT_ID FROM 
mw_ch) 
 and a.latitude > 0
 and a.longitude < 0

 ) WITH DATA;
 
 
 alter table tempdb.mw_TRAIN_5000_COUNTS  add od_dist decimal (10,4);
 
 update tempdb.mw_TRAIN_5000_COUNTS
 set od_dist = raddist
 where tempdb.mw_TRAIN_5000_COUNTS.agent_id = TEMPDB.MW_AGENT_X_DIST.agent_id;
 
 ---CLEAR OUT AGENT ORDERS TO STORES
 
delete from   tempdb.mw_TRAIN_5000_COUNTS
 where od_dist = 0;
 
 --ADD  AND SET FLAG FOR MISSING DISTANCES
 
 alter table  tempdb.mw_TRAIN_5000_COUNTS
 add dist_miss int default 0;
 
 update
  tempdb.mw_TRAIN_5000_COUNTS
  set dist_miss = 1
  where od_dist is null;
  
 update  tempdb.mw_TRAIN_5000_COUNTS
 set od_dist = 6.5
 where od_dist is null;
