-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Congestive Heart Failure Preventative Quality Indicator
-- MAGIC ICD9 code version https://www.cms.gov/files/document/aco-10-prevention-quality-indicator-pqi-ambulatory-sensitive-conditions-admissions-heart-failure-hf.pdf  
-- MAGIC ICD10 code (latest) version https://qualityindicators.ahrq.gov/Downloads/Modules/PQI/V2022/TechSpecs/PQI_08_Heart_Failure_Admission_Rate.pdf

-- COMMAND ----------

CREATE WIDGET TEXT SOURCE_DB  DEFAULT "hls_cms_synpuf";
CREATE WIDGET TEXT TARGET_DB  DEFAULT "ahrq_pqi";

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${TARGET_DB};
USE ${TARGET_DB};
show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculating the Denominator 
-- MAGIC 
-- MAGIC "Expected discharges from an acute care or critical access hospital with a principal diagnosis of HF, for Medicare FFS beneficiaries assigned or aligned to an ACO, aged 18 years and older, with HF"
-- MAGIC 
-- MAGIC Membership
-- MAGIC 1. Include all patients with both part A and part B Medicare
-- MAGIC 2. Include any patient who has been diagnosed with CHF in any setting
-- MAGIC 3. Exclude a diagnosis of ESRD (end stage renal disease) 
-- MAGIC 
-- MAGIC Inpatient Event
-- MAGIC 1. Exclude Admissions that are transfers from other facilities 

-- COMMAND ----------

-- DBTITLE 1,Create Widget for Denominator Diagnosis Inclusions (Heart Failure)
CREATE WIDGET TEXT denominator_icd_dx_inclusions DEFAULT ("'39891', '4280', '4281', '42820', '42821', '42822', '42823', '42830', '42831', '42832', '42833', '42840', '42841', '42842', '42843', '4289'");

-- COMMAND ----------

-- DBTITLE 1,Create widgets for diagnosis column lists to filter on 
-- MAGIC %python
-- MAGIC #dynamically select diagnosis columns
-- MAGIC def dx_cols(tablename):
-- MAGIC   return [str("NVL(" + col.name + ", '')") for col in spark.table(tablename).schema.fields if 'DGNS' in col.name]
-- MAGIC #dynamically select procedure columns
-- MAGIC def px_cols(tablename):
-- MAGIC   return [str("NVL(" + col.name + ", '')") for col in spark.table(tablename).schema.fields if ('PRCDR' in col.name)]
-- MAGIC 
-- MAGIC #inpatient
-- MAGIC widget_name = 'inp_dx_cols' 
-- MAGIC columns_select = ', '.join(dx_cols('hls_healthcare.hls_cms_synpuf.inp_claims')) #diagnosis columns
-- MAGIC dbutils.widgets.remove(widget_name)
-- MAGIC dbutils.widgets.text(widget_name, columns_select)
-- MAGIC 
-- MAGIC #carrier
-- MAGIC widget_name = 'car_dx_cols'
-- MAGIC columns_select = ', '.join(dx_cols('hls_healthcare.hls_cms_synpuf.car_claims') ) #diagnosis columns
-- MAGIC dbutils.widgets.remove(widget_name)
-- MAGIC dbutils.widgets.text(widget_name, columns_select)
-- MAGIC 
-- MAGIC #outpatient
-- MAGIC widget_name = 'out_dx_cols'
-- MAGIC columns_select = ', '.join(dx_cols('hls_healthcare.hls_cms_synpuf.out_claims')) #diagnosis columns
-- MAGIC dbutils.widgets.remove(widget_name)
-- MAGIC dbutils.widgets.text(widget_name, columns_select)

-- COMMAND ----------

drop table if exists all_member_diagnosis;
create table all_member_diagnosis
as
select DESYNPUF_ID, left(CLM_ADMSN_DT,4) as admission_year, array(${inp_dx_cols}) as dx_cds
  from ${SOURCE_DB}.inp_claims a --inpatient claims
 UNION ALL 
  select   DESYNPUF_ID, left(CLM_FROM_DT,4) as admission_year, array(${car_dx_cols}) as dx_cds
  from ${SOURCE_DB}.car_claims a --carrier claims
 UNION ALL
  select DESYNPUF_ID, left(CLM_FROM_DT,4) as admission_year, array(${out_dx_cols}) as dx_cds
  from ${SOURCE_DB}.out_claims a --outpatient claims

-- COMMAND ----------

-- DBTITLE 1,Eligible Denominator Members
--Scope of (1) (2) and (3) to create the denominator base memberset
drop table if exists denominator_inclusions_membership;
create table denominator_inclusions_membership
as
select distinct eligible_members.desynpuf_id
from
(
  select distinct desynpuf_id
  from ${SOURCE_DB}.ben_sum
  group by desynpuf_id
  having sum(BENE_HI_CVRAGE_TOT_MONS) > 0 and sum(BENE_SMI_CVRAGE_TOT_MONS) > 0 
) eligible_members -- (1) all patients with A & B 
INNER JOIN all_member_diagnosis  heart_failure_diagnosis   
  on eligible_members.desynpuf_id = heart_failure_diagnosis.desynpuf_id --(2) join filter of members with heart failure
    and arrays_overlap(heart_failure_diagnosis.dx_cds,array(${denominator_icd_dx_inclusions}))
where not exists --(3) exclude esrd members
  (select 1 
    from ${SOURCE_DB}.ben_sum 
    where BENE_ESRD_IND = 'Y' --this can also be a filter on ESRD Diagnosis 5856
      and ben_sum.desynpuf_id = eligible_members.desynpuf_id
   )
  ;
select count(1) from denominator_inclusions_membership; -- 935,276 members

-- COMMAND ----------

-- DBTITLE 1,All Denominator Events
drop table if exists denominator;
create table denominator as
select  members.desynpuf_id, 
  inp_claims.clm_id,
  inp_claims.segment,
  inp_claims.CLM_ADMSN_DT 
from denominator_inclusions_membership members
inner join ${SOURCE_DB}.inp_claims --inpatient events 
  on inp_claims.desynpuf_id = members.desynpuf_id
--where src_adms not in (UB04 Point of Origin -4, 5, 6)  No transfers, this field does not exist in our claims dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculating the Numerator
-- MAGIC 
-- MAGIC (1) DRG is not ungroupable aka drg_cd != 999  
-- MAGIC (2) Principal diagnosis is heart failure  
-- MAGIC (3) Remove procedure exclusions list 

-- COMMAND ----------

-- DBTITLE 1,Create Widget for Numerator Procedure Exclusions 
CREATE WIDGET TEXT numerator_icd_px_exclusions DEFAULT ("'0050', '0051', '0052', '0053', '0054', '0056', '0057', '0066', '1751', '1752', '1755', '3500', '3501', '3502', '3503', '3504', '3505', '3506', '3507', '3508', '3509', '3510', '3511', '3512', '3513', '3514', '3520', '3521', '3522', '3523', '3524', '3525', '3526', '3527', '3528', '3531', '3532', '3533', '3534', '3535', '3539', '3541', '3542', '3550', '3551', '3552', '3553', '3554', '3555', '3560', '3561', '3562', '3563', '3570', '3571', '3572', '3573', '3581', '3582', '3583', '3584', '3591', '3592', '3593', '3594', '3595', '3596', '3597', '3598', '3599', '3603', '3604', '3606', '3607', '3609', '3610', '3611', '3612', '3613', '3614', '3615', '3616', '3617', '3619', '3631', '3632', '3633', '3634', '3639', '3691', '3699', '3731', '3732', '3733', '3734', '3735', '3736', '3737', '3741', '3751', '3752', '3753', '3754', '3755', '3760', '3761', '3762', '3763', '3764', '3765', '3766', '3770', '3771', '3772', '3773', '3774', '3775', '3776', '3777', '3778', '3779', '3780', '3781', '3782', '3783', '3785', '3786', '3787', '3789', '3794', '3795', '3796', '3797', '3798', '3826'");
--'362'

-- COMMAND ----------

drop table if exists numerator ;
create table numerator
as
select denom.*
from denominator denom
  inner join ${SOURCE_DB}.inp_claims claims
on denom.clm_id = claims.clm_id
  and denom.segment = claims.segment
where claims.clm_drg_cd != 999 --(1) ungroupable DRG
  and ICD9_DGNS_CD_1 in (${denominator_icd_dx_inclusions}) -- (2) Principal dx of hf
  and not arrays_overlap(ARRAY(NVL(ICD9_PRCDR_CD_1, ''), NVL(ICD9_PRCDR_CD_2, ''), NVL(ICD9_PRCDR_CD_3, ''), NVL(ICD9_PRCDR_CD_4, ''), NVL(ICD9_PRCDR_CD_5, ''), NVL(ICD9_PRCDR_CD_6, '')), array(${numerator_icd_px_exclusions})) -- (3) removed all major heart procedures from list
;
select count(1) from numerator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculating Quality Metric Scores for ACOs
-- MAGIC 
-- MAGIC lower score = better preventative care

-- COMMAND ----------

-- DBTITLE 1,National score of our entire dataset
select sum(case when o.clm_id is not null then 1 else 0 end) as numerator,
  count(1) as denominator,
  sum(case when o.clm_id is not null then 1 else 0 end)  / count(1)  as score
from denominator d
left outer join numerator o
  on d.clm_id = o.clm_id and d.segment=o.segment

-- COMMAND ----------

-- DBTITLE 1,Scores by years
select left(d.CLM_ADMSN_DT,4) as year, 
  sum(case when o.clm_id is not null then 1 else 0 end) as numerator,
  count(1) as denominator,
  sum(case when o.clm_id is not null then 1 else 0 end)  / count(1)  as score
from denominator d
left outer join numerator o
  on d.clm_id = o.clm_id and d.segment=o.segment
group by left(d.CLM_ADMSN_DT,4)
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note: Determining the proper ACO typically requires attributing membership to PCPs. For CHF PQI, this is not indictive of hospital ineffeciency but rather other outpatient care settings leading to an inpatient event.
-- MAGIC 
-- MAGIC For this, we naively attribute a member to a max value in the PCP visits from carrier claims. In practice this would not be the case

-- COMMAND ----------

-- DBTITLE 1,Scores by ACO by Year
select attributed_aco, 
  left(d.CLM_ADMSN_DT,4) as year, 
  sum(case when o.clm_id is not null then 1 else 0 end) as numerator,
  count(1) as denominator,
  sum(case when o.clm_id is not null then 1 else 0 end)  / count(1)  as score
from denominator d
left outer join numerator o
  on d.clm_id = o.clm_id and d.segment=o.segment
inner join (select desynpuf_id, max(tax_num_1) as attributed_aco from ${SOURCE_DB}.car_claims group by desynpuf_id) attribution
  on attribution.desynpuf_id = d.desynpuf_id
group by attributed_aco, left(d.CLM_ADMSN_DT,4)
having count(1) > 50
;
