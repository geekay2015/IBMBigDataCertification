/************************************************************************************/
/*** Program Name : MediCarePlanAnalysis.hql                                      ***/
/*** Author       : Gangadhar Kadam                                               ***/
/*** Project      : Medicare Analysis                                             ***/
/*** PIG version  : hive-0.12.0                                                   ***/
/*** HDFS Version : hadoop-2.6.0                                                  ***/
/************************************************************************************/

/************************************************************************************/
/* Create and Laod  PlanInfoCounty external table
/************************************************************************************/

--Create a plan info table;
CREATE EXTERNAL TABLE PLAN_INFO_COUNTY 
  (
  contract_id string,
  plan_id string,
  segment_id string,
  plan_name string,
  CountyFIPSCode string
  ) 
  COMMENT 'Plan Information Table created by pig Output' 
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY',' 
  LINES TERMINATED BY'\n' 
  STORED AS TEXTFILE;
  
--Load the Data
LOAD DATA INPATH '/IBM/DATA/PlanInfoCounty/part-m-*'
INTO TABLE PLAN_INFO_COUNTY;


/************************************************************************************/
/* Create and Load  PlanServices external table
/************************************************************************************/

--Create the Plan services table
CREATE EXTERNAL TABLE PLAN_SERVICES
  (
  Contract_ID string, 
  Plan_ID string, 
  CategoryDescription string, 
  CategoryCode string, 
  Benefit string, 
  package_name string,
  prm_amt string
  )
  COMMENT 'Plan Services Table created by pig Output' 
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY',' 
  LINES TERMINATED BY'\n' 
  STORED AS TEXTFILE;
                  
--Load the Data
LOAD DATA INPATH '/IBM/DATA/PlanServices/part-m-*'
INTO TABLE PLAN_SERVICES;

--Plan with lowest premium
CREATE TABLE ALL_PLANS AS
SELECT 
  t1.contract_ID as contract_id,
  t1.plan_id as plan_id,
  t1.plan_name as plan_name,
  cast(t1.CountyFIPSCode as BIGINT) as county_cd,
  cast(regexp_extract(t2.Benefit,'<[a-z]>[$](.*)</[a-z]>', 1) as FLOAT) as prm_amt
FROM
  PLAN_INFO_COUNTY t1
LEFT OUTER JOIN 
  PLAN_SERVICES t2 
ON 
  t1.contract_ID  = t2.Contract_ID 
  AND t1.plan_id = t2.Plan_ID
WHERE 
  t2.CategoryDescription = 'Monthly Premium  Deductible  and Limits on How Much You Pay for Covered Services'
  AND t2.package_name = 'Base Plan'
  AND regexp_extract(t2.Benefit,'<[a-z]>[$](.*)</[a-z]>', 1)> 0
  AND CountyFIPSCode IS NOT NULL
ORDER BY county_cd, prm_amt ASC;

--Change the data typr;
--ALTER TABLE PLANS_BY_PRM CHANGE CountyFIPSCode CountyFIPSCode BIGINT; 
--ALTER TABLE PLANS_BY_PRM CHANGE prm_amt prm_amt FLOAT;

/************************************************************************************/
/* UseCase#1: Top 5 plan by lowes premium by county                                            */                                                                                  */
/************************************************************************************/
--Rank it by county prmium
CREATE TABLE TOP5_PLANS_BY_LOW_PRM AS
SELECT
  contract_id,
  plan_ID,
  plan_name,
  county_cd,
  prm_amt
FROM
(
SELECT
  contract_id,
  plan_ID,
  plan_name,
  county_cd,
  prm_amt,
  rank() over (PARTITION BY county_cd ORDER BY prm_amt ASC) as RANK 
FROM
  ALL_PLANS
) RNK
WHERE 
  RNK.RANK <=5
  AND county_cd IS NOT NULL AND prm_amt IS NOT NULL 
ORDER BY 
  county_cd,prm_amt ASC;
  
--test query;
  SELECT * FROM TOP5_PLANS_BY_LOW_PRM LIMIT 10;
  
--output;
--H0150	007	Cigna-HealthSpring TotalCare (HMO SNP)	1001	22.6
--H0151	015	UnitedHealthcare Dual Complete (HMO SNP)	1001	26.1
--H0150	024	Cigna-HealthSpring Preferred (HMO)	1001	49.0
--H0151	001	AARP MedicareComplete Plan 1 (HMO)	1001	55.0
--H6609	118	HumanaChoice H6609-118 (PPO)	1001	62.0

--H5322	001	UnitedHealthcare Nursing Home Plan (HMO-POS SNP)	1003	21.8
--H0150	007	Cigna-HealthSpring TotalCare (HMO SNP)	1003	22.6
--H0151	015	UnitedHealthcare Dual Complete (HMO SNP)	1003	26.1
--H2012	070	Humana Gold Plus SNP-DE H2012-070 (HMO SNP)	1003	30.2
--H0150	024	Cigna-HealthSpring Preferred (HMO)	1003	49.0

/************************************************************************************/
/* UseCase#2:-Plan with Highest copays                                                         */
/************************************************************************************/
CREATE TABLE CO_PAY_PLANS AS
SELECT 
  t1.contract_ID as contract_id,
  t1.plan_id as plan_id,
  t1.plan_name as plan_name,
  cast(t1.CountyFIPSCode as BIGINT) as county_cd,
  t2.CategoryDescription,
  cast(regexp_extract(t2.Benefit,'<[a-z]>[$](.*)</[a-z]>', 1) as FLOAT) as copay
FROM
  PLAN_INFO_COUNTY t1
LEFT OUTER JOIN 
  PLAN_SERVICES t2 
ON 
  t1.contract_ID  = t2.Contract_ID 
  AND t1.plan_id = t2.Plan_ID
WHERE 
  t2.CategoryDescription LIKE 'Doc%Office Visits%'
  AND regexp_extract(t2.Benefit,'<[a-z]>[$](.*)</[a-z]>', 1)> 0
  AND CountyFIPSCode IS NOT NULL
ORDER BY county_cd, copay DESC;

--Rank it by county copay
CREATE TABLE HIGH_CO_PAY_PLANS AS
SELECT
  contract_id,
  plan_ID,
  plan_name,
  county_cd,
  copay
FROM
(
SELECT
  A.*,
  rank() over (PARTITION BY A.county_cd ORDER BY A.copay DESC) as RANK 
FROM 
(SELECT distinct contract_id,plan_ID, plan_name,county_cd,copay FROM CO_PAY_PLANS) A
) RNK
WHERE 
  RNK.RANK <= 5
  AND county_cd IS NOT NULL AND copay IS NOT NULL 
ORDER BY 
  county_cd,copay DESC;

--Test Query;
  SELECT * FROM HIGH_CO_PAY_PLANS LIMIT 15;
  
--Output
--H0154	001	VIVA Medicare Plus (HMO)	1001	50.0
--H0154	008	VIVA Medicare Select (HMO)	1001	45.0
--H0151	001	AARP MedicareComplete Plan 1 (HMO)	1001	45.0
--H0150	024	Cigna-HealthSpring Preferred (HMO)	1001	40.0
--H0104	011	Blue Advantage Complete (PPO)	1001	40.0
--H0150	012	Cigna-HealthSpring Advantage (HMO)	1001	40.0

--H0154	001	VIVA Medicare Plus (HMO)	1003	50.0
--H0151	001	AARP MedicareComplete Plan 1 (HMO)	1003	45.0
--H0154	008	VIVA Medicare Select (HMO)	1003	45.0
--H0150	024	Cigna-HealthSpring Preferred (HMO)	1003	40.0
--H0150	012	Cigna-HealthSpring Advantage (HMO)	1003	40.0
--H0104	011	Blue Advantage Complete (PPO)	1003	40.0

/************************************************************************************/
/* UseCase#3:- Plans that provide ambulance service                                             */
/************************************************************************************/
CREATE TABLE AMB_PLANS AS
SELECT 
  t1.contract_ID as contract_id,
  t1.plan_id as plan_id,
  t1.plan_name as plan_name,
  cast(t1.CountyFIPSCode as BIGINT) as county_cd,
  t2.CategoryDescription,
  cast(regexp_extract(t2.Benefit,'<[a-z]>[$](.*)</[a-z]>', 1) as FLOAT) as prm_amt
FROM
  PLAN_INFO_COUNTY t1
LEFT OUTER JOIN 
  PLAN_SERVICES t2 
ON 
  t1.contract_ID  = t2.Contract_ID 
  AND t1.plan_id = t2.Plan_ID
WHERE 
  t2.CategoryDescription LIKE '%mbulance%'
  AND regexp_extract(t2.Benefit,'<[a-z]>[$](.*)</[a-z]>', 1)> 0
  AND CountyFIPSCode IS NOT NULL
ORDER BY county_cd, prm_amt ASC;

--Rank it by county copay
CREATE TABLE AMB_PLANS1 AS
SELECT
  contract_id,
  plan_ID,
  plan_name,
  county_cd,
  prm_amt
FROM
(
SELECT
  A.*,
  rank() over (PARTITION BY A.county_cd ORDER BY A.prm_amt ASC) as RANK 
FROM 
(SELECT distinct contract_id,plan_ID, plan_name,county_cd,prm_amt FROM AMB_PLANS) A
) RNK
WHERE 
  RNK.RANK <= 5
  AND county_cd IS NOT NULL AND prm_amt IS NOT NULL 
ORDER BY 
  county_cd,prm_amt ASC;

--Test Queries;
  SELECT * FROM AMB_PLANS1 LIMIT 13;

--ouput;
--H0104	010	Blue Advantage Premier (PPO)	1001	150.0
--H0154	011	VIVA Medicare Premier (HMO)	1001	175.0
--H0104	011	Blue Advantage Complete (PPO)	1001	200.0
--R5826	065	HumanaChoice R5826-065 (Regional PPO)	1001	200.0
--H0154	008	VIVA Medicare Select (HMO)	1001	200.0

--H0104	010	Blue Advantage Premier (PPO)	1003	150.0
--H0154	011	VIVA Medicare Premier (HMO)	1003	175.0
--H2012	003	Humana Gold Plus H2012-003 (HMO)	1003	200.0
--H0154	008	VIVA Medicare Select (HMO)	1003	200.0
--H0104	011	Blue Advantage Complete (PPO)	1003	200.0
--H2012	051	Humana Gold Plus H2012-051 (HMO)	1003	200.0
--H5525	020	HumanaChoice H5525-020 (PPO)	1003	200.0
--R5826	065	HumanaChoice R5826-065 (Regional PPO)	1003	200.0

/************************************************************************************/
/* UseCase#4:- Plans that provide diabetes service                                              */
/************************************************************************************/
CREATE TABLE DIAB_PLANS AS
SELECT 
  DISTINCT
  t1.contract_ID as contract_id,
  t1.plan_id as plan_id,
  t1.plan_name as plan_name,
  cast(t1.CountyFIPSCode as BIGINT) as county_cd,
  t2.CategoryDescription
FROM
  PLAN_INFO_COUNTY t1
LEFT OUTER JOIN 
  PLAN_SERVICES t2 
ON 
  t1.contract_ID  = t2.Contract_ID 
  AND t1.plan_id = t2.Plan_ID
WHERE 
  (t2.CategoryDescription LIKE '%Diabetes%' OR t2.CategoryDescription LIKE '%diabetes%')
  AND CountyFIPSCode IS NOT NULL
ORDER BY county_cd ASC;
  
--test queries; 
  SELECT * FROM DIAB_PLANS where county_cd=1001 limit 5;
  SELECT * FROM DIAB_PLANS where county_cd=1001 limit 5;

--output
--H0151	001	AARP MedicareComplete Plan 1 (HMO)	1001	Diabetes Supplies and Services
--H0154	012	VIVA Medicare Extra Value (HMO SNP)	1001	Diabetes Supplies and Services
--H0151	025	AARP MedicareComplete Plan 2 (HMO)	1001	Diabetes Supplies and Services
--H0150	007	Cigna-HealthSpring TotalCare (HMO SNP)	1001	Diabetes Supplies and Services
--R5826	001	HumanaChoice R5826-001 (Regional PPO)	1001	Diabetes Supplies and Services

--R5826	065	HumanaChoice R5826-065 (Regional PPO)	1003	Diabetes Supplies and Services
--H0104	010	Blue Advantage Premier (PPO)	1003	Diabetes Supplies and Services
--H0104	011	Blue Advantage Complete (PPO)	1003	Diabetes Supplies and Services
--H0150	007	Cigna-HealthSpring TotalCare (HMO SNP)	1003	Diabetes Supplies and Services
--H0150	012	Cigna-HealthSpring Advantage (HMO)	1003	Diabetes Supplies and Services

/************************************************************************************/
/* UseCase#5:- Plans that provide diabete and mental health service                 */
/************************************************************************************/
CREATE TABLE DIAB_MNTL_PLANS AS
SELECT 
 distinct
  t1.contract_ID as contract_id,
  t1.plan_id as plan_id,
  t1.plan_name as plan_name,
  cast(t1.CountyFIPSCode as BIGINT) as county_cd,
  t2.CategoryDescription
FROM
  PLAN_INFO_COUNTY t1
LEFT OUTER JOIN 
  PLAN_SERVICES t2 
ON 
  t1.contract_ID  = t2.Contract_ID 
  AND t1.plan_id = t2.Plan_ID
WHERE 
  (t2.CategoryDescription LIKE '%Diabetes%' OR t2.CategoryDescription LIKE '%diabetes%'
  OR t2.CategoryDescription LIKE '%Mental%' OR t2.CategoryDescription LIKE '%mental%')
  AND CountyFIPSCode IS NOT NULL
ORDER BY county_cd ASC;

--Test Queries;
SELECT * FROM DIAB_MNTL_PLANS where county_cd=1001 limit 5;
SELECT * FROM DIAB_MNTL_PLANS where county_cd=1001 limit 5;

--Output;
--R5826	065	HumanaChoice R5826-065 (Regional PPO)	1001	Inpatient Mental Health Care
--H0150	007	Cigna-HealthSpring TotalCare (HMO SNP)	1001	Mental Health Care
--H0150	007	Cigna-HealthSpring TotalCare (HMO SNP)	1001	Inpatient Mental Health Care
--H0150	007	Cigna-HealthSpring TotalCare (HMO SNP)	1001	Diabetes Supplies and Services
--H0150	012	Cigna-HealthSpring Advantage (HMO)	1001	Diabetes Supplies and Services

--H0150	012	Cigna-HealthSpring Advantage (HMO)	1003	Mental Health Care
--H2012	003	Humana Gold Plus H2012-003 (HMO)	1003	Diabetes Supplies and Services
--H8145	075	Humana Gold Choice H8145-075 (PFFS)	1003	Mental Health Care
--H8145	075	Humana Gold Choice H8145-075 (PFFS)	1003	Inpatient Mental Health Care
--H8145	075	Humana Gold Choice H8145-075 (PFFS)	1003	Diabetes Supplies and Services
