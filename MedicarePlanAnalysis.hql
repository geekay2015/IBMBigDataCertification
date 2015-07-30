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
  RNK.RANK < 6
  AND county_cd IS NOT NULL AND prm_amt IS NOT NULL 
ORDER BY 
  county_cd,prm_amt;





