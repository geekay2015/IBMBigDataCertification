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
CREATE TABLE PLANS_BY_PRM AS
  SELECT 
    t1.Contract_ID,
    t1.Plan_ID,
    t2.plan_name,
    t2.CountyFIPSCode,
    t1.CategoryCode,
    t1.CategoryDescription,
    t1.prm_amt
  FROM 
    PLAN_SERVICES t1 INNER JOIN PLAN_INFO_COUNTY t2
  ON 
    t1.Contract_ID = t2.contract_ID AND t1.Plan_ID = t2.plan_id
  WHERE 
    t1.Benefit LIKE '%remium%' AND regexp_extract(t1.Benefit,'<[a-z]>[$](.*)</[a-z]>', 1)> 0;

--Change the data typr;
ALTER TABLE PLANS_BY_PRM CHANGE CountyFIPSCode CountyFIPSCode BIGINT; 
ALTER TABLE PLANS_BY_PRM CHANGE prm_amt prm_amt FLOAT;

--Order the data by premium;
CREATE TABLE LOW_PRM_PLNS AS
SELECT
  Contract_ID,
  Plan_ID,
  plan_name,
  CountyFIPSCode,
  prm_amt
FROM 
  PLANS_BY_PRM
WHERE
  CountyFIPSCode IS NOT NULL
ORDER BY countyfipscode, prm_amt ASC;
