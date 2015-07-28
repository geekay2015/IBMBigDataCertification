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
CREATETABLE TOP5_LOW_PRM_PLAN AS
SELECT
  t1.contractid
	.planid
	,A.segmentid
	,A.categorydesc
	,A.categorycode
	,B.plan_name
	,b.countyfipscode
	,regexp_extract(A.benefit,'<[a-z]>[$](.*)</[a-z]>', 1)AS premium
FROMPlanServiceFinal A
INNER JOINPlanInfoFinal B ONA.contractid=B.contract_id
	ANDA.planid=B.plan_id
	ANDA.segmentid=B.segment_id
WHEREA.benefitLIKE'%remium%'
	ANDregexp_extract(A.benefit,'<[a-z]>[$](.*)</[a-z]>', 1)> 0;

