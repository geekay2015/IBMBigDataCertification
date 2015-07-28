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
  PARTITIONED BY(FILENAME string) 
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY',' 
  LINES TERMINATED BY'\n' 
  STORED AS TEXTFILE;
  
  --Add the partion by file names;
  ALTER TABLE PLAN_INFO_COUNTY ADD PARTITION(filename ='part-m-00000');
  
  --Load the Data
LOAD DATA INPATH "/Users/gk/IBM/output/PlanInfoCounty/part-m-00000"
INTO TABLE PLAN_INFO_COUNTY PARTITION(FILENAME ='part-m-00000');


/************************************************************************************/
/* Create and Load  PlanServices external table
/************************************************************************************/

                  Contract_ID, 
                  Plan_ID, 
                  CategoryDescription, 
                  CategoryCode, 
                  Benefit, 
                  package_name,
