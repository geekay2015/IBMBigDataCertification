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

/************************************************************************************/
/* Create and Load  PlanServices external table
/************************************************************************************/

PlanServices2: {Contract_ID: chararray,Plan_ID: chararray,CategoryDescription: chararray,CategoryCode: chararray,Benefit: chararray,package_name: chararray,prm_amt: chararray}
