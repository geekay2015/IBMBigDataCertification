/*Medicare.pig*/

--Register the Jar file;
REGISTER /Users/gk/Downloads/piggybank-0.12.0.jar;

--Load the plan info file PlanInfoCounty_FipsCodeLessThan30000.csv;
PlanInfoCounty1 = LOAD '/Users/gk/Downloads/PlanInfoCounty_FipsCodeLessThan30000.csv' 
                  USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS 
                  (
                  contract_id : chararray, 
                  plan_id : chararray,  
                  segment_id : chararray, 
                  contract_year : chararray, 
                  org_name : chararray, 
                  plan_name : chararray, 
                  sp_plan_name : chararray, 
                  geo_name : chararray, 
                  tax_stus_cd : chararray, 
                  tax_status_desc : chararray, 
                  sp_tax_status_desc : chararray, 
                  plan_type : chararray, 
                  plan_type_desc : chararray, 
                  web_address : chararray, 
                  partd_wb_adr : chararray, 
                  frmlry_wbst_adr : chararray, 
                  phrmcy_wbst_adr : chararray, 
                  fed_approval_status : chararray, 
                  sp_fed_approval_status : chararray, 
                  pos_available_flag : chararray, 
                  mail_ordr_avlblty : chararray, 
                  cvrg_gap_ofrd : chararray, 
                  cvrg_gap_ind : chararray, 
                  cvrg_gap_desc : chararray, 
                  contract_important_note : chararray, 
                  sp_contract_important_note : chararray, 
                  plan_important_note : chararray, 
                  sp_plan_important_note : chararray, 
                  segment_important_note : chararray, 
                  sp_segment_important_note : chararray, 
                  legal_entity_name : chararray, 
                  trade_name : chararray, 
                  network_english : chararray, 
                  network_spanish : chararray, 
                  contact_person : chararray, 
                  street_address : chararray, 
                  city : chararray, 
                  state_code : chararray, 
                  zip_code : chararray, 
                  email_prospective : chararray, 
                  local_phone_prospective : chararray, 
                  tollfree_phone_prospective : chararray, 
                  local_tty_prospective : chararray, 
                  tollfree_tty_prospective : chararray, 
                  email_current : chararray, 
                  local_phone_current : chararray, 
                  tollfree_phone_current : chararray, 
                  local_tty_current : chararray, 
                  tollfree_tty_current : chararray, 
                  contact_person_pd : chararray, 
                  street_address_pd : chararray, 
                  city_pd : chararray, 
                  state_code_pd : chararray, 
                  zip_code_pd : chararray, 
                  email_prospective_pd : chararray, 
                  local_phone_prospective_pd : chararray, 
                  tollfree_phone_prospective_pd : chararray, 
                  local_tty_prospective_pd : chararray, 
                  tollfree_tty_prospective_pd : chararray, 
                  email_current_pd : chararray, 
                  local_phone_current_pd : chararray, 
                  tollfree_phone_current_pd : chararray, 
                  local_tty_current_pd : chararray, 
                  tollfree_tty_current_pd : chararray, 
                  ma_pd_indicator : chararray, 
                  ppo_pd_indicator : chararray, 
                  snp_id : chararray, 
                  snp_desc : chararray, 
                  sp_snp_desc : chararray, 
                  lis_100 : chararray, 
                  lis_75 : chararray, 
                  lis_50 : chararray, 
                  lis_25 : chararray, 
                  regional_indicator : chararray, 
                  CountyFIPSCode : chararray
                  );

--Verify the Relation;
DESCRIBE PlanInfoCounty1;

--Load the Plan info file PlanInfoCounty_FipsCodeMoreThan30000.csv;
PlanInfoCounty2 = LOAD '/Users/gk/Downloads/PlanInfoCounty_FipsCodeMoreThan30000.csv' 
                  USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS 
                  (
                  contract_id : chararray, 
                  plan_id : chararray,  
                  segment_id : chararray, 
                  contract_year : chararray, 
                  org_name : chararray, 
                  plan_name : chararray, 
                  sp_plan_name : chararray, 
                  geo_name : chararray, 
                  tax_stus_cd : chararray, 
                  tax_status_desc : chararray, 
                  sp_tax_status_desc : chararray, 
                  plan_type : chararray, 
                  plan_type_desc : chararray, 
                  web_address : chararray, 
                  partd_wb_adr : chararray, 
                  frmlry_wbst_adr : chararray, 
                  phrmcy_wbst_adr : chararray, 
                  fed_approval_status : chararray, 
                  sp_fed_approval_status : chararray, 
                  pos_available_flag : chararray, 
                  mail_ordr_avlblty : chararray, 
                  cvrg_gap_ofrd : chararray, 
                  cvrg_gap_ind : chararray, 
                  cvrg_gap_desc : chararray, 
                  contract_important_note : chararray, 
                  sp_contract_important_note : chararray, 
                  plan_important_note : chararray, 
                  sp_plan_important_note : chararray, 
                  segment_important_note : chararray, 
                  sp_segment_important_note : chararray, 
                  legal_entity_name : chararray, 
                  trade_name : chararray, 
                  network_english : chararray, 
                  network_spanish : chararray, 
                  contact_person : chararray, 
                  street_address : chararray, 
                  city : chararray, 
                  state_code : chararray, 
                  zip_code : chararray, 
                  email_prospective : chararray, 
                  local_phone_prospective : chararray, 
                  tollfree_phone_prospective : chararray, 
                  local_tty_prospective : chararray, 
                  tollfree_tty_prospective : chararray, 
                  email_current : chararray, 
                  local_phone_current : chararray, 
                  tollfree_phone_current : chararray, 
                  local_tty_current : chararray, 
                  tollfree_tty_current : chararray, 
                  contact_person_pd : chararray, 
                  street_address_pd : chararray, 
                  city_pd : chararray, 
                  state_code_pd : chararray, 
                  zip_code_pd : chararray, 
                  email_prospective_pd : chararray, 
                  local_phone_prospective_pd : chararray, 
                  tollfree_phone_prospective_pd : chararray, 
                  local_tty_prospective_pd : chararray, 
                  tollfree_tty_prospective_pd : chararray, 
                  email_current_pd : chararray, 
                  local_phone_current_pd : chararray, 
                  tollfree_phone_current_pd : chararray, 
                  local_tty_current_pd : chararray, 
                  tollfree_tty_current_pd : chararray, 
                  ma_pd_indicator : chararray, 
                  ppo_pd_indicator : chararray, 
                  snp_id : chararray, 
                  snp_desc : chararray, 
                  sp_snp_desc : chararray, 
                  lis_100 : chararray, 
                  lis_75 : chararray, 
                  lis_50 : chararray, 
                  lis_25 : chararray, 
                  regional_indicator : chararray, 
                  CountyFIPSCode : chararray
                  );

--Verify the relation;
DESCRIBE PlanInfoCounty2;

--Append the file;
PlanInfoCounty3 = UNION PlanInfoCounty1, PlanInfoCounty2;

--Verify the unioned relation;
DESCRIBE PlanInfoCounty3;

--Verfiy the counts in all three relation;
GRP1= GROUP PlanInfoCounty1 ALL;
GRP1_CNT = FOREACH GRP1 GENERATE COUNT($1);
DUMP GRP1_CNT; /*Output 586076*/

GRP2= GROUP PlanInfoCounty2 ALL;
GRP2_CNT = FOREACH GRP2 GENERATE COUNT($1);
DUMP GRP2_CNT; /*Output 596815*/

GRP3= GROUP PlanInfoCounty3 ALL;
GRP3_CNT = FOREACH GRP3 GENERATE COUNT($1);
DUMP GRP3_CNT; /*Output 1182891*/


--Filter the records with null values for the key fields;
PlanInfoCounty4 = FILTER PlanInfoCounty3 BY 
                      (contract_id !='' OR contract_id !='NULL')
                  AND (plan_id != '' OR plan_id != 'NULL')
                  AND (segment_id !='' OR segment_id != 'NULL')
                  AND (CountyFIPSCode != '' OR CountyFIPSCode != 'NULL')
                  AND (plan_name != '' OR plan_name != 'NULL');

--Verify the filter count;
GRP4= GROUP PlanInfoCounty3 ALL;
GRP4_CNT = FOREACH GRP4 GENERATE COUNT($1);
DUMP GRP4_CNT; /*Output 1182891*/

