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

--Filter the records with null values for the key fields;
PlanInfoCounty4 = FILTER PlanInfoCounty3 BY 
                      (contract_id !='' OR contract_id !='NULL')
                  AND (plan_id != '' OR plan_id != 'NULL')
                  AND (segment_id !='' OR segment_id != 'NULL')
                  AND (CountyFIPSCode != '' OR CountyFIPSCode != 'NULL')
                  AND (plan_name != '' OR plan_name != 'NULL');


