export HADOOP_HOME=/usr/local/hadoop/hadoop-2.6.0
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_51.jdk/Contents/Home

DB_CON="jdbc:mysql://127.0.0.1:9000/hadoop_test?useUnicode=true&amp;characterEncoding=utf8"

DB_USERNAME="root"
DB_PASSWORD="root"

DB_TABLE_NAME="[TOP5_PLANS_BY_LOW_PRM]"
DB_COLUMNS="contract_id, plan_ID, plan_name,county_cd,prm_amt"


EXPORT_DIR="/user/hive/warehouse/top5_plans_by_low_prm"
EXPORT_DIR2="/tmp/temp_org"

##### sqoop Query #####
/usr/local/sqoop/bin/sqoop export \
--verbose \
--connect ${DB_CON} \
--username ${DB_USERNAME} \
--password ${DB_PASSWORD} \
--table ${DB_TABLE_NAME} \
--columns "${DB_COLUMNS}" \
--export-dir ${EXPORT_DIR} \
--input-fields-terminated-by "\t" \
--outdir /data/hadoop/sqoop/generated \
--bindir /data/hadoop/sqoop/generated \
-m 10
