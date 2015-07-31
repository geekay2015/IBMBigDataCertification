export HADOOP_HOME=/usr/local/hadoop/hadoop-2.6.0
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_51.jdk/Contents/Home

DB_CON="jdbc:mysql://127.0.0.1:9000/hadoop_test?useUnicode=true&amp;characterEncoding=utf8"

DB_USERNAME="root"
DB_PASSWORD="root"

DB_TABLE_NAME="[]"
DB_COLUMNS="COL1, COL2, COL3, COL4"

EXPORT_DIR="/user/hadoop/result_rc/keyword-r-00000"
EXPORT_DIR2="/tmp/temp_org"

##### sqoop Query #####
/home/hadoop/sqoop/bin/sqoop export \
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
