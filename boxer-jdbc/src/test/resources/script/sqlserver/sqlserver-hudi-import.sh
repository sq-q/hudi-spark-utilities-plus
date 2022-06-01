# sqlserver jdbc driver: https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc/8.2.2.jre8

$SPARK_HOME/bin/spark-submit \
--master local[2] \
--jars $SPARK_HOME/jars/<hudi-spark>.jar,$SPARK_HOME/jars/mssql-jdbc-8.2.2.jre8.jar \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED \
--conf spark.sql.hive.convertMetastoreParquet=false \
--class tech.odes.hudi.spark.jdbc.HoodieJDBCImporter \
/opt/boxer-jdbc-<version>.jar \
--dialect sqlserver \
--table dbo.stu \
--props /tmp/jdbc/sqlserver-hudi-import.properties \
>test.log 2>&1 &
