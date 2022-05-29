$SPARK_HOME/bin/spark-submit \
--master local[2] \
--jars $SPARK_HOME/jars/<hudi-spark>.jar,$SPARK_HOME/jars/ojdbc6.jar \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED \
--conf spark.sql.hive.convertMetastoreParquet=false \
--class tech.odes.hudi.spark.jdbc.HoodieJDBCImporter \
/opt/boxer-jdbc-<version>.jar \
--dialect oracle \
--table direct.cx_bas_blacklist \
--props /tmp/jdbc/oracle-hudi-import.properties \
>test.log 2>&1 &