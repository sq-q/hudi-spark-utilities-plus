$SPARK_HOME/bin/spark-submit \
--master local[2] \
--jars $SPARK_HOME/jars/<hudi-spark>.jar \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED \
--conf spark.sql.hive.convertMetastoreParquet=false \
--class tech.odes.hudi.spark.excel.HoodieExcelImporter \
/opt/boxer-excel-<version>.jar \
--resource /tmp/excel/v2readwritetest/simple_excel/test_simple.xlsx \
--props /tmp/excel/hudi-on-excel.properties \
>hoodie-exce-importer.log 2>&1 &