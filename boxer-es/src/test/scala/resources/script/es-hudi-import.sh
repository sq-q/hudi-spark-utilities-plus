$SPARK_HOME/bin/spark-submit \
--master yarn \
--executor-memory 4g \
--driver-memory 4g \
--num-executors 4 \
--executor-cores 4 \
--jars $SPARK_HOME/jars/<hudi-spark>.jar,$SPARK_HOME/jars/spark-avro_2.12-3.1.1.jar,$SPARK_HOME/jars/elasticsearch-spark-30_2.12-7.12.1.jar \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED \
--conf spark.sql.hive.convertMetastoreParquet=false \
--class tech.odes.hudi.spark.es.HoodieESBatch \
/opt/boxer-es-<version>.jar \
--resource index_clear_outside_trans \
--nodes 172.16.10.72,172.16.10.205,172.16.10.206 \
--port 9200 \
--props /tmp/excel/es-hudi-import.properties \
>hoodie-es-importer.log 2>&1 &
