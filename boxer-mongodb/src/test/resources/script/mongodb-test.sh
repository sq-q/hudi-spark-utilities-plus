$SPARK_HOME/bin/spark-submit \
--master local[2] \
--executor-memory 4g \
--driver-memory 2g \
--num-executors 4 \
--total-executor-cores 8 \
--jars $SPARK_HOME/jars/hudi-spark3-bundle_2.12-0.9.0.jar,hdfs://ns/test_lib/mongo-spark-connector_2.12-3.0.1.jar,hdfs://ns/test_lib/mongo-java-driver-3.12.10.jar \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED \
--conf spark.sql.hive.convertMetastoreParquet=false \
--class tech.odes.hudi.spark.mongodb.HoodieMongoBatch \
hdfs://ns/test_lib/boxer-mongodb-0.9.0.jar \
--uri mongodb://172.0.0.1:27017/ \
--database database_test \
--collection statsGas \
--props /test_lib/hudi_mongodb_batch.properties \
>mongodb/conf/database_test/statsgas/statsGas.log 2>&1 &
