package tech.odes.hudi.spark.mongodb

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HoodieMongoBatchTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("testhive").setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .appName("Spark_mongoDB")
      .enableHiveSupport()
      .getOrCreate()

    val options = Map(
      "spark.mongodb.input.uri" -> "mongodb://172.16.10.72:27017/sang.sang_kong",
      "spark.mongodb.input.partitionerOptions.partitionKey" -> "_id"
    )

    val df = spark.read.format("mongo")
      .options(options)
      .load()

    df.printSchema()
    df.show(false)

    df.write.format("hudi")
      .option("hoodie.datasource.write.recordkey.field", "_id") //设置主键
      .option("hoodie.datasource.write.precombine.field", "_id") //数据更新时间戳
      .option("hoodie.datasource.write.keygenerator.class","org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .option("hoodie.table.name", "sang_collec") //hudi表名
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有 HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局 GLOBAL_BLOOM
      .option("hoodie.table.type", "COPY_ON_WRITE")
      .option(DataSourceWriteOptions.COMMIT_METADATA_KEYPREFIX_OPT_KEY,4)
      .option("hoodie.commits.archival.batch",3)
      .option("path","/duo/sang/sang_collec")
      .mode("append")
      .save()

    spark.close()
  }
}
