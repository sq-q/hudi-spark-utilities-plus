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
      "spark.mongodb.input.uri" -> "mongodb://172.0.0.1:27017/test.testcollection",
      "spark.mongodb.input.partitionerOptions.partitionKey" -> "_id"
    )

    val df = spark.read.format("mongo")
      .options(options)
      .load()

    df.printSchema()
    df.show(false)

    df.write.format("hudi")
      .option("hoodie.datasource.write.recordkey.field", "_id")
      .option("hoodie.datasource.write.precombine.field", "_id")
      .option("hoodie.datasource.write.keygenerator.class","org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .option("hoodie.table.name", "testcollection") //hudi表名
      .option("hoodie.table.type", "COPY_ON_WRITE")
      .option("hoodie.commits.archival.batch",3)
      .option("hoodie.bulkinsert.shuffle.parallelism", "12")
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .option("hoodie.delete.shuffle.parallelism", "12")
      .option("hoodie.bootstrap.parallelism", "12")
      .option("path","/duo/test/testcollection")
      .mode("append")
      .save()

    spark.close()
  }
}
