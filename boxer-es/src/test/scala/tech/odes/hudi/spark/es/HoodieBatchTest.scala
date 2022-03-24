package tech.odes.hudi.spark.es

import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object HoodieBatchTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(sc).getOrCreate()

    val options = Map(
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "172.16.10.72",
      "es.port" -> "9200",
      "es.resource" -> "es_json"
    )
    val frame = spark.read.format("es").options(options).load()

    frame.createOrReplaceTempView("t1")
    val df = spark.sql("select * from t1 where t1.name = 'Hadoop' limit 1")

    df.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE") //选择表的类型到底是MERGE_ON_READ还是COPY_ON_WRITE
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "credit") //设置主键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "credit") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "") //hudi分区列
      .option("hoodie.table.name", "es_test") //hudi表名
      //        .option(HoodieInde"org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .option("hoodie.bulkinsert.shuffle.parallelism", "12")
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .option("hoodie.delete.shuffle.parallelism", "12")
      .option("hoodie.bootstrap.parallelism", "12")
      .mode(SaveMode.Append)
      .save("/ss/es/es_test" )
  }
}
