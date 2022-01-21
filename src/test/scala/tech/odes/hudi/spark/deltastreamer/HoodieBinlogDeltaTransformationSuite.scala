package tech.odes.hudi.spark.deltastreamer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object HoodieBinlogDeltaTransformationSuite {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("delta-streamer-*")
      .getOrCreate()

    val df = spark.readStream.
      format("mysql-binlog").
      option("host", "172.16.2.120").
      option("port", "3306").
      option("userName", "root").
      option("password", "123456").
      option("databaseNamePattern", "db_issue_clear").
      option("tableNamePattern", "tb_refund_payment_method_his").
      option("bingLogNamePrefix", "mysql-bin").
      option("binlogIndex", "1").
      option("binlogFileOffset", "275366").
      load()

    df.writeStream.
      outputMode("append").
      format("binlog-hudi").
      option("path", "").
      option("mode", "Append").
      option("option.hoodie.path", "/hudi/tmp/a/{db}/ods_{db}_{table}").
      option("checkpointLocation", "/hudi/tmp/spark-cdc-hudi-sync-suite/").
      // tb_refund_payment_method_his partition
      option("db_issue_clear.tb_refund_payment_method_his.hoodie.base.path", "/hudi/tmp/db_issue_clear/tb_refund_payment_method_his").
      option("db_issue_clear.tb_refund_payment_method_his.hoodie.table.name", "ods_db_issue_clear_tb_refund_payment_method_his").
      option("db_issue_clear.tb_refund_payment_method_his.hoodie.datasource.write.recordkey.field", "id").
      option("db_issue_clear.tb_refund_payment_method_his.hoodie.datasource.write.precombine.field", "id").
      option("db_issue_clear.tb_refund_payment_method_his.hoodie.datasource.write.partitionpath.field", "dt").
      option("db_issue_clear.tb_refund_payment_method_his.hoodie.transformer.sql",
        "SELECT *, nvl(cast(to_date(create_date_time) as string), '1970-01-01') as dt FROM <SRC>").
      trigger(Trigger.ProcessingTime("10 seconds")).
      start().
      awaitTermination()
  }

}
