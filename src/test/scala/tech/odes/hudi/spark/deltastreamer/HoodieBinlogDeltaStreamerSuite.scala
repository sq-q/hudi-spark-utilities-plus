package tech.odes.hudi.spark.deltastreamer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object HoodieBinlogDeltaStreamerSuite {

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
      option("tableNamePattern", "person|student").
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
      // table [person]
      option("db_issue_clear.person.hoodie.base.path", "/hudi/tmp/db_issue_clear/ods_db_issue_clear_person").
      option("db_issue_clear.person.hoodie.table.name", "ods_db_issue_clear_person").
      option("db_issue_clear.person.hoodie.datasource.write.recordkey.field", "id").
      option("db_issue_clear.person.hoodie.datasource.write.precombine.field", "id").
      option("db_issue_clear.person.hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator").
      // table [student]
      option("db_issue_clear.student.hoodie.base.path", "/hudi/tmp/db_issue_clear/ods_db_issue_clear_student").
      option("db_issue_clear.student.hoodie.table.name", "ods_db_issue_clear_student").
      option("db_issue_clear.student.hoodie.datasource.write.recordkey.field", "id").
      option("db_issue_clear.student.hoodie.datasource.write.precombine.field", "id").
      option("db_issue_clear.student.hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator").
      trigger(Trigger.ProcessingTime("10 seconds")).
      start().
      awaitTermination()
  }
}
