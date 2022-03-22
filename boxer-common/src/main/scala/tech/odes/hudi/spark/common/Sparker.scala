package tech.odes.hudi.spark.common

import java.util
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Sparker {
  private def defaultConf = {
    val additionalConfigs = new util.HashMap[String, String]
    additionalConfigs.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    additionalConfigs.put("spark.kryoserializer.buffer.max", "512m")
    additionalConfigs
  }

  def defaultSparkConf(appName: String): SparkConf = buildSparkConf(appName, defaultConf)

  def buildSparkConf(appName: String, additionalConfigs: util.Map[String, String]): SparkConf = {
    val sparkConf = new SparkConf().setAppName(appName)
    additionalConfigs.forEach(sparkConf.set)
    sparkConf
  }

  def defaultSparkSession(appName: String): SparkSession = buildSparkSession(appName, defaultConf)

  def buildSparkSession(appName: String, additionalConfigs: util.Map[String, String]): SparkSession = {
    val builder = SparkSession.builder.appName(appName)
    builder.getOrCreate
  }

}

