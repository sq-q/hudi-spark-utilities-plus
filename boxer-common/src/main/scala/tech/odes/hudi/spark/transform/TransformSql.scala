package tech.odes.hudi.spark.transform

import java.util.UUID

import org.apache.hudi.common.config.TypedProperties
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import tech.odes.hudi.spark.es.HoodieESBatch

object TransformSql {
  private val logger = LoggerFactory.getLogger(classOf[HoodieESBatch])

  private val SRC_PATTERN = "<SRC>"
  private val TMP_TABLE = "HOODIE_SRC_TMP_TABLE_"
  val TRANSFORMER_SQL = "hoodie.deltastreamer.transformer.sql"
  /**
   * A transformer that allows a sql-query template be used to transform the source before writing to Hudi data-set.
   * The query should reference the source as a table named "\<SRC\>"
   *
   * @param sparkSession
   * @param rowDataframe
   * @param properties
   * @return
   */
  def transform(sparkSession: SparkSession, rowDataframe: DataFrame, properties: TypedProperties) = {
    val transformerSQL = properties.getString(TRANSFORMER_SQL)
    if (null == transformerSQL){
      throw new IllegalArgumentException("Missing configuration : (" + TRANSFORMER_SQL + ")")
    }
    // tmp table name doesn't like dashes
    val tmpTable = TMP_TABLE.concat(UUID.randomUUID.toString.replace("-", "_"))
    logger.info("Registering tmp table : " + tmpTable)
    rowDataframe.registerTempTable(tmpTable)
    val sqlStr = transformerSQL.replaceAll(SRC_PATTERN, tmpTable)
    logger.debug("SQL Query for transformation : (" + sqlStr + ")")
    sparkSession.sql(sqlStr)
  }
}
