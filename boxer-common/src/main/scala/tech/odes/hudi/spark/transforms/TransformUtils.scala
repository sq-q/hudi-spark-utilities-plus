package tech.odes.hudi.spark.transforms

import java.util.UUID

import org.apache.hudi.common.config.TypedProperties
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TransformUtils extends Logging {

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
    logInfo("Registering tmp table : " + tmpTable)
    rowDataframe.registerTempTable(tmpTable)
    val sqlStr = transformerSQL.replaceAll(SRC_PATTERN, tmpTable)
    logDebug("SQL Query for transformation : (" + sqlStr + ")")
    sparkSession.sql(sqlStr)
  }

  /**
   * Automatically level nested json arrays and nested jsons
   *
   * @param df
   * @return
   */
  def flatten(df: DataFrame): DataFrame = {
    // getting all the fields from schema
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    // length shows the number of fields inside dataframe
    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          arrayType.elementType match {
            case _: StructType =>
              val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
              val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
              //val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
              val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
              return flatten(explodedDf)
            case _ =>
          }

        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("$", "_").replace("__", "_").replace(" ", "").replace("-", ""))))
          val explodedf = df.select(renamedcols: _*)
          return flatten(explodedf)
        case _ =>
      }
    }
    df
  }
}
