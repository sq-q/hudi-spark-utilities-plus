package tech.odes.hudi.spark.batch

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import com.beust.jcommander.internal.Lists
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.Option
import org.apache.hudi.utilities.{IdentitySplitter, UtilHelpers}
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import java.util.{Objects, UUID}

import HoodieESBatch.transform
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.slf4j.LoggerFactory
import tech.odes.hudi.spark.common.Sparker


/**
 * The purpose of this class is to encapsulate a spark application for synchronization of data from ES
 *
 * @author sq
 */
class HoodieESBatch(val cfg: HoodieESBatch.Config,
                    val spark: SparkSession,
                    val conf: Configuration,
                    val props: Option[TypedProperties]) extends Logging {

  private var properties: TypedProperties = null

  init

  def this(
            cfg: HoodieESBatch.Config,
            spark: SparkSession) = this(cfg, spark, spark.sparkContext.hadoopConfiguration, Option.empty())

  def this(
            cfg: HoodieESBatch.Config,
            spark: SparkSession,
            props: Option[TypedProperties]) = this(cfg, spark, spark.sparkContext.hadoopConfiguration, props)

  def this(
            cfg: HoodieESBatch.Config,
            spark: SparkSession,
            conf: Configuration) = this(cfg, spark, conf, Option.empty())

  def init() = {
    // Resolving the properties first in a consistent way
    if (props.isPresent) {
      this.properties = props.get
    } else if (cfg.propsFilePath == Config.DEFAULT_DFS_SOURCE_PROPERTIES) {
      this.properties = UtilHelpers.getConfig(cfg.configs).getConfig
    } else {
      this.properties = UtilHelpers.readConfig(FSUtils.getFs(cfg.propsFilePath,
        spark.sparkContext.hadoopConfiguration),
        new Path(cfg.propsFilePath),
        cfg.configs).getConfig
    }
  }

  def sync() = {
    if (Objects.isNull(this.properties) || this.properties.isEmpty) {
      throw new RuntimeException("table config is missing!")
    }

    // popluate hoodie tables config
    val propertyNames = this.properties.stringPropertyNames
    val TablesConfig = scala.collection.mutable.Map[String, String]()
    propertyNames.asScala.foreach(name => {
      TablesConfig += (name -> this.properties.getString(name))
    })

    val df = spark.read.
      format("es").
      option("es.resource" , cfg.index).
      options(TablesConfig.toMap).load

    df.show()

    if (HoodieESBatch.TRANSFORMER_SQL != null) {
      val tframe = transform(spark, df, properties)
      val frame = flattenDataframedf(tframe)
      frame.write.
        mode("append").
        format("hudi").
        options(TablesConfig.toMap).
        save()
    } else {
      val frame = flattenDataframedf(df)
      frame.write.
        mode("append").
        format("hudi").
        options(TablesConfig.toMap).
        save()
    }
  }

  /**
   * Automatically level nested json arrays and nested jsons
   * @param df
   * @return
   */
  def flattenDataframedf(df: DataFrame): DataFrame = {
    //getting all the fields from schema
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    //length shows the number of fields inside dataframe
    val length = fields.length
    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldName1 = fieldName
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName1)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName1) as $fieldName1")
          //val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName1.*"))
          val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flattenDataframedf(explodedDf)

        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("$", "_").replace("__", "_").replace(" ", "").replace("-", ""))))
          val explodedf = df.select(renamedcols: _*)
          return flattenDataframedf(explodedf)
        case _ =>
      }
    }
    df
  }

  def console() = {
    val df = spark.read.
      format("es").
      load

    df.write.
      format("console").
      option("truncate", "false").
      option("numRows", "100000").
      mode("append").
      save()
  }

  def print = {
    val bannerText = s"""
                        |----------------------------------------------------------------------------------------
                        |
                        |
                        |--  ┌┐ ┬┌┐┌┬  ┌─┐┌─┐  ┬ ┬┌─┐┌─┐┌┬┐┬┌─┐  ┌┬┐┌─┐┬  ┌┬┐┌─┐  ┌─┐┌┬┐┬─┐┌─┐┌─┐┌┬┐┌─┐┬─┐
                        |--  ├┴┐│││││  │ ││ ┬  ├─┤│ ││ │ │││├┤    ││├┤ │   │ ├─┤  └─┐ │ ├┬┘├┤ ├─┤│││├┤ ├┬┘
                        |--  └─┘┴┘└┘┴─┘└─┘└─┘  ┴ ┴└─┘└─┘─┴┘┴└─┘  ─┴┘└─┘┴─┘ ┴ ┴ ┴  └─┘ ┴ ┴└─└─┘┴ ┴┴ ┴└─┘┴└─
                        |
                        |
                        |----------------------------------------------------------------------------------------
                        |[ base ]
                        |index: ${cfg.index}
                        |propsFilePath: ${cfg.propsFilePath}
                        |debug: ${cfg.debug}
                        |help: ${cfg.help}
                        |----------------------------------------------------------------------------------------
                        |[ hoodie ]
                        |${this.properties.asScala.toArray.mkString("\n")}
                        |----------------------------------------------------------------------------------------
                        |""".stripMargin

    logInfo(bannerText)
  }
}

object HoodieESBatch extends Logging {

  private val logger = LoggerFactory.getLogger(classOf[HoodieESBatch])

  private val SRC_PATTERN = "<SRC>"
  private val TMP_TABLE = "HOODIE_SRC_TMP_TABLE_"
  private val TRANSFORMER_SQL = "hoodie.deltastreamer.transformer.sql"

  class Config extends Serializable {

    val DEFAULT_DFS_SOURCE_PROPERTIES: String =
      s"file://${System.getProperty("user.dir")}/src/test/resources/delta-streamer-config/dfs-source.properties"

    @Parameter(names = Array("--index"),description = "elasticsearch resource")
    var index : String = null

    @Parameter(names = Array("--props"),
      description = "path to properties file on localfs or dfs, with configurations for hoodie client, schema provider, " +
        "key generator and data source. For hoodie client props, sane defaults are  used, but recommend use to provide " +
        "basic things like metrics endpoints, hive configs etc. For sources, refer to individual classes, for supported " +
        "properties. Properties in this file can be overridden by \"--hoodie-conf\"")
    var propsFilePath: String = DEFAULT_DFS_SOURCE_PROPERTIES

    @Parameter(names = Array("--hoodie-conf"),
      description = "Any configuration that can be set in the properties file (using the CLI parameter \"--props\") " +
        "can also be passed command line using this parameter. This can be repeated",
      splitter = classOf[IdentitySplitter])
    var configs = Lists.newArrayList[String]()

    @Parameter(names = Array("--es-conf"), splitter = classOf[IdentitySplitter])
    var es_confs = Lists.newArrayList[String]()

    @Parameter(names = Array("--debug"),
      description = "If you set debug mode, binlog synchronization can not work. " +
        "The application will start spark structed console mode to observe es data.")
    var debug: Boolean = false

    @Parameter(names = Array("--help", "-h"), help = true)
    var help: Boolean = false

    override def toString =
      s"""
         |=============================================
         |propsFilePath: $propsFilePath
         |index: $index
         |debug: $debug
         |help: $help
         |configs:
         |${configs.asScala.toArray[String].map(e => "  ".concat(e)).mkString("\n")}
         |=============================================
         |""".stripMargin
  }

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
    if (null == transformerSQL) throw new IllegalArgumentException("Missing configuration : (" + TRANSFORMER_SQL + ")")
    // tmp table name doesn't like dashes
    val tmpTable = TMP_TABLE.concat(UUID.randomUUID.toString.replace("-", "_"))
    logger.info("Registering tmp table : " + tmpTable)
    rowDataframe.registerTempTable(tmpTable)
    val sqlStr = transformerSQL.replaceAll(SRC_PATTERN, tmpTable)
    logger.debug("SQL Query for transformation : (" + sqlStr + ")")
    sparkSession.sql(sqlStr)
  }

  def config(args: Array[String]): Config = {
    val cfg = new Config
    val cmd = new JCommander(cfg, null, args: _*)
    if (cfg.help || args.length == 0) {
      cmd.usage
      System.exit(1)
    }
    cfg
  }

  def appName(config: Config): String = {

    val indexString = config.index.split(",") match {
      case Array(first) => first
      case index@Array(first, _*) => s"${first}-* [${index.length}]"
    }

    s"hoodie-es-batch-${indexString}"

  }

  def main(args: Array[String]): Unit = {
    val cfg = config(args)
    val spark = Sparker.buildSparkSession(appName(cfg), null)
    try {
      val batch = new HoodieESBatch(cfg, spark)
      batch.print
      if (cfg.debug) {
        batch.console()
      } else {
        batch.sync()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
