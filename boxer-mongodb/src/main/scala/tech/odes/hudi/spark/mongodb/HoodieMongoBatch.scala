package tech.odes.hudi.spark.mongodb

import java.util
import java.util.Objects

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.{Option, StringUtils}
import org.apache.hudi.utilities.{IdentitySplitter, UtilHelpers}
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}
import com.beust.jcommander.{JCommander, Parameter}
import com.beust.jcommander.internal.Lists
import org.apache.log4j.LogManager
import tech.odes.hudi.spark.common.Sparker
import tech.odes.hudi.spark.mongodb.HoodieMongoBatch.logger
import tech.odes.hudi.spark.transforms.TransformUtils

import scala.collection.JavaConverters._


/**
 *The purpose of this class is to encapsulate a spark application for batch reading mongodb data to a spark application in Hoodie.
 *
 * @author bl
 */
class HoodieMongoBatch(val cfg: HoodieMongoBatch.Config,
                         val spark: SparkSession,
                         val conf: Configuration,
                         val props: Option[TypedProperties]) extends Logging {

  private var properties: TypedProperties = null

  init

  def this(
            cfg: HoodieMongoBatch.Config,
            spark: SparkSession) = this(cfg, spark, spark.sparkContext.hadoopConfiguration, Option.empty())

  def this(
            cfg: HoodieMongoBatch.Config,
            spark: SparkSession,
            props: Option[TypedProperties]) = this(cfg, spark, spark.sparkContext.hadoopConfiguration, props)

  def this(
            cfg: HoodieMongoBatch.Config,
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

  def validate() = {
    if (Objects.isNull(this.properties) || this.properties.isEmpty) {
      throw new RuntimeException("table config is missing!")
    }

    if (Objects.isNull(cfg.uri) || cfg.uri.isEmpty) {
      throw new RuntimeException("Nodes and port are required fields, please enter uri")
    }

    if (Objects.isNull(cfg.database) || cfg.database.isEmpty) {
      throw new RuntimeException("Database are required fields, please enter database")
    }

    if (Objects.isNull(cfg.collection) || cfg.collection.isEmpty) {
      throw new RuntimeException("Collection are required fields, please enter collection")
    }
  }

  def mongoOptions(properties: TypedProperties, dataFrameReader: DataFrameReader): Unit = {
    val objects: util.Set[AnyRef] = properties.keySet
    import scala.collection.JavaConversions._
    for (property <- objects) {
      val prop: String = property.toString
      if (prop.startsWith(cfg.EXTRA_OPTIONS)) {
        val key: String = prop.replace(cfg.EXTRA_OPTIONS,"")
        val value: String = properties.getString(prop)
        if (!StringUtils.isNullOrEmpty(value)) {
          logger.info(String.format("Adding %s -> %s to es options", key, value))
          dataFrameReader.option(key, value)
        }
      }
    }
  }

  def sync() = {
    val propertyNames = this.properties.stringPropertyNames
    val TablesConfig = scala.collection.mutable.Map[String, String]()
    propertyNames.asScala.foreach(name => {
      TablesConfig += (name -> this.properties.getString(name))
    })

    val dataFrameReader = spark.read.format("mongo").
      option(HoodieMongoBatch.URI, cfg.database).
      option(HoodieMongoBatch.DATABASE, cfg.database).
      option(HoodieMongoBatch.COLLECTION, cfg.database)

    mongoOptions(properties, dataFrameReader)
    var df = dataFrameReader.load()

    if(properties.containsKey(HoodieMongoBatch.FALLY_FLATTENED) &&
      properties.getBoolean(HoodieMongoBatch.FALLY_FLATTENED)) {
      df = TransformUtils.flatten(df)
    }

    if(properties.containsKey(TransformUtils.TRANSFORMER_SQL) &&
      Objects.isNull(this.properties.getString(TransformUtils.TRANSFORMER_SQL))) {
      df = TransformUtils.transform(spark, df, properties)
    }

    df.write.format("hudi").
      mode(SaveMode.Append).
      options(TablesConfig.toMap).
      save()
  }

  def console() = {
    val df = spark.read.
      format("mongo").
      option(HoodieMongoBatch.URI,cfg.database).
      option(HoodieMongoBatch.DATABASE,cfg.database).
      option(HoodieMongoBatch.COLLECTION,cfg.database).
      load
    df.show(10,false)
  }
}
object HoodieMongoBatch extends Logging {
  private val logger = LogManager.getLogger(classOf[HoodieMongoBatch])

  /**
   * {@value #URI} The connection string in the form mongodb://host:port/.
   *
   * The host can be a hostname.It uses the default MongoDB port, 27017.
   */
  private val URI = "uri"

  /**
   * {@value #DATABASE} The database name to read data from.
   */
  private val DATABASE = "database"

  /**
   * {@value #COLLECTION} The collection name to read data from.
   */
  private val COLLECTION = "collection"

  /**
   * {@value #FALLY_FLATTENED}Whether the nested json or nested json array is fully flattened, defaults to false
   */
  private val FALLY_FLATTENED = "hoodie.deltastreamer.mongodb.fally.flattened.enable";

  class Config extends Serializable {
    val DEFAULT_DFS_SOURCE_PROPERTIES: String =
      s"file://${System.getProperty("user.dir")}/src/test/resources/delta-streamer-config/dfs-source.properties"

    @Parameter(names = Array("--uri"),
      description = "Mongodb uri",
      required = true)
    var uri: String = null

    @Parameter(names = Array("--db"),
      description = "Mongodb database ",
      required = true)
    var database: String = null

    @Parameter(names = Array("--c"),
      description = "Mongodb collection ",
      required = true)
    var collection: String = null

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

    @Parameter(names = Array("--debug"),
      description = "If you set debug mode, binlog synchronization can not work. " +
        "The application will start spark structed console mode to observe es data.")
    var debug: Boolean = false

    @Parameter(names = Array("--help", "-h"), help = true)
    var help: Boolean = false

    var EXTRA_OPTIONS = "hoodie.deltastreamer.mongodb.extra.options."

    override def toString =
      s"""
         |=============================================
         |uri: $uri
         |database: $database
         |collection: $collection
         |propsFilePath: $propsFilePath
         |debug: $debug
         |help: $help
         |configs:
         |${configs.asScala.toArray[String].map(e => "  ".concat(e)).mkString("\n")}
         |=============================================
         |""".stripMargin
  }

  def config(args: Array[String]):Config = {
    val cfg = new Config
    val cmd = new JCommander(cfg, null, args: _*)
    if (cfg.help || args.length == 0) {
      cmd.usage
      System.exit(1)
    }
    cfg
  }

  def appName(config: Config): String = {
    val database = config.database
    val tableString = config.collection
    s"hoodie-${database}-${tableString}"
  }

  def main(args: Array[String]): Unit = {
    val cfg = config(args)
    val spark = Sparker.buildSparkSession(appName(cfg), null)
    try {
      val deltaStreamer = new HoodieMongoBatch(cfg, spark)
      println(deltaStreamer)
      if (cfg.debug) {
        deltaStreamer.console()
      } else {
        deltaStreamer.sync()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}