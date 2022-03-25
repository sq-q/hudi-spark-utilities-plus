package tech.odes.hudi.spark.es

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import com.beust.jcommander.internal.Lists
import java.util.Set
import java.util.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.{Option, StringUtils}
import org.apache.hudi.utilities.{IdentitySplitter, UtilHelpers}
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrameReader, SaveMode, SparkSession}
import scala.collection.JavaConverters._
import tech.odes.hudi.spark.common.Sparker
import tech.odes.hudi.spark.transforms.TransformUtils

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

  def this(cfg: HoodieESBatch.Config,
           spark: SparkSession) = this(cfg, spark, spark.sparkContext.hadoopConfiguration, Option.empty())

  def this(cfg: HoodieESBatch.Config,
           spark: SparkSession,
           props: Option[TypedProperties]) = this(cfg, spark, spark.sparkContext.hadoopConfiguration, props)

  def this(cfg: HoodieESBatch.Config,
           spark: SparkSession,
           conf: Configuration) = this(cfg, spark, conf, Option.empty())

  def init = {
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

  def validate: Unit = {
    if (Objects.isNull(this.properties) || this.properties.isEmpty) {
      throw new RuntimeException("table config is missing!")
    }
    if (Objects.isNull(cfg.resource)) {
      throw new RuntimeException("--resource is required, please fill out resouce")
    }
    if (Objects.isNull(cfg.nodes)) {
      throw new RuntimeException("--nodes is required, please fill out nodes")
    }
    if (Objects.isNull(cfg.port)) {
      throw new RuntimeException("--port is required fields, please fill out port")
    }
  }

  def addExtraOptions(properties: TypedProperties, frameReader: DataFrameReader): Unit = {
    val objects: Set[AnyRef] = properties.keySet
    import scala.collection.JavaConversions._
    for (property <- objects) {
      val prop: String = property.toString
      if (prop.startsWith(cfg.EXTRA_OPTIONS)) {
        val key: String = prop.replace(cfg.EXTRA_OPTIONS, StringUtils.EMPTY_STRING)
        val value: String = properties.getString(prop)
        if (!StringUtils.isNullOrEmpty(value)) {
          logInfo(String.format("Adding %s -> %s to es options", key, value))
          frameReader.option(key, value)
        }
      }
    }
  }

  def sync() = {

    validate

    // popluate hoodie tables config
    val propertyNames = this.properties.stringPropertyNames
    val tableConfig = scala.collection.mutable.Map[String, String]()
    propertyNames.asScala.foreach(name => {
      tableConfig += (name -> this.properties.getString(name))
    })

    val dataFrameReader = spark.read.format("es").
      option(HoodieESBatch.ES_RESOURCE, cfg.resource).
      option(HoodieESBatch.ES_NODES, cfg.nodes).
      option(HoodieESBatch.ES_PORT, cfg.port)

    addExtraOptions(this.properties, dataFrameReader)

    var df = dataFrameReader.load

    // print original schema
    df.printSchema()

    // auto flatten
    if (this.properties.containsKey(HoodieESBatch.ES_AUTO_FLATTEN_ENABLE) &&
        this.properties.getBoolean(HoodieESBatch.ES_AUTO_FLATTEN_ENABLE)){
      df = TransformUtils.flatten(df)
    }

    // transform
    if (this.properties.containsKey(TransformUtils.TRANSFORMER_SQL) &&
        Objects.isNull(this.properties.getString(TransformUtils.TRANSFORMER_SQL))){
      df = TransformUtils.transform(spark, df, this.properties)
    }

    df.write.
      mode(SaveMode.Append).
      format("hudi").
      options(tableConfig.toMap).
      save()
  }

  def console = {
    val dataFrameReader = spark.read.format("es").
      option(HoodieESBatch.ES_RESOURCE, cfg.resource).
      option(HoodieESBatch.ES_NODES, cfg.nodes).
      option(HoodieESBatch.ES_PORT, cfg.port)

    addExtraOptions(this.properties, dataFrameReader)

    val df = dataFrameReader.load

    df.show(10, false)
  }
}

object HoodieESBatch extends Logging {

  /**
   * {@value #ES_RESOURCE} Elasticsearch resource location, where data is read and written to.
   *
   * Requires the format <index>/<type> (relative to the Elasticsearch host/port (see below))).
   */
  private val ES_RESOURCE = "es.resource"

  /**
   * {@value #ES_NODES} List of Elasticsearch nodes to connect to.
   */
  private val ES_NODES = "es.nodes"

  /**
   * {@value #ES_PORT} Default HTTP/REST port used for connecting to Elasticsearch - this setting is applied to the nodes in es.
   *
   * nodes that do not have any port specified.
   */
  private val ES_PORT = "es.port"

  /**
   * {@value #AUTO_FLATTEN_ENABLE} Whether nested json or nested json arrays are automatically expanded, the default is false
   */
  private val ES_AUTO_FLATTEN_ENABLE = "hoodie.deltastreamer.es.auto.flatten.enable"

  class Config extends Serializable {

    val DEFAULT_DFS_SOURCE_PROPERTIES: String =
      s"file://${System.getProperty("user.dir")}/src/test/resources/delta-streamer-config/dfs-source.properties"

    @Parameter(names = Array("--resource"),description = "Elasticsearch resource location, where data is read and written to. " +
      "Requires the format <index>/<type> (relative to the Elasticsearch host/port (see below))).",required = true)
    var resource: String = null

    @Parameter(names = Array("--nodes"),description = "List of Elasticsearch nodes to connect to.",required = true)
    var nodes: String = null

    @Parameter(names = Array("--port"),description = "Default HTTP/REST port used for connecting to Elasticsearch - this setting is applied to the nodes in es." +
      " nodes that do not have any port specified.",required = true)
    var port: String = null

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

    override def toString =
      s"""
         |=============================================
         |propsFilePath: $propsFilePath
         |resource: $resource
         |nodes: $nodes
         |port: $port
         |debug: $debug
         |help: $help
         |configs:
         |${configs.asScala.toArray[String].map(e => "  ".concat(e)).mkString("\n")}
         |=============================================
         |""".stripMargin

    var EXTRA_OPTIONS = "hoodie.deltastreamer.es.extra.options."
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

  def appName(config: Config): String = s"hoodie-es-batch-${config.resource}"

  def main(args: Array[String]): Unit = {
    val cfg = config(args)
    val spark = Sparker.buildSparkSession(appName(cfg), null)
    try {
      val batch = new HoodieESBatch(cfg, spark)
      if (cfg.debug) {
        batch.console
      } else {
        batch.sync()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
