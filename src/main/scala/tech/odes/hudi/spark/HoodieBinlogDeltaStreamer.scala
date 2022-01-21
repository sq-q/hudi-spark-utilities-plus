package tech.odes.hudi.spark


import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import com.beust.jcommander.internal.Lists
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.Option
import org.apache.hudi.utilities.{IdentitySplitter, UtilHelpers}
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import scala.collection.JavaConverters._
import java.util.Objects


/**
 * The purpose of this class is to encapsulate a spark application for real-time synchronization of data from MySQL
 * binlog into Hoodie.
 *
 * @author sq
 */
class HoodieBinlogDeltaStreamer(val cfg: HoodieBinlogDeltaStreamer.Config,
                                val spark: SparkSession,
                                val conf: Configuration,
                                val props: Option[TypedProperties]) extends Logging {

  private var properties: TypedProperties = null

  init

  def this(
            cfg: HoodieBinlogDeltaStreamer.Config,
            spark: SparkSession) = this(cfg, spark, spark.sparkContext.hadoopConfiguration, Option.empty())

  def this(
            cfg: HoodieBinlogDeltaStreamer.Config,
            spark: SparkSession,
            props: Option[TypedProperties]) = this(cfg, spark, spark.sparkContext.hadoopConfiguration, props)

  def this(
            cfg: HoodieBinlogDeltaStreamer.Config,
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
      throw new RuntimeException("Hoodie table config is missing!")
    }

    // popluate hoodie tables config
    val propertyNames = this.properties.stringPropertyNames
    val hoodieTablesConfig = scala.collection.mutable.Map[String, String]()
    propertyNames.asScala.foreach(name => {
      hoodieTablesConfig += (name -> this.properties.getString(name))
    })

    val df = spark.readStream.
      format("mysql-binlog").
      options(Map(
        "host" -> cfg.host,
        "port" -> cfg.port,
        "userName" -> cfg.username,
        "password" -> cfg.password,
        "databaseNamePattern" -> cfg.database,
        "tableNamePattern" -> cfg.tables.replaceAll(",", "\\|"),
        "bingLogNamePrefix" -> cfg.binaryLogNamePrefix,
        "binlogIndex" -> cfg.binaryLogIndex,
        "binlogFileOffset" -> cfg.binarylogFileOffset
      )).load

    df.writeStream.
      outputMode("append").
      format("binlog-hudi").
      option("path", StringUtils.EMPTY).
      option("mode", "Append").
      option("option.hoodie.path", cfg.hoodiePathPattern).
      option("checkpointLocation", cfg.checkpoint).
      options(hoodieTablesConfig.toMap).
      trigger(Trigger.ProcessingTime(s"${cfg.triggerTime} seconds")).
      start().
      awaitTermination()
  }

  def console() = {
    val df = spark.readStream.
      format("mysql-binlog").
      options(Map(
        "host" -> cfg.host,
        "port" -> cfg.port,
        "userName" -> cfg.username,
        "password" -> cfg.password,
        "databaseNamePattern" -> cfg.database,
        "tableNamePattern" -> cfg.tables.replaceAll(",", "\\|"),
        "bingLogNamePrefix" -> cfg.binaryLogNamePrefix,
        "binlogIndex" -> cfg.binaryLogIndex,
        "binlogFileOffset" -> cfg.binarylogFileOffset
      )).load

    val query = df.writeStream.
      format("console").
      option("mode", "Append").
      option("truncate", "false").
      option("numRows", "100000").
      option("checkpointLocation", cfg.checkpoint).
      outputMode("append").
      trigger(Trigger.ProcessingTime(s"${cfg.triggerTime} seconds")).
      start()

    query.awaitTermination()
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
                        |host: ${cfg.host}
                        |port: ${cfg.port}
                        |username: ${cfg.username}
                        |password: ${cfg.password}
                        |database: ${cfg.database}
                        |tables: ${cfg.tables}
                        |binaryLogNamePrefix: ${cfg.binaryLogNamePrefix}
                        |binaryLogIndex: ${cfg.binaryLogIndex}
                        |binarylogFileOffset: ${cfg.binarylogFileOffset}
                        |checkpoint: ${cfg.checkpoint}
                        |hoodiePathPattern: ${cfg.hoodiePathPattern}
                        |propsFilePath: ${cfg.propsFilePath}
                        |triggerTime: ${cfg.triggerTime}
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

object HoodieBinlogDeltaStreamer extends Logging {

  class Config extends Serializable {

    val DEFAULT_DFS_SOURCE_PROPERTIES: String =
      s"file://${System.getProperty("user.dir")}/src/test/resources/delta-streamer-config/dfs-source.properties"

    @Parameter(names = Array("--host"),
      description = "MySQL host",
      required = true)
    var host: String = null

    @Parameter(names = Array("--port"),
      description = "MySQL port",
      required = true)
    var port: String = "3306"

    @Parameter(names = Array("--username"),
      description = "MySQL username",
      required = true)
    var username: String = null

    @Parameter(names = Array("--password"),
      description = "MySQL password",
      required = true)
    var password: String = null

    @Parameter(names = Array("--database"),
      description = "MySQL database",
      required = true)
    var database: String = null

    @Parameter(names = Array("--tables"),
      description = "MySQL table, support regular expression",
      required = true)
    var tables: String = null

    @Parameter(names = Array("--binlog-name-prefix"),
      description = "MySQL binlog name prefix, e.g: mysql-bin.xxxx",
      required = true)
    var binaryLogNamePrefix: String = "mysql-bin"

    @Parameter(names = Array("--binlog-index"),
      description = "MySQL binlog index, e.g: mysql-bin.0000004, binlog index is 4",
      required = true)
    var binaryLogIndex: String = null

    @Parameter(names = Array("--binlog-offset"),
      description = "MySQL binlog offset, you can set show master logs in mysql cli to get offset",
      required = true)
    var binarylogFileOffset: String = "4"

    @Parameter(names = Array("--checkpoint"),
      description = "Resume from this checkpoint. it's spark checkpointLocation",
      required = true)
    var checkpoint: String = null

    @Parameter(names = Array("--hoodie-path-pattern"),
      description = "Setting hoodie tables path, support regular expression")
    var hoodiePathPattern: String = null

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

    @Parameter(names = Array("--trigger-time"),
      description = "Spark Structed Streaming Processing trigger-time(s)")
    var triggerTime: String = "60"

    @Parameter(names = Array("--debug"),
      description = "If you set debug mode, binlog synchronization can not work. " +
        "The application will start spark structed streaming console mode to observe binlog.")
    var debug: Boolean = false

    @Parameter(names = Array("--help", "-h"), help = true)
    var help: Boolean = false

    override def toString =
      s"""
         |=============================================
         |host: $host
         |port: $port
         |username: $username
         |password: $password
         |database: $database
         |tables: $tables
         |binaryLogNamePrefix: $binaryLogNamePrefix
         |binaryLogIndex: $binaryLogIndex
         |binarylogFileOffset: $binarylogFileOffset
         |checkpoint: $checkpoint
         |hoodiePathPattern: $hoodiePathPattern
         |propsFilePath: $propsFilePath
         |triggerTime: $triggerTime
         |debug: $debug
         |help: $help
         |configs:
         |${configs.asScala.toArray[String].map(e => "  ".concat(e)).mkString("\n")}
         |=============================================
         |""".stripMargin
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
    val tableString = config.tables.split(",") match {
      case Array(first) => first
      case tables@Array(first, _*) => s"${first}-* [${tables.length}]"
    }
    s"hoodie-binlog-delta-streamer-${config.database}-${tableString}"
  }

  def main(args: Array[String]): Unit = {
    val cfg = config(args)
    val spark = Utils.buildSparkSession(appName(cfg), null)
    try {
      val deltaStreamer = new HoodieBinlogDeltaStreamer(cfg, spark)
      deltaStreamer.print
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


