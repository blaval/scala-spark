package com.github.blaval.scalaspark.scalaspark.common

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class CheckpointDir(name: String)
case class DirectoryName(name: String)

class AppContext {
  val config: Config = ConfigFactory.load()

  def checkpointDir(arguments: RunnableArgs): String =
    arguments.checkpointDirectory.getOrElse("checkpoint/directory/path")

  def setupAppContext(applicationName: String, arguments: RunnableArgs): SparkSession = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.sql.parquet.compression.codec", "snappy")
    val spark = SparkSession
      .builder()
      .appName(applicationName)
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    val blocksize = 1024 * 1024 * 256 // 256 Mb block size
    spark.sparkContext.hadoopConfiguration.setInt("dfs.blocksize", blocksize)
    spark.sparkContext.hadoopConfiguration.setInt("parquet.block.size", blocksize)
    val path =
      generateCheckpointPath(CheckpointDir(checkpointDir(arguments)), DirectoryName(spark.sparkContext.applicationId))

    spark.sparkContext.setCheckpointDir(path)
    spark.sparkContext.broadcast()
    spark
  }

  def deleteFolder(dir: String): Unit = {
    val hadoopConf = new Configuration()
    val fileSystem = FileSystem.get(hadoopConf)
    val path       = new Path(dir)
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }

  def tearDownContext(spark: SparkSession, arguments: RunnableArgs): Unit = {
    spark.stop()
    val path =
      generateCheckpointPath(CheckpointDir(checkpointDir(arguments)), DirectoryName(spark.sparkContext.applicationId))
    deleteFolder(path)
  }

  def generateCheckpointPath(checkpointDir: CheckpointDir, directoryName: DirectoryName): String = {
    s"hdfs://${checkpointDir.name}/${directoryName.name}"
  }

}
