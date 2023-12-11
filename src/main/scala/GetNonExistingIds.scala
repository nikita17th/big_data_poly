import ch.qos.logback.classic.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.{FileInputStream, PrintWriter}
import java.util.Properties
import scala.reflect.io.File

object GetNonExistingIds {
  private val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties()
    if (File("config.properties").exists) {
      properties.load(new FileInputStream("config.properties"))
    } else {
      properties.load(new FileInputStream("src/conf/config.properties"))
    }

    val config: Config = Config.create(properties)
    run(config)
  }

  private def run(config: Config): Unit = {
    val conf = new SparkConf()

    if (config.isLocalMode) {
      conf.setMaster("local[*]")
        .set("spark.driver.host", "127.0.0.1")
        .set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.driver.port", "7070")
        .set("spark.port.maxRetries", "20")
        .set("spark.ui.enabled", "true")
        .set("spark.executor.memory", "8g")
        .set("spark.executor.instances", "1")
        .set("spark.driver.extraJavaOptions", " -XX:+UseG1GC")
        .set("spark.executor.extraJavaOptions", " -XX:+UseG1GC")
    }

    val spark: SparkSession = SparkSession.builder()
      .appName(f"Get ids not in range ${config.nonExistingSourceStartId} to ${config.nonExistingSourceEndId}")
      .config(conf)
      .getOrCreate()
    LOG.info(f"${config.nonExistingSourceDir}")

    val jopa: Array[Boolean] = new Array[Boolean](config.nonExistingSourceEndId - config.nonExistingSourceStartId + 1)
//    val allIds = config.nonExistingSourceStartId to config.nonExistingSourceEndId
//    23:21:45
    spark
      .read
      .parquet(f"${config.nonExistingSourceDir}")
      .select("id")
      .collect()
      .map(_.getInt(0))
      .foreach(id => {
        jopa(id - config.nonExistingSourceStartId) = true
//        LOG.info(f"$id ${jopa(id - config.nonExistingSourceStartId)}")
      })

    val output = new PrintWriter(new java.io.File(f"non-existing_${config.startTime}.txt"))
    var i = 0
    var counter = 0
    while (i < config.nonExistingSourceEndId - config.nonExistingSourceStartId + 1) {
      if (!jopa(i)) {
        counter += 1
        output.println(i + config.nonExistingSourceStartId)
      }
      i += 1
    }
    LOG.error(f"PIZDEZ RAZMEROM S: $counter")
    LOG.error(f"${config.nonExistingSourceEndId - config.nonExistingSourceStartId + 1 - counter}")
  }
}
