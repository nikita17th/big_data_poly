import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.LoggerFactory

import java.io.FileInputStream
import java.util.Properties
import scala.language.postfixOps
import scala.reflect.io.File

object Aggregate {
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
      .appName("Aggregate data to one file")
      .config(conf)
      .getOrCreate()

    spark
      .read
      .parquet(f"${config.aggregationSourceDir}")
      .filter("id < 837015103")
      .coalesce(32)
      .dropDuplicates("id")
      .write
      .parquet(f"data_0_0_${config.startTime}")
  }
}
