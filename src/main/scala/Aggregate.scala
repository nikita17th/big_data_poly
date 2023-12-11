import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import java.io.FileInputStream
import java.util.Properties
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
    if (config.vkTokens.size > conf.getInt("spark.executor.instances", 1)) {
      conf.set("spark.executor.instances", config.vkTokens.size.toString)
    }

    val spark: SparkSession = SparkSession.builder()
      .appName("Aggregate data to one file")
      .config(conf)
      .getOrCreate()

    spark
      .read
      .parquet(f"${config.aggregationSourceDir}")
      .sort("id")
      .repartition(32)
      .dropDuplicates("id")
      .write
      .parquet(f"data_0_0_${config.startTime}")
  }
}
