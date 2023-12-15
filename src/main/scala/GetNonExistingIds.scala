import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.FileInputStream
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
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

    val size = config.finishId - config.startId + 1
    val spark: SparkSession = SparkSession.builder()
      .appName(f"Get ids not in range ${config.startId} to ${config.finishId}")
      .config(conf)
      .getOrCreate()
    val jopa: Array[Boolean] = new Array[Boolean](size)

    spark
      .read
      .parquet(f"${config.sourceDir}")
      .select("id")
      .rdd
      .map(a => a.getInt(0))
      .collect()
      .foreach(id => {
        if (id - config.startId < size) {
          jopa(id - config.startId) = true
        }
      })
    LOG.info("Processing of existing id is ended")

    val collection = ArrayBuffer[Int]()
    var i = 0
    var counter = 0
    while (i < size) {
      if (!jopa(i)) {
        counter += 1
        collection += (i + config.startId)
      }
      if (i % 10000000 == 0) {
        LOG.info(f"Processed id = $i, counter = $counter")
      }
      i += 1
    }

    spark.sparkContext.makeRDD(collection, 1)
      .saveAsTextFile(f"non-existing_${config.startTime}")

    LOG.error(f"Count lost: $counter")
    LOG.error(f"${size - counter}")
  }
}
