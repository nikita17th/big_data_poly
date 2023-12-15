import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.slf4j.LoggerFactory

import java.io.FileInputStream
import java.util.Properties
import scala.language.postfixOps
import scala.reflect.io.File

object Convert {
  private val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties()
    if (File("config.properties").exists) {
      properties.load(new FileInputStream("config.properties"))
    } else {
      properties.load(new FileInputStream("src/conf/config.properties"))
    }

    val downloadDataConfig: Config = Config.create(properties)
    run(downloadDataConfig)
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
      .appName("Convert users data from vk")
      .config(conf)
      .getOrCreate()
    implicit val encoder: Encoder[UserData] = Encoders.product[UserData]

    val half = 3600 * 24 * 180
    val data = spark
      .read
      .schema(ScalaReflection.schemaFor[UserData].dataType.asInstanceOf[StructType])
      .parquet(config.sourceDir)
      .as[UserData]
      .rdd
      .map(userData => {
        // 0 = active
        // 1 = non-active
        // 2 = bot
        // 3 = deleted
        // 4 = banned
        var user_type = 0
        val currentSeconds = System.currentTimeMillis() / 1000
        var platform: Option[Int] = Option.empty

        if (userData.deactivated.isDefined) {
          if (userData.deactivated.get == "deleted") {
            user_type = 3
          } else {
            user_type = 4
          }
        } else if (userData.photos.isDefined) {
          val photos = userData.photos.get.toList.sorted
          if (photos.size > 3) {
            var count: Double = 0
            for (photo <- photos) {
              if (photo <= photos.head + 3600) {
                count += 1
              }
            }
            if (count / photos.size > 0.7) {
              user_type = 2
            }
          }
        }
        if (user_type == 0 && userData.last_seen.isDefined && currentSeconds - half > userData.last_seen.get.time) {
          user_type = 1
        }
        if (userData.last_seen.isDefined) {
          platform = userData.last_seen.get.platform
        }
        UserDataExtended(userData.id, userData.city, userData.country, userData.first_name, userData.last_name, platform, userData.is_closed, Option(user_type))
      })

    spark.createDataFrame(data)
      .coalesce(config.countPartitions)
      .write
      .parquet(config.outputDir)


    LOG.info(f"Finish all")
    spark.stop()
  }
}
