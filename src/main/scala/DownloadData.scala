import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import spray.json._

import java.io.FileInputStream
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.reflect.io.File
import scala.util.{Failure, Success, Try}

object DownloadData {
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
    if (config.vkTokens.size > conf.getInt("spark.executor.instances", 1)) {
      conf.set("spark.executor.instances", config.vkTokens.size.toString)
    }


    val spark: SparkSession = SparkSession.builder()
      .appName("Download users data from vk")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext


    val ids = config.startId to config.finishId
    val groupedIds = ids.grouped(config.batchSizeForRequestVkIds).toSeq
    val gropedBatches = groupedIds.grouped(config.batchSize / config.batchSizeForRequestVkIds).toSeq

    var counter = 0
    for (batch <- gropedBatches) {
      val rdd: RDD[UserData] = sc.makeRDD(batch, numSlices = config.vkTokens.size)
        .mapPartitionsWithIndex { (partitionIndex, part) =>

          val httpWorker = new HttpWorker(config.requestTimeout,
            config.maxRetries,
            config.rateLimit,
            config.rateLimitDuration)

          part
            .flatMap(ids => downloadUserData(ids,
              config.vkTokens(partitionIndex),
              httpWorker,
              config.apiVersion)
            )
        }

      spark.createDataFrame(rdd)
        .write
        .parquet(f"${config.batchDir}" +
          f"/data_batch_${config.startId + counter * config.batchSize}" +
          f"_${math.min(config.startId + (counter + 1) * config.batchSize, config.finishId)}" +
          f"_${System.currentTimeMillis()}")

      counter += 1
      LOG.info(f"Processed: ${math.min(config.startId + counter * config.batchSize, config.finishId)} ids")
    }

    spark
      .read
      .parquet(f"${config.batchDir}/data_batch_*_*_*")
      .coalesce(1)
      .write
      .parquet(config.outPath)


    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val filesToDelete = fs.globStatus(new Path(f"${config.batchDir}"))

    for (file <- filesToDelete) {
      fs.delete(file.getPath, true)
      println(s"Deleted file: ${file.getPath}")
    }


    LOG.info(f"Finish all: ${config.finishId} ids")
    spark.stop()
  }


  private def downloadUserData(batchIds: Seq[Int],
                               key: String,
                               httpWorker: HttpWorker,
                               apiVersion: String
                              ): List[UserData] = {
    val strIds = batchIds.mkString(",")
    val header: HttpHeader = RawHeader("Authorization", key)
    val url = s"https://api.vk.com/method/execute.getUsersData?v=$apiVersion&user_ids=$strIds&fields=last_seen,city,country"

    val responseBody: Future[String] = httpWorker.makeRequest(url, header)
    import UserDataJsonProtocol._

    val resultFuture: Future[UserDataResponse] =
      responseBody.map { result =>
        result.parseJson.convertTo[UserDataResponse]
      }.recover { case ex: Throwable =>
        LOG.error(s"Request failed: ${ex.getMessage}")
        UserDataResponse(UserDataList(Seq.empty, Seq.empty), Seq.empty)
      }


    Try(Await.result(resultFuture, httpWorker.requestTimeout * httpWorker.maxRetries)) match {
      case Success(userDataResponse) =>
        val userPhotosMap: Map[Int, Seq[Int]] = userDataResponse
          .response
          .photos_dates
          .map(userPhotos => userPhotos.user_id -> userPhotos.photos)
          .toMap

        userDataResponse.response.users.flatMap { user =>
          for {
            city <- Option(user.city.map(c => City(c.id, None)))
            country <- Option(user.country.map(c => Country(c.id, None)))
            firstName <- Option(Option(user.first_name.getOrElse("")).filterNot(_.isEmpty).filterNot(_ == "DELETED"))
            lastName <- Option(Option(user.last_name.getOrElse("")).filterNot(_.isEmpty).filterNot(_ == "DELETED"))
          } yield {
            UserData(
              user.id,
              city,
              country,
              firstName,
              lastName,
              user.last_seen,
              user.can_access_closed,
              user.is_closed,
              userPhotosMap.get(user.id),
              user.deactivated
            )
          }
        }.toList
      case Failure(ex) =>
        LOG.error("Failed to fetch user data", ex)
        List.empty
    }
  }

}
