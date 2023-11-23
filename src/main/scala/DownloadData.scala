import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpHeader, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
import ch.qos.logback.classic.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object DownloadData {
  private val LOG = LoggerFactory.getLogger(getClass)
  private implicit val system: ActorSystem = ActorSystem()
  private implicit val materializer: ActorMaterializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.driver.port", "7070")
      .set("spark.port.maxRetries", "20")
      .set("spark.ui.enabled", "true")
      .set("spark.executor.memory", "8g")
      .set("spark.executor.instances", "1")
      .set("spark.driver.extraJavaOptions", " -XX:+UseG1GC")
      .set("spark.executor.extraJavaOptions", " -XX:+UseG1GC")

    val coresExecutor: Int = conf.getInt(Config.COUNT_CORES_PARAM, Config.COUNT_CORES_DEFAULT)
    conf.set("spark.executor.cores", f"$coresExecutor")


    val spark: SparkSession = SparkSession.builder()
      .appName("Download users data from vk")
      .master("local[*]")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    val startId: Int = conf.getInt(Config.START_ID_PARAM, Config.START_ID_DEFAULT)
    val finishId = conf.getInt(Config.FINISH_ID_PARAM, Config.FINISH_ID_DEFAULT)
    val batchVkIdSize = conf.getInt(Config.BATCH_SIZE_VK_IDS_PARAM, Config.BATCH_SIZE_VK_IDS_DEFAULT)
    val vk_token: String = conf.get(Config.VK_TOKEN_ID_PARAM, Config.VK_TOKEN_ID_DEFAULT)
    val outputPath = conf.get(Config.OUT_PATH_PARAM, Config.OUT_PATH_DEFAULT)
    val batchSize: Int = conf.getInt(Config.BATCH_SIZE_PARAM, Config.CHECKPOINT_INTERVAL_DEFAULT)

    val broadcast: Broadcast[String] = spark.sparkContext.broadcast(vk_token)
    val ids = startId to finishId
    val groupedIds = ids.grouped(batchVkIdSize).toSeq
    val gropedBatches = groupedIds.grouped(batchSize).toSeq


    var counter = 0
    for (batch <- gropedBatches) {
      val rdd: RDD[UserData] = sc.parallelize(batch)
        .flatMap(ids => downloadUserData(ids, broadcast.value))

      spark.createDataFrame(rdd)
        .coalesce(1)
        .write
        .parquet(f"batches/data_batch_${startId + counter * batchVkIdSize}" +
          f"_${math.min(startId + (counter + batchSize) * batchVkIdSize, finishId)}" +
          f"_${System.currentTimeMillis()}")
      counter += batchSize
    }

    spark
      .read
      .parquet(f"batches/data_batch_*_*_*")
      .coalesce(1)
      .write
      .parquet(outputPath)

    spark.stop()
  }

  private val ApiVersion = "5.154"
  private val RequestTimeout = 5.seconds

  private def downloadUserData(batchIds: Seq[Int], key: String): List[UserData] = {
    val http = Http()
    val strIds = batchIds.mkString(",")
    val header: HttpHeader = RawHeader("Authorization", key)
    val url = s"https://api.vk.com/method/execute.getUsersData?v=$ApiVersion&user_ids=$strIds&fields=last_seen,city,country"
    val request = HttpRequest(uri = url).addHeader(header)

    val responseFuture: Future[HttpResponse] = http.singleRequest(request)
    import UserDataJsonProtocol._

    val resultFuture: Future[UserDataResponse] = responseFuture.flatMap { response =>
      if (response.status == StatusCodes.OK) {
        val responseBody: Future[String] = response.entity.toStrict(RequestTimeout).map(_.data.utf8String)
        responseBody.map { result =>
          result.parseJson.convertTo[UserDataResponse]
        }
      } else {
        LOG.error(s"Request failed with status code ${response.status}")
        Future.successful(UserDataResponse(UserDataList(Seq.empty, Seq.empty), Seq.empty))
      }
    }

    Try(Await.result(resultFuture, RequestTimeout)) match {
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


  private object Config {
    val START_ID_PARAM: String = "scan.users.vk.id"
    val START_ID_DEFAULT: Int = 1

    val FINISH_ID_PARAM: String = "scan.users.vk.id"
    val FINISH_ID_DEFAULT: Int = 1000000

    val VK_TOKEN_ID_PARAM: String = "vk.token.id"
    val VK_TOKEN_ID_DEFAULT: String = "Bearer {your_token}"

    val BATCH_SIZE_VK_IDS_PARAM: String = "scan.batch.size.vk.id"
    val BATCH_SIZE_VK_IDS_DEFAULT: Int = 24

    val COUNT_CORES_PARAM: String = "count.cores"
    val COUNT_CORES_DEFAULT: Int = Runtime.getRuntime.availableProcessors()

    val OUT_PATH_PARAM: String = "path.data.users"
    val OUT_PATH_DEFAULT: String = f"data_${START_ID_DEFAULT}_${FINISH_ID_DEFAULT}_${System.currentTimeMillis()}"

    val BATCH_SIZE_PARAM: String = "scan.batch.size";
    val CHECKPOINT_INTERVAL_DEFAULT: Integer = 416

  }

  private def configureLogger(name: String): Unit = {
    val logger = LoggerFactory.getLogger(name).asInstanceOf[Logger]
    logger.setLevel(Level.INFO)
    logger.setAdditive(false)
  }

  Seq(
    "org.spark_project.jetty",
    "org.apache.hadoop",
    "org.apache.spark",
    "io.netty"
  ).foreach(configureLogger)

}