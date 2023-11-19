import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpHeader, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
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

object DownloadData {
  private val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.driver.port", "7070")
      .set("spark.port.maxRetries", "20")
      .set("spark.ui.enabled", "true")
      .set("spark.executor.memory", "8g")
      .set("spark.executor.instances", "1")


    val coresExecutor: Int = conf.getInt(Config.COUNT_CORES_PARAM, Config.COUNT_CORES_DEFAULT)
    conf.set("spark.executor.cores", f"$coresExecutor")


    val spark: SparkSession = SparkSession.builder()
      .appName("Download users data from vk")
      .master("local[*]")
      .config(conf)
      .getOrCreate()

    val startId: Int = conf.getInt(Config.START_ID_PARAM, Config.START_ID_DEFAULT)
    val finishId = conf.getInt(Config.FINISH_ID_PARAM, Config.FINISH_ID_DEFAULT)
    val batchSize = conf.getInt(Config.BATCH_SIZE_PARAM, Config.BATCH_SIZE_DEFAULT)
    val vk_token: String = conf.get(Config.VK_TOKEN_ID_PARAM, Config.VK_TOKEN_ID_DEFAULT)

    val broadcast: Broadcast[String] = spark.sparkContext.broadcast(vk_token)
    val ids = startId to finishId
    val groupedIds = ids.grouped(batchSize).toSeq

    val rdd: RDD[UserData] = spark.sparkContext
      .parallelize(groupedIds)
      .flatMap(batchIds => {
        implicit val system: ActorSystem = ActorSystem()
        implicit val materializer: ActorMaterializer = ActorMaterializer()

        val http = Http()
        val strIds = batchIds.mkString(",")
        val header: HttpHeader = RawHeader("Authorization", broadcast.value)
        val url = s"https://api.vk.com/method/execute.getUsersData?v=5.154&&user_ids=$strIds&fields=last_seen,city,country"
        val request = HttpRequest(uri = url).addHeader(header)

        val responseFuture: Future[HttpResponse] = http.singleRequest(request)
        import UserDataJsonProtocol._
        val resultFuture: Future[UserDataResponse] = responseFuture.flatMap { response =>
          if (response.status == StatusCodes.OK) {
            val responseBody: Future[String] = response.entity.toStrict(1.seconds).map(_.data.utf8String)
            responseBody.map { result =>
              LOG.info(s"Response Body: $result")
              val userDataResponse = result.parseJson.convertTo[UserDataResponse]
              userDataResponse
            }
          } else {
            LOG.error(s"Request failed with status code ${response.status}")
            Future.successful(UserDataResponse(UserDataList(Seq.empty, Seq.empty), Seq.empty))
          }
        }

        val userDataResponse: UserDataResponse = Await.result(resultFuture, 1.seconds)
        val userPhotosMap: Map[Int, Seq[Int]] =
          userDataResponse
            .response
            .photos_dates
            .map(userPhotos => userPhotos.user_id -> userPhotos.photos)
            .toMap

        val users: List[UserData] =
          userDataResponse
            .response
            .users
            .map(user => UserData(
              user.id,
              if (user.city.isDefined) {
                Some(City(user.city.get.id, None))
              } else {
                None
              },
              if (user.country.isDefined) {
                Some(Country(user.country.get.id, None))
              } else {
                None
              },
              if (user.first_name.isDefined) {
                val v = user.first_name.get
                if (v.isEmpty || v == "DELETED") {
                  None
                } else {
                  Some(v)
                }
              } else {
                None
              },
              if (user.last_name.isDefined) {
                val v = user.last_name.get
                if (v.isEmpty || v == "DELETED") {
                  None
                } else {
                  Some(v)
                }
              } else {
                None
              },
              user.last_seen,
              user.can_access_closed,
              user.is_closed,
              userPhotosMap.get(user.id))
            )
            .toList

        users
      })


    spark.createDataFrame(rdd)
      .coalesce(1)
      .write
      .parquet("gqnms3")

    spark.stop()
  }


  private object Config {
    val START_ID_PARAM: String = "scan.users.vk.id"
    val START_ID_DEFAULT: Int = 1

    val FINISH_ID_PARAM: String = "scan.users.vk.id"
    val FINISH_ID_DEFAULT: Int = 24

    val VK_TOKEN_ID_PARAM: String = "vk.token.id"
    val VK_TOKEN_ID_DEFAULT: String = "Bearer 029fbd2a029fbd2a029fbd2a23018a4aa00029f029fbd2a67b46f7e5929e3d48386dae3"

    val BATCH_SIZE_PARAM: String = "scan.batch.size"
    val BATCH_SIZE_DEFAULT: Int = 24

    val COUNT_CORES_PARAM: String = "count.cores"
    val COUNT_CORES_DEFAULT: Int = Runtime.getRuntime.availableProcessors()

    val OUT_PATH_PARAM: String = "path.data.users"
    val OUT_PATH_DEFAULT: String = "/user/hduser/spark/data_users"

  }
}