import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class Config(startId: Int,
                  finishId: Int,
                  vkTokens: List[String],
                  isLocalMode: Boolean,
                  batchSize: Int,
                  requestTimeout: FiniteDuration,
                  maxRetries: Int
                 ) extends Serializable {
  private val time = System.currentTimeMillis()
  val apiVersion: String = "5.199"
  val batchSizeForRequestVkIds: Int = 24
  val rateLimit: Int = 3
  val rateLimitDuration: FiniteDuration = 1.seconds
  val batchDir: String = f"batches_$time"
  val outPath: String = f"data_${startId}_${finishId}_$time"
}

object Config {
  private val VK_TOKENS_PARAM: String = "download.data.vk.tokens"
  private val START_ID_PARAM: String = "download.data.users.start.vk.id"
  private val COUNT_ID_PARAM: String = "download.data.users.count.vk.id"

  private val BATCH_SIZE_PARAM: String = "download.data.batch.size" // It's better to be divisible by 24
  private val BATCH_SIZE_DEFAULT: String = "100008"

  private val LOCAL_MODE_PARAM: String = "download.data.is.local.mode"
  private val LOCAL_MODE_DEFAULT: String = "false"

  private val REQUEST_TIMEOUT_PARAM: String = "download.data.request.timeout.in.seconds"
  private val REQUEST_TIMEOUT_DEFAULT: String = "5"

  private val MAX_RETRIES_PARAM: String = "download.data.max.retries"
  private val MAX_RETRIES_DEFAULT: String = "10"

  def create(properties: Properties): Config = {
    val startId: Int = properties.getProperty(Config.START_ID_PARAM).toInt
    val finishId = startId + properties.getProperty(Config.COUNT_ID_PARAM).toInt

    val strings: String = properties.getProperty(Config.VK_TOKENS_PARAM, "")
    if (strings.isEmpty) {
      throw new IllegalStateException("You did not specify a single token to access the vk api!")
    }
    val vkTokens = strings.split(":").toList

    val isLocalMode: Boolean = properties.getProperty(Config.LOCAL_MODE_PARAM, Config.LOCAL_MODE_DEFAULT).toBoolean
    val batchSize: Int = properties.getProperty(Config.BATCH_SIZE_PARAM, Config.BATCH_SIZE_DEFAULT).toInt
    val requestTimeout: FiniteDuration = FiniteDuration(
      properties.getProperty(Config.REQUEST_TIMEOUT_PARAM, REQUEST_TIMEOUT_DEFAULT).toLong,
      TimeUnit.SECONDS)
    val maxRetries: Int = properties.getProperty(Config.MAX_RETRIES_PARAM, Config.MAX_RETRIES_DEFAULT).toInt

    Config(startId,
      finishId,
      vkTokens,
      isLocalMode,
      batchSize,
      requestTimeout,
      maxRetries
    )

  }

}