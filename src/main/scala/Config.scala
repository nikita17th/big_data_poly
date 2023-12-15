import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class Config(startId: Int,
                  finishId: Int,
                  vkTokens: List[String],
                  isLocalMode: Boolean,
                  batchSize: Int,
                  requestTimeout: FiniteDuration,
                  maxRetries: Int,
                  sourceDir: String,
                  outputDir: String,
                  countPartitions: Int,
                  aggregateFilterId: Int
                 ) extends Serializable {
  val startTime: Long = System.currentTimeMillis()
  val apiVersion: String = "5.199"
  val batchSizeForRequestVkIds: Int = 24
  val rateLimit: Int = 3
  val rateLimitDuration: FiniteDuration = 1.seconds
  val batchDir: String = f"batches_$startTime"
  val outPath: String = f"data_${startId}_${finishId}_$startTime"
}

object Config {
  private val VK_TOKENS_PARAM: String = "download.data.vk.tokens"
  private val START_ID_PARAM: String = "data.users.start.vk.id"
  private val COUNT_ID_PARAM: String = "data.users.count.vk.id"

  private val BATCH_SIZE_PARAM: String = "download.data.batch.size" // It's better to be divisible by 24
  private val BATCH_SIZE_DEFAULT: String = "100008"

  private val LOCAL_MODE_PARAM: String = "is.local.mode"
  private val LOCAL_MODE_DEFAULT: String = "false"

  private val REQUEST_TIMEOUT_PARAM: String = "download.data.request.timeout.in.seconds"
  private val REQUEST_TIMEOUT_DEFAULT: String = "5"

  private val MAX_RETRIES_PARAM: String = "download.data.max.retries"
  private val MAX_RETRIES_DEFAULT: String = "10"

  private val SOURCE_DIR_PARAM: String = "source.dir"
  private val SOURCE_DIR_DEFAULT: String = "data_*_*_*"

  private val OUTPUT_DIR_PARAM: String = "output.dir"

  private val COUNT_PARTITIONS_PARAM = "count.partitions"
  private val COUNT_PARTITIONS_DEFAULT = "32"

  private val AGGREGATE_FILTER_ID_PARAM = "aggregate.filter.id"
  private val AGGREGATE_FILTER_ID_DEFAULT = "837015103"

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
    val sourceDir: String = properties.getProperty(Config.SOURCE_DIR_PARAM, Config.SOURCE_DIR_DEFAULT)
    val outputDir: String = properties.getProperty(Config.OUTPUT_DIR_PARAM, "")
    val countPartitions: Int = properties.getProperty(Config.COUNT_PARTITIONS_PARAM, Config.COUNT_PARTITIONS_DEFAULT).toInt
    val aggregateFilterId: Int = properties.getProperty(Config.AGGREGATE_FILTER_ID_PARAM, Config.AGGREGATE_FILTER_ID_DEFAULT).toInt

    Config(startId,
      finishId,
      vkTokens,
      isLocalMode,
      batchSize,
      requestTimeout,
      maxRetries,
      sourceDir,
      outputDir,
      countPartitions,
      aggregateFilterId
    )

  }

}