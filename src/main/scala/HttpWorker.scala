import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Flow, RestartFlow, Sink, Source}
import akka.stream.{ActorMaterializer, ThrottleMode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class HttpWorker(val requestTimeout: FiniteDuration,
                 val maxRetries: Int,
                 val rateLimit: Int,
                 val rateLimitDuration: FiniteDuration
                ) {
  implicit val ast: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()(ast)

  def makeRequest(urls: List[String],
                  header: HttpHeader): List[String] = {

    val responseFlow = RestartFlow.onFailuresWithBackoff(
      minBackoff = requestTimeout,
      maxBackoff = requestTimeout * maxRetries,
      randomFactor = 0.2,
      maxRestarts = maxRetries
    ) { () =>
      Flow[HttpRequest]
        .mapAsync(parallelism = 1)(Http().singleRequest(_))
        .mapAsync(parallelism = 1) {
          case HttpResponse(StatusCodes.OK, _, entity, _) =>
            Unmarshal(entity).to[String]
          case HttpResponse(status, _, entity, _) =>
            Unmarshal(entity).to[String].flatMap { error =>
              Future.failed(new Exception(s"Non-OK response: $status, error : $error"))
            }
        }
    }

    val resultFuture = Source(urls)
      .map(u => createRequest(u, header))
      .throttle(rateLimit, rateLimitDuration, rateLimit, ThrottleMode.Shaping)
      .via(responseFlow)
      .runWith(Sink.seq)

    Await.result(resultFuture, Duration.Inf).toList
  }

  private def createRequest(url: String, header: HttpHeader): HttpRequest = {
    HttpRequest(uri = Uri(url))
      .withMethod(HttpMethods.GET)
      .addHeader(header)
  }

}

