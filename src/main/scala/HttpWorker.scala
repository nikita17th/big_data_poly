import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpHeader, HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Flow, RestartFlow, Sink, Source}
import akka.stream.{ActorMaterializer, ThrottleMode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class HttpWorker(val requestTimeout: FiniteDuration,
                 val maxRetries: Int,
                 val rateLimit: Int,
                 val rateLimitDuration: FiniteDuration
                ) {
  implicit val ast: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()(ast)

  def makeRequest(url: String,
                  header: HttpHeader): Future[String] = {
    val request = HttpRequest(uri = url)
      .addHeader(header)

    val throttledSource = Source.single(request)
      .throttle(rateLimit, rateLimitDuration, rateLimit, ThrottleMode.Shaping)

    val responseFlow = RestartFlow.onFailuresWithBackoff(
      minBackoff = requestTimeout,
      maxBackoff = requestTimeout * maxRetries,
      randomFactor = 0.2,
      maxRestarts = maxRetries
    ) { () =>
      Flow[HttpRequest].mapAsync(1)(Http().singleRequest(_))
    }

    val promise = Promise[HttpResponse]()
    throttledSource
      .via(responseFlow)
      .runWith(Sink.head)
      .onComplete(promise.complete)

    val responseFuture = promise.future
    responseFuture.flatMap {
      case HttpResponse(status, _, entity, _) if status.isSuccess() =>
        Unmarshal(entity).to[String]
      case HttpResponse(status, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { error =>
          Future.failed(new Exception(s"Request failed with status $status: $error"))
        }
    }
  }
}

