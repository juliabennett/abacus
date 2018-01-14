package abacus.webserver

import scala.concurrent.Await
import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.{Directives, HttpApp, Route}
import akka.pattern.ask
import akka.util.Timeout
import spray.json._
import abacus.dgim.DgimActor.QueryAll

/**
  * Akka HTTP webserver handling queries against DgimActor.
  *
  * @param dgimActor DgimActor maintaining state
  */
case class WebServer(dgimActor: ActorRef)(implicit timeout: Timeout) extends HttpApp with Directives {

  // Classes handling actor response
  final case class LabelCount(label: String, count: Long)
  final case class Counts(labelCounts: List[LabelCount])

  // Configure JSON marshalling
  import JsonFormatSupport._
  object JsonFormatSupport {
    import DefaultJsonProtocol._
    implicit val labelCountFormat: RootJsonFormat[LabelCount] = jsonFormat2(LabelCount)
    implicit val countsFormat: RootJsonFormat[Counts] = jsonFormat1(Counts)
  }

  // Define single GET route for executing queries
  override def routes: Route =
    path("counts") {
      get {
        parameters('k.as[Long].?) { k =>
          val future = dgimActor ? QueryAll(k)
          val result = Await.result(future, timeout.duration)

          val labelCounts = result.asInstanceOf[List[(String, Long)]]
              .map(tup => LabelCount(tup._1, tup._2))

          complete(Counts(labelCounts))
        }
      }
    }

}