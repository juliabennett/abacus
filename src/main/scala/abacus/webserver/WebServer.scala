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
  * Akka HTTP webserver handling queries against DgimActors.
  *
  * @param dgimActors Mapping from stream names to DgimActors maintaining state
  */
case class WebServer(dgimActors: Map[String, ActorRef])(implicit timeout: Timeout)
    extends HttpApp with Directives {

  // Classes handling actor response
  case class LabelCount(label: String, count: Long)
  case class Counts(positionsInWindow: Long, labelCounts: List[LabelCount])

  // Configure JSON marshalling
  import JsonFormatSupport._
  object JsonFormatSupport {
    import DefaultJsonProtocol._
    implicit val labelCountFormat: RootJsonFormat[LabelCount] = jsonFormat2(LabelCount)
    implicit val countsFormat: RootJsonFormat[Counts] = jsonFormat2(Counts)
  }

  // Routes
  override def routes: Route = {
    path("") {
      getFromResource("index.html")
    } ~
    path("resources" / Segment) { name =>
      getFromResource(s"$name")
    } ~
    path("counts" / Segment) { name =>
      get {
        parameters('k.as[Long].?, 'topN.as[Int].?) { (k, topN) =>
          println(k)
          println(topN)
          val future = dgimActors(name) ? QueryAll(k, topN)
          val result = Await.result(future, timeout.duration)
            .asInstanceOf[(Long, List[(String, Long)])]

          complete(
            Counts(
              result._1,
              result._2.map(tup => LabelCount(tup._1, tup._2))
            ))
        }
      }
    }
  }

}