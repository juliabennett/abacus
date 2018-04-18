package abacus

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import akka.actor.{ActorRef, ActorSystem, Props, Terminated}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import abacus.actors.DgimActor
import abacus.streams.bitcoin.TransactionStream
import abacus.streams.twitter.{HashtagStream, SearchTopic}
import abacus.webserver.WebServer

object Main {

  def main(args: Array[String]): Unit = {

    // Initialize actor system
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val timeout: Timeout = Timeout(5.seconds)

    // Launch hashtag stream
    val trumpKeywords =  List("@realdonaldtrump", "trump")
    val topics: List[SearchTopic] = List(SearchTopic("trump", trumpKeywords))
    val twitterKillSwitch = HashtagStream(topics).run

    // Launch bitcoin stream
    val bitcoinActor = system.actorOf(Props(classOf[DgimActor], "BITCOIN", 1000000L, 25)
      .withDispatcher("prio-dispatcher"))
    TransactionStream(bitcoinActor).run

    // Open webserver
    val dgimActors: Map[String, ActorRef] =
      (("bitcoin", bitcoinActor) :: topics.map(_.toTuple)).toMap // Supports multiple Twitter topics but not currently used
    WebServer(dgimActors).startServer("0.0.0.0", 8080, system)

    // Shutdown once webserver closes (on ENTER for now)
    twitterKillSwitch.shutdown()
    val poolShutdown: Future[Unit] = Http().shutdownAllConnectionPools
    Await.result(poolShutdown, timeout.duration)
    val systemShutdown: Future[Terminated] = system.terminate()
    Await.result(systemShutdown, timeout.duration)

  }
}