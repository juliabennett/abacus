package abacus

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import akka.actor.{ActorRef, ActorSystem, Props, Terminated}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import abacus.dgim.DgimActor
import abacus.streams.bitcoin.TransactionStream
import abacus.streams.twitter.{HashtagStream, Topic}
import abacus.webserver.WebServer

object Main {

  def main(args: Array[String]): Unit = {

    // Initialize actor system
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val timeout: Timeout = Timeout(5.seconds)

    // Launch hashtag stream
    val trumpKeywords =  List("@realdonaldtrump", "trump", "#trump")
    val topics: List[Topic] = List(Topic("trump", trumpKeywords))
    val twitterKillSwitch = HashtagStream(topics).run

    // Launch bitcoin stream
    val bitcoinActor = system.actorOf(Props(classOf[DgimActor], 1000000L, 2))
    val bitcoinKillSwitch = TransactionStream(bitcoinActor).run

    // Open webserver
    val dgimActors: Map[String, ActorRef] =
      (("bitcoin", bitcoinActor) :: topics.map(_.toTuple)).toMap // Supports multiple Twitter topics but not currently used
    WebServer(dgimActors).startServer("localhost", 8080, system)

    // Shutdown once webserver closes (on ENTER for now)
    twitterKillSwitch.shutdown()
    bitcoinKillSwitch.shutdown()
    val poolShutdown: Future[Unit] = Http().shutdownAllConnectionPools
    Await.result(poolShutdown, timeout.duration)
    val systemShutdown: Future[Terminated] = system.terminate()
    Await.result(systemShutdown, timeout.duration)

  }
}