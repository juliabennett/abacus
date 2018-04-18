package abacus.streams.bitcoin

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.math._
import akka.actor.{ActorRef, ActorSystem}
import akka.Done
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.Http
import akka.stream._
import akka.stream.scaladsl._
import org.slf4j
import org.slf4j.LoggerFactory
import spray.json._
import spray.json.DefaultJsonProtocol._
import abacus.actors.DgimActor.Update

case class TransactionStream(dgimActor: ActorRef) {

  // Initialize json formats
  case class Output(value: Long)
  case class Transaction(out: List[Output])
  case class Record(x: Transaction)
  implicit val formatOutput: RootJsonFormat[Output] = jsonFormat1(Output)
  implicit val formatTransaction: RootJsonFormat[Transaction] = jsonFormat1(Transaction)
  implicit val formatRecord: RootJsonFormat[Record] = jsonFormat1(Record)

  // Initialize logger
  val log: slf4j.Logger = LoggerFactory.getLogger(classOf[TransactionStream])

  /* Launches live stream of bitcoin transactions to DgimActor. */
  def run(implicit system: ActorSystem, materializer: ActorMaterializer): Unit = {
    import system.dispatcher

    // Process both strict and streamed messages
    val incoming: Sink[Message, Future[Done]] =
      Sink.foreach[Message] {
        case message: TextMessage.Strict =>
          log.debug("BITCOIN - Received strict transaction")
          update(message.text)
        case message: TextMessage.Streamed =>
          log.debug("BITCOIN - Received streamed transaction")
          val reduced = message.textStream.runWith(Sink.reduce[String](_ + _))
          val text = Await.result(reduced, 15.seconds)
          update(text)
        case _ => log.warn("BITCOIN - Message receipt failed")
      }.withAttributes(
          ActorAttributes.supervisionStrategy(failure => {
            log.warn(s"BITCOIN - Actor update failure $failure")
            Supervision.Restart
          }))

    // Send up subscription message and then hold connection open
    val outgoing = Source.single(TextMessage("{\"op\":\"unconfirmed_sub\"}"))
      .concatMat(Source.maybe[Message])(Keep.right)

    // Websocket connection to blockchain.info
    val webSocketFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] =
      Http().webSocketClientFlow(WebSocketRequest("wss://ws.blockchain.info/inv"))

    // Run graph with killswitch
    val (upgradeResponse, promise) =
      outgoing
        .viaMat(webSocketFlow)(Keep.right)
        .toMat(incoming)(Keep.both)
        .run()

    // Throw exception on unsuccessful connection attempts
    val connected = upgradeResponse.flatMap { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        log.info("BITCOIN - Connection established")
        Future.successful(Done)
      } else {
        log.warn(s"BITCOIN - Connection failed ${upgrade.response.status}")
        Thread.sleep(10000) // Manual backoff strategy for now of simply waiting 10 seconds
        Future.successful(Done)
      }
    }
    connected.onComplete(_ => log.info("BITCOIN - Connection attempt complete"))

    // Manually restart connection when fails or closes
    promise.onComplete(_ => run)
  }

  /* Updates DgimActor with transaction value. */
  def update(text: String): Unit = {
    log.debug("BITCOIN - Processing transaction")

    // Compute total out value
    val record = text.parseJson.convertTo[Record]
    val totalValue = record.x.out.map(_.value).sum

    // Bucketize and update DGIM
    val lowerBound = pow(10, floor(log10(totalValue)))/100000000L
    val upperBound = pow(10, floor(log10(totalValue)) + 1)/100000000L
    dgimActor ! Update(Set(s"$lowerBound - $upperBound"))
  }
}