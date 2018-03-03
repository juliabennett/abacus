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
import spray.json._
import spray.json.DefaultJsonProtocol._
import abacus.dgim.DgimActor.Update

case class TransactionStream(dgimActor: ActorRef) {

  // Initialize Json formats
  case class Output(value: Long)
  case class Transaction(out: List[Output])
  case class Record(x: Transaction)
  implicit val formatOutput: RootJsonFormat[Output] = jsonFormat1(Output)
  implicit val formatTransaction: RootJsonFormat[Transaction] = jsonFormat1(Transaction)
  implicit val formatRecord: RootJsonFormat[Record] = jsonFormat1(Record)

  /**
    * Launches live stream of bitcoin transactions to DgimActor.
    *
    * @return Killswitch for terminating stream
    */
  def run(implicit system: ActorSystem, materializer: ActorMaterializer): UniqueKillSwitch = {
    import system.dispatcher

    // Process both strict and streamed messages
    val incoming: Sink[Message, Future[Done]] =
      Sink.foreach[Message] {
        case message: TextMessage.Strict => update(message.text)
        case message: TextMessage.Streamed =>
          val reduced = message.textStream.runWith(Sink.reduce[String](_ + _))
          val text = Await.result(reduced, 1.seconds)
          update(text)
      }.withAttributes(
          ActorAttributes.supervisionStrategy(_ => Supervision.Restart))

    // Send up subscription message and then hold connection open
    val outgoing = Source.single(TextMessage("{\"op\":\"unconfirmed_sub\"}"))
      .concatMat(Source.maybe[Message])(Keep.right)

    // Websocket connection to blockchain.info
    val webSocketFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] =
      Http().webSocketClientFlow(WebSocketRequest("wss://ws.blockchain.info/inv"))
        .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Restart))

    // Run graph with killswitch
    val (upgradeResponse, killswitch) =
      outgoing
        .viaMat(webSocketFlow)(Keep.right)
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(incoming)(Keep.left)
        .run()

    // Throw exception on unsuccessful connection attempts
    val connected = upgradeResponse.flatMap { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }
    connected.onComplete(println)

    killswitch
  }

  /* Updates DgimActor with transaction value. */
  def update(text: String): Unit = {

    // Compute total out value
    val record = text.parseJson.convertTo[Record]
    val totalValue = record.x.out.map(_.value).sum

    // Bucketize and update DGIM
    val lowerBound = pow(10, floor(log10(totalValue)))/100000000L
    val upperBound = pow(10, floor(log10(totalValue)) + 1)/100000000L
    dgimActor ! Update(Set(s"$lowerBound - $upperBound"))
  }
}