package abacus.streams.twitter

import scala.concurrent.duration._
import scala.util.{Success, Try}
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl._
import akka.stream._
import akka.util.ByteString
import spray.json._
import spray.json.DefaultJsonProtocol._
import abacus.dgim.DgimActor.Update

/**
  * Live stream of hashtags from Twitter's filter endpoint to DgimActor.
  *
  * @param params Filter API request parameters
  */
case class HashtagStream(params: Map[String, String], dgimActor: ActorRef) {

  /**
    * Launches live stream of hashtags matching parameters to actor.
    *
    * @return Killswitch for terminating stream
    */
  def run(implicit system: ActorSystem, materializer: ActorMaterializer): UniqueKillSwitch = {

    // Streaming response from HTTP request
    val responseStream = Source.single((TwitterRequest(params).request, -1))
      .via(Http().superPool())
      .via(Flow[(Try[HttpResponse], Int)].flatMapConcat{
        case (Success(response), _) =>  response.entity.withoutSizeLimit.dataBytes
        case _ => throw new RuntimeException})

    // Backoff strategy to handle closed or failed requests
    val restartSource = RestartSource
      .withBackoff(
        minBackoff = 5.seconds,
        maxBackoff =  320.seconds,
        randomFactor = 0.2) { () => responseStream}

    // Transforms chunked data into individual tweets
    val framing = Framing.delimiter(
        ByteString("\n"),
        maximumFrameLength = 100000,
        allowTruncation = true)
      .withAttributes(
        ActorAttributes.supervisionStrategy(_ => Supervision.Restart))

    // Parse tweets and extract set of hashtags
    implicit val format: RootJsonFormat[Tweet] = jsonFormat1(Tweet)
    val parsing  = Flow[ByteString]
      .map(_.utf8String.parseJson.convertTo[Tweet].hashtags)
      .withAttributes(
        ActorAttributes.supervisionStrategy(_ => Supervision.Restart))

    // Run and return killswitch
    restartSource
      .viaMat(KillSwitches.single)(Keep.right)
      .via(framing)
      .via(parsing)
      .to(Sink.foreach(dgimActor ! Update(_)))
      .run()
  }

  /**
    * Tweet
    *
    * @param text Body of tweet
    */
  case class Tweet(text: String) {

    /* Returns set of hashtags from body of tweet. */
    def hashtags: Set[String] =
      // Nice hashtag parsing implementation from Akka docs
      text.split("\\s+").collect{ case t if t.startsWith("#") => t }.toSet
  }
}