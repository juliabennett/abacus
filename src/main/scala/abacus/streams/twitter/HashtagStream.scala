package abacus.streams.twitter

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl._
import akka.stream._
import akka.util.ByteString
import org.slf4j
import org.slf4j.LoggerFactory
import spray.json._
import spray.json.DefaultJsonProtocol._
import abacus.dgim.DgimActor
import abacus.dgim.DgimActor.Update

/**
  * Live stream of hashtags from Twitter's filter endpoint to DgimActors.
  *
  * @param topics Topics to be used in querying Twitter's filter API
  */
case class HashtagStream(topics: List[Topic]) {

  // Initialize logger
  val log: slf4j.Logger = LoggerFactory.getLogger(classOf[HashtagStream])

  /**
    * Launches live stream of hashtags matching parameters to DgimActor.
    *
    * @return Killswitch for terminating stream
    */
  def run(implicit system: ActorSystem, materializer: ActorMaterializer): UniqueKillSwitch = {

    // Streaming response from HTTP request
    val params = Map(
      "track" -> topics.map(topic => topic.keywords.mkString(",")).mkString(","),
      "language" -> "en")
    val responseStream = Source.single((TwitterRequest(params, usePostRequest=true).request, -1))
      .via(Http().superPool())
      .via(Flow[(Try[HttpResponse], Int)].flatMapConcat{
        case (Success(response), _) =>
          log.debug("TWITTER - Received message chunk")
          response.entity.withoutSizeLimit.dataBytes
        case (Failure(e), _) =>
          log.warn(s"TWITTER - Restarting due to failed request ${e.getMessage}")
          throw new RuntimeException})

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
        ActorAttributes.supervisionStrategy(failure => {
          log.warn(s"TWITTER - Framing failure $failure")
          Supervision.Restart }))

    val parsing  = Flow[ByteString]
      .map(jsonToTweet)
      .withAttributes(
        ActorAttributes.supervisionStrategy(failure => Supervision.Restart))

    // Setup sink with restart strategy
    val incoming = Sink.foreach(update).withAttributes(
      ActorAttributes.supervisionStrategy(failure => {
        log.warn(s"TWITTER - Actor update failure $failure")
        Supervision.Restart }))

    // Run and return killswitch
    restartSource
      .viaMat(KillSwitches.single)(Keep.right)
      .via(framing)
      .via(parsing)
      .to(incoming)
      .run()
  }

  /* Parses json ByteString to Tweet */
  private def jsonToTweet(json: ByteString): Option[Tweet] = {

    // Initialize format of standard Tweet message
    implicit val tweetFormat: RootJsonFormat[Tweet] = jsonFormat1(Tweet)

    // Initialize format of limit message to disambiguate from genuine parsing errors
    case class LimitDetails(track: Long, timestamp_ms: String)
    case class LimitMessage(limit: LimitDetails)
    implicit val limitDetailsFormat: RootJsonFormat[LimitDetails] = jsonFormat2(LimitDetails)
    implicit val limitMessageFormat: RootJsonFormat[LimitMessage] = jsonFormat1(LimitMessage)

    // Parse json and return Tweet as an option
    Try(json.utf8String.parseJson.convertTo[Either[Tweet, LimitMessage]]) match {
      case Success(Left(tweet)) => Some(tweet)
      case Success(Right(limitMessage)) => None
      case Failure(e) =>
        log.warn(s"TWITTER - Failure to parse json: ${json.utf8String}")
        throw e
    }
  }

  /* Updates all relevant DgimActors with latest hashtag observation */
  private def update(tweetOpt: Option[Tweet]): Unit =
    tweetOpt match {
      case Some(tweet) =>
        log.debug(s"TWITTER - Processing tweet")
        val text = tweet.text.toLowerCase()
        val hashtags = tweet.hashtags

        topics.foreach{ topic =>
          var relevant = topic.keywords.map(text.contains(_)).reduce(_ || _)
          if (relevant) topic.dgimActor ! Update(hashtags)
        }
      case _ => log.debug(s"TWITTER - Ignoring message")
    }

}

/**
  * Search topic against Twitter's filter API, maintaining DgimActor
  *   that listens for hashtag stream.
  *
  * @param name Name of topic
  * @param keywords Lowercase track terms in Twitter's filter API
  */
case class Topic(name: String, keywords: List[String])(implicit system: ActorSystem) {
  val dgimActor: ActorRef = system.actorOf(Props(classOf[DgimActor], "TWITTER", 1000000000L, 2))

  def toTuple: (String, ActorRef) = (name, dgimActor)
}

/**
  * Simple wrapper for Tweet returned by Twitter's filter API.
  *
  * @param text Body of tweet
  */
case class Tweet(text: String) {

  /* Returns set of hashtags from body of tweet. */
  def hashtags: Set[String] =
    text.split("\\s+")
      .collect{case t if t.startsWith("#") => t.replaceAll("[^A-Za-z0-9#]", "")}
      .map(_.toLowerCase)
      .filterNot(_ == "#")
      .toSet
}
