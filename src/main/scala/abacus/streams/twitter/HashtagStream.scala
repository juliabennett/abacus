package abacus.streams.twitter

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl._
import akka.stream._
import akka.util.ByteString
import org.slf4j
import org.slf4j.LoggerFactory
import spray.json._
import spray.json.DefaultJsonProtocol._
import abacus.actors.DgimActor.Update

/**
  * Live stream of hashtags from Twitter's filter endpoint to DgimActors.
  *
  * @param topics Search topics to be used in querying Twitter's filter API
  */
case class HashtagStream(topics: List[SearchTopic]) {

  // Initialize logger
  val log: slf4j.Logger = LoggerFactory.getLogger(classOf[HashtagStream])

  // Initialize json format of standard Tweet message
  implicit val tweetFormat: RootJsonFormat[Tweet] = jsonFormat1(Tweet)

  // Initialize json format of limit message to disambiguate from genuine parsing errors
  case class LimitDetails(track: Long, timestamp_ms: String)
  case class LimitMessage(limit: LimitDetails)
  implicit val limitDetailsFormat: RootJsonFormat[LimitDetails] = jsonFormat2(LimitDetails)
  implicit val limitMessageFormat: RootJsonFormat[LimitMessage] = jsonFormat1(LimitMessage)

  /**
    * Launches live stream of hashtags matching topics to DgimActors.
    *
    * @return Killswitch for terminating stream
    */
  def run(implicit system: ActorSystem, materializer: ActorMaterializer): UniqueKillSwitch = {

    // Streaming response from HTTP request
    val restartSource = RestartSource.withBackoff(
      minBackoff = 5.seconds,
      maxBackoff =  320.seconds,
      randomFactor = 0.2
    ) { () => {
      Source.single((TwitterRequest(requestParams).request, -1))
        .via(Http().superPool())
        .via(Flow[(Try[HttpResponse], Int)].flatMapConcat {
          case (Success(response), _) if response.status == StatusCodes.OK =>
            log.info(s"TWITTER - Connection open with status ${response.status}")
            response.entity.withoutSizeLimit.dataBytes
          case (Success(response), _) =>
            log.warn(s"TWITTER - Received status ${response.status}")
            throw new RuntimeException
          case (Failure(e), _) =>
            log.warn(s"TWITTER - Restarting due to failed request ${e.getMessage}")
            throw new RuntimeException
        })
      }
    }

    // Transforms chunked data into stream of json
    val framing = Framing.delimiter(
        ByteString("\n"),
        maximumFrameLength = 100000,
        allowTruncation = true)
      .withAttributes(
        ActorAttributes.supervisionStrategy(failure => {
          log.warn(s"TWITTER - Framing failure $failure")
          Supervision.Restart }))

    // Transforms json to tweets
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

  /* Parses json byte string to Tweet */
  private def jsonToTweet(json: ByteString): Option[Tweet] = {
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
          val relevant = topic.keywords.map(text.contains(_)).reduce(_ || _)
          val filteredHashtags = hashtags diff topic.keywords.map("#" + _).toSet
          if (relevant) topic.dgimActor ! Update(filteredHashtags)
        }
      case _ => log.debug(s"TWITTER - Ignoring message")
    }

  /* Returns request parameters generated from topics */
  private def requestParams: Map[String, String] = {
    val track = topics.map(topic => topic.keywords.mkString(",")).mkString(",")
    Map("track"->track, "language"->"en")
  }

}
