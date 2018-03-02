package abacus.streams.twitter

import scala.concurrent.duration._
import scala.util.{Success, Try}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl._
import akka.stream._
import akka.util.ByteString
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

  /**
    * Launches live stream of hashtags matching parameters to actor.
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
      .map(_.utf8String.parseJson.convertTo[Tweet])
      .withAttributes(
        ActorAttributes.supervisionStrategy(_ => Supervision.Restart))

    // Run and return killswitch
    restartSource
      .viaMat(KillSwitches.single)(Keep.right)
      .via(framing)
      .via(parsing)
      .to(Sink.foreach(update))
      .run()
  }

  /* Updates all relevant DgimActors with latest hashtag observation */
  private def update(tweet: Tweet): Unit = {
    val text = tweet.text.toLowerCase()
    val hashtags = tweet.hashtags

    topics.foreach{ topic =>
      var relevant = topic.keywords.map(text.contains(_)).reduce(_ || _)
      if (relevant) topic.dgimActor ! Update(hashtags)
    }
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
  val dgimActor: ActorRef = system.actorOf(Props(classOf[DgimActor], 1000000L, 2))

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
      .filterNot(_ == "#")
      .toSet
}