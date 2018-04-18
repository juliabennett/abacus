package abacus.streams.twitter

import akka.actor.{ActorRef, ActorSystem, Props}
import abacus.dgim.DgimActor

/**
  * Search topic against Twitter's filter API, maintaining DgimActor that listens for hashtag stream.
  *
  * @param name Name of topic
  * @param keywords Lowercase track terms in Twitter's filter API
  */
case class SearchTopic(name: String, keywords: List[String])(implicit system: ActorSystem) {
  val dgimActor: ActorRef = system.actorOf(Props(classOf[DgimActor], "TWITTER", 1000000L, 25)
    .withDispatcher("prio-dispatcher"))

  def toTuple: (String, ActorRef) = (name, dgimActor)
}