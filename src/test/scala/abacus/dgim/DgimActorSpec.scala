package abacus.dgim

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.scalatest.{Assertion, BeforeAndAfterAll, Matchers, WordSpecLike}

class DgimActorSpec extends TestKit(ActorSystem("DgimActorSpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  // Testing utils
  implicit val timeout: Timeout = Timeout(1.seconds)
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  /* Wrapper for asserting against query results. */
  def assertQuery(
      dgimActor: ActorRef,
      k: Option[Long],
      topN: Option[Int],
      expected: (Long, List[(String, Long)])): Assertion = {

    val reply = dgimActor ? DgimActor.QueryAll(k, topN)
    val result = Await.result(reply, 1.seconds)
      .asInstanceOf[(Long, List[(String, Long)])]
    assert(result._1 == expected._1 && result._2.toSet == expected._2.toSet)
  }

  "DgimActor" should {

    "prioritize query messages over updates" in {

      val dgimActor = system.actorOf(Props(classOf[DgimActor], "TESTING", 5L, 2)
        .withDispatcher("prio-dispatcher"))

      dgimActor ! DgimActor.Update(Set("a"))
      assertQuery(dgimActor, None, None, (0, List()))
      Thread.sleep(1000)
      assertQuery(dgimActor, None, None, (1, List(("a", 1))))

    }

    "track DGIM states of labels appearing in stream" in {

      // Normal series

      val dgimActor1 = system.actorOf(Props(classOf[DgimActor], "TESTING", 5L, 2))

      val series: List[Set[String]] = List(
        Set("a", "b", "c"),
        Set("d"),
        Set("b"),
        Set("d", "c"),
        Set("b"),
        Set("b", "c") ,
        Set("e")
      )

      series.foreach{ labelSet => dgimActor1 ! DgimActor.Update(labelSet) }

      assertQuery(
        dgimActor1,
        Some(5),
        None,
        (5, List(("b", 3), ("c", 2), ("d", 1), ("e", 1))))
      assertQuery(
        dgimActor1,
        None,
        None,
        (5, List(("b", 3), ("c", 2), ("d", 1), ("e", 1))))
      assertQuery(
        dgimActor1,
        None,
        Some(1),
        (5, List(("b", 3))))
      assertQuery(
        dgimActor1,
        Some(4),
        Some(3),
        (4, List(("b",2), ("c", 2), ("d", 1), ("e", 1))))
      assertQuery(
        dgimActor1,
        Some(3),
        None,
        (3, List(("b", 2), ("c", 1), ("e", 1))))
      assertQuery(
        dgimActor1,
        Some(2),
        None,
        (2, List(("b", 1), ("c", 1), ("e", 1))))
      assertQuery(
        dgimActor1,
        Some(1),
        None,
        (1, List(("e", 1))))


      // Series with some empty updates

      val dgimActor2 = system.actorOf(Props(classOf[DgimActor], "TESTING", 3L, 2))

      val withEmpty: List[Set[String]] = List(
        Set("a", "b", "c"),
        Set(),
        Set("d"),
        Set()
      )

      withEmpty.foreach{ labelSet => dgimActor2 ! DgimActor.Update(labelSet) }
      assertQuery(
        dgimActor2,
        Some(3),
        None,
        (3, List(("d", 1))))
      assertQuery(
        dgimActor2,
        Some(2),
        None,
        (2, List(("d", 1))))
      assertQuery(
        dgimActor2,
        Some(1),
        None,
        (1, List()))


      // Series with all empty updates

      val dgimActor3 = system.actorOf(Props(classOf[DgimActor], "TESTING", 3L, 2))

      val allEmpty: List[Set[String]] = List(
        Set(),
        Set(),
        Set(),
        Set()
      )

      allEmpty.foreach{ labelSet => dgimActor2 ! DgimActor.Update(labelSet) }

      assertQuery(
        dgimActor2,
        Some(3),
        None,
        (3, List()))
      assertQuery(
        dgimActor2,
        Some(2),
        None,
        (2, List()))
      assertQuery(
        dgimActor2,
        Some(1),
        None,
        (1, List()))
    }
  }
}