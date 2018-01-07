package abacus.dgim

import org.scalatest.{Assertion, BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

class DgimActorSpec extends TestKit(ActorSystem("DgimActorSpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(1.seconds)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def assertQuery(
      dgimActor: ActorRef,
      k: Long,
      expected: Map[String, Long]): Assertion = {

    val reply = dgimActor ? DgimActor.QueryAll(k)
    assert(Await.result(reply, 1.seconds) == expected)
  }

  "A DgimActor" must {

    "track DGIM states of labels appearing in stream" in {

      // Normal series

      val dgimActor1 = system.actorOf(Props(classOf[DgimActor], 5L, 2))

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

      assertQuery(dgimActor1, 5, Map("b"->3, "c"->2, "d"->1, "e"->1))
      assertQuery(dgimActor1, 4, Map("b"->2, "c"->2, "d"->1, "e"->1))
      assertQuery(dgimActor1, 3, Map("b"->2, "c"->1, "e"->1))
      assertQuery(dgimActor1, 2, Map("b"->1, "c"->1, "e"->1))
      assertQuery(dgimActor1, 1, Map("e"->1))


      // Series with some empty updates

      val dgimActor2 = system.actorOf(Props(classOf[DgimActor], 3L, 2))

      val withEmpty: List[Set[String]] = List(
        Set("a", "b", "c"),
        Set(),
        Set("d"),
        Set()
      )

      withEmpty.foreach{ labelSet => dgimActor2 ! DgimActor.Update(labelSet) }
      assertQuery(dgimActor2, 3, Map("d"->1))
      assertQuery(dgimActor2, 2, Map("d"->1))
      assertQuery(dgimActor2, 1, Map())


      // Series with all empty updates

      val dgimActor3 = system.actorOf(Props(classOf[DgimActor], 3L, 2))

      val allEmpty: List[Set[String]] = List(
        Set(),
        Set(),
        Set(),
        Set()
      )

      allEmpty.foreach{ labelSet => dgimActor2 ! DgimActor.Update(labelSet) }

      assertQuery(dgimActor2, 3, Map())
      assertQuery(dgimActor2, 2, Map())
      assertQuery(dgimActor2, 1, Map())
    }
  }
}