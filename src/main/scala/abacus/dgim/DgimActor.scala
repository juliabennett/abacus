package abacus.dgim

import scala.collection.mutable
import akka.actor.{Actor, Props}

/**
  * Companion object declaring DgimActor messages and props.
  */
object DgimActor {

  /* Message to tell DgimActor observed labels from latest position. */
  final case class Update(labels: Set[String])

  /* Message to ask DgimActor for DGIM states of all active labels. */
  final case class QueryAll(k: Option[Long], topN: Option[Int])

  /* Returns a Props for creating DgimActor. */
  def props(name: String, windowLength: Long, r: Int): Props =
    Props(new DgimActor(name: String, windowLength: Long, r: Int))
}

/**
  * Actor maintaining DGIM states for labels appearing in stream of observations.
  *
  * @param windowLength Number of positions in each DGIM window.
  * @param r DGIM precision parameter, where higher values of r have smaller error
  */
class DgimActor(name: String, windowLength: Long, r: Int) extends Actor with akka.actor.ActorLogging {

  import DgimActor._

  /* Implementation of receive method for communicating with actor. */
  def receive: PartialFunction[Any, Unit] = {
    case Update(labels) =>
      log.debug(s"$name - Update: $labels")
      update(labels)
    case QueryAll(k, topN) =>
      log.debug(s"$name - Query: k=$k, topN=$topN")
      sender ! query(k.getOrElse(windowLength), topN.getOrElse(25))
  }

  // Mutable variable for updating latest position
  private var positionsInWindow: Long = 0

  // Mutable mapping from labels to Dgim instances managed by Actor
  private val dgimMap: mutable.Map[String, Dgim] = mutable.Map()

  /** Updates DGIM states across all active labels, adding 1 to the binary stream
    * of each observed label and 0 to the binary stream of each unobserved label.
    *
    * @param labels Observed labels from latest position
    */
  private def update(labels: Set[String]): Unit = {
    // Update number of positions in window
    if (positionsInWindow < windowLength) positionsInWindow += 1

    // Initialize DGIM for each new label
    dgimMap ++= labels.filterNot(dgimMap.contains).map{ label =>
      log.info(s"$name - New DGIM: $label, Total DGIM count: ${dgimMap.size}")
      (label, Dgim(windowLength, r))
    }

    // Update DGIM states across all active labels
    dgimMap ++= dgimMap.map{ case (label, dgim) =>
      val bit = if (labels.contains(label)) 1 else 0
      (label, dgim.update(bit))
    }

    // Drop empty DGIMs
    dgimMap.retain((label, dgim) => ! dgim.isEmpty)
  }

  /** Returns a mapping from each label observed in DGIM window to DGIM approximation
    * of the count of observations in previous k positions that included label.
    *
    * @param k Positive integer not larger than number of positions in each DGIM window
    * @return Tuple consisting of number of positions within range and mapping from
    *         labels to approximate counts
    */
  private def query(k: Long, topN: Int): (Long, List[(String, Long)]) = {
    require(topN > 0)
    val counts = dgimMap.mapValues(_.query(k)).toList
    val cutoff = topNCutoff(counts, topN).max(1)
    (positionsInWindow.min(k), counts.filter(_._2 >= cutoff))
  }

  /* Returns Nth largest count from an unsorted list of count tuples in linear time. */
  private def topNCutoff(elements: List[(String, Long)], topN: Int): Long = {
    val topNElements = elements.foldLeft(List(): List[Long])((topNAcc, element) => {
      if (topNAcc.size < topN) (element._2 :: topNAcc).sorted
      else if (element._2 <= topNAcc.head) topNAcc
      else (element._2 :: topNAcc.tail).sorted
    })
    if (topNElements.isEmpty) -1 else topNElements.head
  }

}