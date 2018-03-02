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
  def props(windowLength: Long, r: Int): Props =
    Props(new DgimActor(windowLength: Long, r: Int))
}

/**
  * Actor maintaining DGIM states for labels appearing in stream of observations.
  *
  * @param windowLength Number of positions in each DGIM window.
  * @param r DGIM precision parameter, where higher values of r have smaller error
  */
class DgimActor(windowLength: Long, r: Int) extends Actor {
  import DgimActor._

  /* Implementation of receive method for communicating with actor. */
  def receive: PartialFunction[Any, Unit] = {
    case Update(labels) => update(labels)
    case QueryAll(k, topN) => sender ! query(k.getOrElse(windowLength), topN.getOrElse(25))
  }

  // Mutable variable for updating latest position
  private var positionsInWindow: Long = 0

  // Mutable mapping from labels to Dgim instances managed by Actor
  private val dgimMap: mutable.Map[String, Dgim] = mutable.Map()

  /** Updates DGIM states across all active labels, adding 1 to the binary stream
    *  of each observed label and 0 to the binary stream of each unobserved label.
    *
    * @param labels Observed labels from latest position
    */
  private def update(labels: Set[String]): Unit = {
    // Update number of positions in window
    if (positionsInWindow < windowLength) positionsInWindow += 1

    // Initialize DGIM for each new label
    dgimMap ++= labels.filterNot(dgimMap.contains).map((_, Dgim(windowLength, r)))

    // Update DGIM states across all active labels
    dgimMap ++= dgimMap.map{ case (label, dgim) =>
      val bit = if (labels.contains(label)) 1 else 0
      (label, dgim.update(bit))
    }

    // Drop empty DGIMs
    dgimMap.retain((label, dgim) => ! dgim.isEmpty)
  }

  /** Returns a mapping from each label observed in DGIM window to DGIM approximation
    *  of the count of observations in previous k positions that included label.
    *
    * @param k Positive integer not larger than number of positions in each DGIM window
    * @return Tuple consisting of number of positions within range and mapping from
    *           labels to approximate counts
    */
  private def query(k: Long, topN: Int): (Long, List[(String, Long)]) = {
    require(topN > 0)

    val counts = dgimMap.mapValues(_.query(k)).toList
      .filter(_._2 > 0)
      .sortBy(-1*_._2)

    val cutoff = if (counts.length <= topN) -1 else counts(topN - 1)._2
    val pairs = counts
      .filter(_._2 >= cutoff)
      .map(tup => (tup._1, tup._2))

    (positionsInWindow.min(k), pairs)
  }
}