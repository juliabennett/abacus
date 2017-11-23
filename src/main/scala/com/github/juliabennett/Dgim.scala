package com.github.juliabennett

/* TODO
- Use mod N timestamps
- Extend to improved error bound
 */

case class Bucket(position: Long, sizeExp: Long) {
  def +(that: Bucket) = Bucket(math.max(this.position, that.position), this.sizeExp + 1)

  override def toString = s"Bucket[position=$position,sizeExp=$sizeExp]"
}

case class Dgim(windowLength: Long, currentPosition: Long = 0, buckets: Vector[Bucket] = Vector()) {

  def update(newElements: List[Int]): Dgim = newElements.foldLeft(this)(_ update _)

  def update(newElement: Int): Dgim = {
    this.copy(
      currentPosition = currentPosition + 1,
      buckets = if (newElement == 0) trimBuckets
                else addBucket(trimBuckets))
  }

  def query(k: Long) = scanAndCount(k, buckets)

  override def toString: String = {
    val bucketStr = buckets.mkString("[", ",", "]")
    s"Dgim[windowLength=$windowLength,currentPosition=$currentPosition,buckets=$bucketStr]"
  }

  private def trimBuckets(): Vector[Bucket] = {
    buckets match {
      case start :+ last if last.position + windowLength <= currentPosition + 1 => start
      case _ => buckets
    }
  }

  @scala.annotation.tailrec
  private def addBucket(
      currentBuckets: Vector[Bucket],
      newBucket: Bucket = Bucket(currentPosition + 1, 0),
      bucketAcc: Vector[Bucket] = Vector()): Vector[Bucket] = {

    currentBuckets match {
      case first +: second +: tail if first.sizeExp == second.sizeExp =>
        addBucket(tail, first + second, bucketAcc :+ newBucket)
      case _ => (bucketAcc :+ newBucket) ++ currentBuckets
    }
  }

  @scala.annotation.tailrec
  private def scanAndCount(k: Long, currentBuckets: Vector[Bucket], acc: Long = 0): Long = {
    currentBuckets match {
      case first +: second +: tail if positionInRange(second.position, k) =>
        scanAndCount(k, second +: tail, acc + computeSize(first.sizeExp))
      case first +: tail if positionInRange(first.position, k) =>
        acc + computeSize(first.sizeExp - 1)
      case _ => 0
    }
  }

  private def positionInRange(position: Long, k: Long) = position > currentPosition - k

  @scala.annotation.tailrec
  private def computeSize(sizeExp: Long, acc: Long = 1): Long = {
    if (sizeExp == -1) 1
    else if (sizeExp == 0) acc
    else computeSize(sizeExp - 1, acc * 2)
  }

}