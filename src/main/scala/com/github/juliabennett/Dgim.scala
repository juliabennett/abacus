package com.github.juliabennett

/* TODO
- Use mod N timestamps
- Extend to improved error bound
 */

case class Bucket(position: Long, sizeExp: Long) {
  def +(that: Bucket) = Bucket(math.max(this.position, that.position), this.sizeExp + 1)

  override def toString = s"position=$position,sizeExp=$sizeExp"
}

case class Dgim(windowLength: Long, currentPosition: Long = 0, buckets: Vector[Bucket] = Vector()) {

  def update(newElements: List[Int]): Dgim = {
    newElements.foldLeft(this)((currentDgim, newElement) =>
      currentDgim.update(newElement))
  }

  def update(newElement: Int): Dgim = {
    val trimmedBuckets = trimBuckets()
    this.copy(
      currentPosition = currentPosition + 1,
      buckets = if (newElement == 0) trimmedBuckets
                else addBucket(trimmedBuckets))
  }

  def query(k: Long): Long = scanAndCount(k, buckets)

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
  private def addBucket(currentBuckets: Vector[Bucket], newBucket: Bucket = Bucket(currentPosition + 1, 0),
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
      case first +: second +: tail if currentPosition - second.position < k =>
        scanAndCount(k, second +: tail, acc + pow2(first.sizeExp))
      case first +: second +: tail if currentPosition - first.position < k =>
        acc + pow2(first.sizeExp - 1)
      case first +: tail if currentPosition - first.position < k =>
        acc + pow2(first.sizeExp - 1)
      case _ => acc
    }
  }

  @scala.annotation.tailrec
  private def pow2(exp: Long, acc: Long = 1): Long = {
    if (exp < 0) 0
    else if (exp == 0) acc
    else pow2(exp-1, acc*2)
  }

}