
/* TODO
- Implement query method
- Use mod N timestamps
- Extend to improved error bound
 */

case class Bucket(position: Long, sizeExp: Long) {
  def +(that: Bucket) = Bucket(math.max(this.position, that.position), this.sizeExp + 1)

  override def toString = s"position=$position,sizeExp=$sizeExp"
}

case class Dgim(windowLength: Long, currentPosition: Long, buckets: Vector[Bucket] = Vector()) {

  def update(newElement: Int): Dgim = {
    val trimmedBuckets = trimBuckets()
    this.copy(
      currentPosition = currentPosition + 1,
      buckets = if (newElement == 0) trimmedBuckets
                else addBucket(trimmedBuckets)
    )
  }

  override def toString = {
    val bucketStr = buckets.mkString("[", ",", "]")
    s"windowLength=$windowLength,currentPosition=$currentPosition,buckets=$bucketStr"
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

  private def trimBuckets(): Vector[Bucket] = {
    buckets match {
      case start :+ last if last.position + windowLength <= currentPosition + 1 => start
      case _ => buckets
    }
  }

}