package abacus.dgim

/** Data structure storing state of Datar-Gionis-Indyk-Motwani (DGIM) algorithm
  * for approximating number of ones in a window of a binary stream.
  *
  * @param windowLen Number of positions in window
  * @param r DGIM precision parameter, where higher values of r have smaller error
  * @param currentStreamPos Latest position in stream modulo window length
  * @param buckets Vector of DGIM buckets describing current DGIM state
  */
case class Dgim(windowLen: Long, r: Int = 2, currentStreamPos: Long = -1, buckets: Vector[Bucket] = Vector()) {

  /** Batch processes segment of binary stream
    *
    * @param newElements Segment of binary stream ordered from earliest to latest
    * @return New Dgim instance reflecting updated state after processing stream segment
    */
  def update(newElements: List[Int]): Dgim = newElements.foldLeft(this)(_ update _)

  /** Processes new element in binary stream
    *
    * @param newElement Next element in binary stream
    * @return New Dgim instance reflecting updated state after processing new element
    */
  def update(newElement: Int): Dgim = {
    require(newElement == 0 || newElement == 1)
    val nextStreamPosition = (currentStreamPos + 1) % windowLen
    this.copy(
      currentStreamPos = nextStreamPosition,
      buckets = if (newElement == 0) trimBuckets
                else bucketsPlusOne(trimBuckets, nextStreamPosition))
  }

  /** Applies DGIM algorithm to return approximate count of ones in previous k elements in binary stream
    *
    * @param k Positive integer not larger than number of positions in DGIM window
    */
  def query(k: Long): Long = {
    @scala.annotation.tailrec
    def scanAndCount(k: Long, bucketListTail: Vector[Bucket], acc: Long = 0): Long = {
      bucketListTail match {
        case first +: second +: tail if bucketInRange(second, k) =>
          scanAndCount(k, second +: tail, acc + computeSize(first))
        case first +: tail if bucketInRange(first, k) =>
          acc + ((computeSize(first) + 1)/2)
        case _ => 0
      }
    }

    require(k > 0 && k <= windowLen)
    scanAndCount(k, buckets)
  }

  /* Returns True if DGIM has no record of any 1 elements within window */
  def isEmpty: Boolean = buckets.isEmpty

  override def toString: String = {
    val bucketStr = buckets.mkString("[", ",", "]")
    s"Dgim[windowLen=$windowLen,r=$r,currentStreamPos=$currentStreamPos,buckets=$bucketStr]"
  }

  /* Returns inputted DGIM buckets plus a new bucket of size one at next position.
     Completes merge process to maintain DGIM conditions. */
  private def bucketsPlusOne(currentBuckets: Vector[Bucket], nextPosition: Long): Vector[Bucket] = {
    @scala.annotation.tailrec
    def joinBucketListSegments(bucketListTail: Vector[Bucket],
                               middleBucket: Bucket,
                               bucketListHead: Vector[Bucket],
                               currentSizeCounter: Int = 0): Vector[Bucket] = {
      bucketListTail match {
        case first +: second +: tail if first.sizeExp == second.sizeExp =>
          if (currentSizeCounter == r-2) {
            val mergedBucket = first.merge(second, windowLen, currentStreamPos)
            joinBucketListSegments(tail, mergedBucket, bucketListHead :+ middleBucket)
          } else {
            joinBucketListSegments(second +: tail, first, bucketListHead :+ middleBucket, currentSizeCounter + 1)
          }
        case _ => (bucketListHead :+ middleBucket) ++ bucketListTail
      }
    }

    joinBucketListSegments(currentBuckets, Bucket(nextPosition, 0), Vector())
  }

  /* Returns current buckets filtered to only include those that will be within window after next update. */
  private def trimBuckets: Vector[Bucket] = buckets match {
    case start :+ last if last.windowPos(windowLen, currentStreamPos) == 0 => start
    case _ => buckets
  }

  /* Returns true if bucket position is within previous k elements in binary stream. */
  private def bucketInRange(bucket: Bucket, k: Long): Boolean = {
    bucket.windowPos(windowLen, currentStreamPos) >= windowLen - k
  }

  /* Returns size of bucket. */
  private def computeSize(bucket: Bucket): Long = {
    @scala.annotation.tailrec
    def powTwo(sizeExp: Long, acc: Long = 1): Long = {
      if (sizeExp == 0) acc
      else powTwo(sizeExp - 1, acc * 2)
    }
    powTwo(bucket.sizeExp)
  }
}