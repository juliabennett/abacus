package com.github.juliabennett

/** Data structure storing state of Datar-Gionis-Indyk-Motwani (DGIM) algorithm
  *  for estimating number of ones in a window.
  *
  * @param windowLen Number of positions in window
  * @param r Dgim precision parameter, where higher values of r have smaller error
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
    val newStreamPos = (currentStreamPos + 1) % windowLen
    this.copy(
      currentStreamPos = newStreamPos,
      buckets = if (newElement == 0) trimBuckets
                else addBucket(trimBuckets, Bucket(newStreamPos, 0))
    )
  }

  /** Applies DGIM algorithm to return approximate count of ones in previous k elements in binary stream
    *
    * @param k Positive integer not larger than number of positions in DGIM window
    */
  def query(k: Long): Long = scanAndCount(k, buckets)

  override def toString: String = {
    val bucketStr = buckets.mkString("[", ",", "]")
    s"Dgim[windowLen=$windowLen,currentStreamPos=$currentStreamPos,buckets=$bucketStr]"
  }

  /* Returns DGIM buckets filtered to only include those that will be within window after next update. */
  private def trimBuckets = buckets match {
    case start :+ last if last.windowPos(windowLen, currentStreamPos) == 0 => start
    case _ => buckets
  }

  /* Adds bucket to head of DGIM tail. Updates tail to ensure DGIM conditions are preserved. */
  @scala.annotation.tailrec
  private def addBucket(
      bucketTail: Vector[Bucket],
      newBucket: Bucket,
      bucketAcc: Vector[Bucket] = Vector(),
      runningCounter: Int = 0): Vector[Bucket] = {

    bucketTail match {
      case first +: second +: tail if first.sizeExp == second.sizeExp => {
        if (runningCounter == r-2) {
          val mergedBucket = first.merge(second, windowLen, currentStreamPos)
          addBucket(tail, mergedBucket, bucketAcc :+ newBucket, 0)
        } else {
          addBucket(second +: tail, first, bucketAcc :+ newBucket, runningCounter + 1)
        }
      }
      case _ => (bucketAcc :+ newBucket) ++ bucketTail
    }
  }

  /* Implementation of query in DGIM algorithm. Returns approximate count of ones that
       are within intersection of DGIM tail and previous k elements in binary stream. */
  @scala.annotation.tailrec
  private def scanAndCount(k: Long, bucketTail: Vector[Bucket], acc: Long = 0): Long = {
    bucketTail match {
      case first +: second +: tail if bucketInRange(second, k) =>
        scanAndCount(k, second +: tail, acc + computeSize(first.sizeExp))
      case first +: tail if bucketInRange(first, k) =>
        acc + computeSize(first.sizeExp - 1)
      case _ => 0
    }
  }

  /* Returns true if bucket position is within previous k elements in binary stream */
  private def bucketInRange(bucket: Bucket, k: Long): Boolean = {
    bucket.windowPos(windowLen, currentStreamPos) >= windowLen - k
  }

  /* Computes bucket size as power of two. Negative input returns one as default value. */
  @scala.annotation.tailrec
  private def computeSize(sizeExp: Long, acc: Long = 1): Long = {
    if (sizeExp < 0) 1
    else if (sizeExp == 0) acc
    else computeSize(sizeExp - 1, acc * 2)
  }

}