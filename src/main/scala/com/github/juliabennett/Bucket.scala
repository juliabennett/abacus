package com.github.juliabennett

/** DGIM Bucket
  *
  * @param streamPos Position in DGIM stream modulo window size
  * @param sizeExp Base two exponent of bucket size, i.e. size = 2^sizeExp
  */
case class Bucket(streamPos: Long, sizeExp: Long) {

  /* Computes position of bucket in DGIM window relative to latest */
  def windowPos(windowLen: Long, currentStreamPos: Long): Long = {
    (streamPos + windowLen - currentStreamPos - 1) % windowLen
  }

  /* Returns true if this is earlier than that in DGIM stream */
  def isEarlier(that: Bucket, windowLen: Long, currentStreamPos: Long): Boolean = {
    this.windowPos(windowLen, currentStreamPos) < that.windowPos(windowLen, currentStreamPos)
  }

  /* Merges two DGIM buckets of the same size */
  def merge(that: Bucket, windowLen: Long, currentStreamPos: Long): Bucket = {
    val newStreamPos = if (this.isEarlier(that, windowLen, currentStreamPos)) that.streamPos else this.streamPos
    Bucket(newStreamPos, this.sizeExp + 1)
  }

  override def toString = s"Bucket[streamPos=$streamPos,sizeExp=$sizeExp]"
}