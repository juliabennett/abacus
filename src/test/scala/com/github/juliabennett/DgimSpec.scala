package com.github.juliabennett

import org.scalatest.FlatSpec

class DgimSpec extends FlatSpec {

  val allZeroes = List(0,0,0,0,0)
  val singleOne = List(1,0,0,0,0)
  val allOnes = List(1,1,1,1,1)
  val series1 = List(1,0,0,1,0,1,0,0,1,1,1,1)
  val series2 = List(0,0,1,0,1,1,0,1,1,1)

  "Dgim" should "add new bucket and merge tail" in {

    assert(Dgim(100).update(allZeroes).buckets == Vector())

    assert(Dgim(100).update(singleOne).buckets == Vector(
      Bucket(0, 0)
    ))

    assert(Dgim(100).update(allOnes).buckets == Vector(
      Bucket(4, 0),
      Bucket(3, 1),
      Bucket(1, 1)
    ))

    assert(Dgim(100).update(series1).buckets == Vector(
      Bucket(11, 0),
      Bucket(10, 1),
      Bucket(8, 2)
    ))

    assert(Dgim(100).update(series2).buckets == Vector(
      Bucket(9, 0),
      Bucket(8, 0),
      Bucket(7, 1),
      Bucket(4, 1)
    ))
  }

  it should "drop buckets that are out of window" in {

    assert(Dgim(4).update(singleOne).buckets == Vector())
    assert(Dgim(5).update(singleOne).buckets == Vector(
      Bucket(0, 0)
    ))

    assert(Dgim(3).update(allOnes).buckets == Vector(
      Bucket(1, 0),
      Bucket(0, 1)
    ))
    assert(Dgim(2).update(allOnes).buckets == Vector(
      Bucket(0, 0),
      Bucket(1, 0)
    ))

    assert(Dgim(4).update(series1).buckets == Vector(
      Bucket(3, 0),
      Bucket(2, 0),
      Bucket(1, 1)
    ))
    assert(Dgim(7).update(series1).buckets == Vector(
      Bucket(4, 0),
      Bucket(3, 1),
      Bucket(1, 1)
    ))

    assert(Dgim(5).update(series2).buckets == Vector(
      Bucket(4, 0),
      Bucket(3, 0),
      Bucket(2, 1)
    ))
    assert(Dgim(3).update(series2).buckets == Vector(
      Bucket(0, 0),
      Bucket(2, 1)
    ))
  }

  it should "return approximate count of ones in window" in {

    assert(Dgim(100).query(10) == 0)

    assert(Dgim(100).update(allZeroes).query(10) == 0)

    assert(Dgim(100).update(singleOne).query(5) == 1)
    assert(Dgim(100).update(singleOne).query(3) == 0)
    assert(Dgim(4).update(singleOne).query(3) == 0)

    assert(Dgim(100).update(allOnes).query(0) == 0)
    assert(Dgim(100).update(allOnes).query(1) == 1)
    assert(Dgim(100).update(allOnes).query(4) == 4)
    assert(Dgim(100).update(allOnes).query(5) == 4)
    assert(Dgim(100).update(allOnes).query(50) == 4)
    assert(Dgim(3).update(allOnes).query(2) == 2)
    assert(Dgim(2).update(allOnes).query(2) == 2)

    assert(Dgim(100).update(series1).query(5) == 5)
    assert(Dgim(100).update(series1).query(3) == 2)
    assert(Dgim(4).update(series1).query(4) == 3)
    assert(Dgim(7).update(series1).query(4) == 4)

    assert(Dgim(100).update(series2).query(5) == 3)
    assert(Dgim(100).update(series2).query(50) == 5)
    assert(Dgim(100).update(series2).query(2) == 2)
    assert(Dgim(5).update(series2).query(3) == 3)
    assert(Dgim(3).update(series2).query(3) == 2)
  }
}