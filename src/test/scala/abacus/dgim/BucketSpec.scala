package abacus.dgim

import org.scalatest.FlatSpec

class BucketSpec extends FlatSpec {

  "Bucket" should "compute position in Dgim window" in {
    assert(Bucket(0, 0).windowPos(10, 0) == 9)
    assert(Bucket(9, 0).windowPos(10, 0) == 8)
    assert(Bucket(1, 0).windowPos(10, 0) == 0)

    assert(Bucket(8, 0).windowPos(10, 8) == 9)
    assert(Bucket(3, 0).windowPos(10, 8) == 4)
    assert(Bucket(1, 0).windowPos(10, 8) == 2)
  }

  it should "compare position to another bucket" in {
    assert(!Bucket(0, 0).isEarlier(Bucket(5, 0), 10, 0))
    assert(Bucket(0, 0).isEarlier(Bucket(5, 0), 10, 5))
    assert(!Bucket(0, 0).isEarlier(Bucket(5, 0), 10, 3))
    assert(Bucket(0, 0).isEarlier(Bucket(5, 0), 10, 8))
  }

  it should "merge with another bucket of the same size" in {
    assert(Bucket(0, 0).merge(Bucket(5, 0), 10, 0) == Bucket(0, 1))
    assert(Bucket(0, 0).merge(Bucket(5, 0), 10, 5) == Bucket(5, 1))
    assert(Bucket(0, 0).merge(Bucket(5, 0), 10, 3) == Bucket(0, 1))
    assert(Bucket(0, 0).merge(Bucket(5, 0), 10, 8) == Bucket(5, 1))
  }
}