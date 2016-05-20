package com.esri.udt

import org.scalatest._

/**
  */
class SegmentIteratorSpec extends FlatSpec with Matchers {

  it should "match no parts" in {
    val polylineM = PolylineMType(0.0, 0.0, 0.0, 0.0, Array.empty[Int], Array.empty[Double])
    SegmentIterator(polylineM).hasNext shouldBe false
  }

  it should "match 0 parts" in {
    val polylineM = PolylineMType(0.0, 0.0, 0.0, 0.0, Array(0), Array.empty[Double])
    SegmentIterator(polylineM).hasNext shouldBe false
  }

  it should "match 1 part with 1 segment" in {
    val polylineM = PolylineMType(0.0, 0.0, 0.0, 0.0, Array(2), Array(10, 20, 30, 20, 30, 40))

    val segmentIterator = SegmentIterator(polylineM)
    segmentIterator.hasNext shouldBe true
    segmentIterator.next shouldBe Segment(10, 20, 30, 20, 30, 40)
    segmentIterator.hasNext shouldBe false
  }

  it should "match 1 part with 2 segments" in {
    val polylineM = PolylineMType(0.0, 0.0, 0.0, 0.0, Array(3), Array(10, 20, 30, 20, 30, 40, 30, 40, 50))

    val segmentIterator = SegmentIterator(polylineM)
    segmentIterator.hasNext shouldBe true
    segmentIterator.next shouldBe Segment(10, 20, 30, 20, 30, 40)
    segmentIterator.hasNext shouldBe true
    segmentIterator.next shouldBe Segment(20, 30, 40, 30, 40, 50)
    segmentIterator.hasNext shouldBe false
  }

  it should "match 2 parts with 1 segment" in {

    val polylineM = new PolylineMType(0.0, 0.0, 0.0, 0.0, Array(2, 2), Array(10, 20, 30, 20, 30, 40, 20, 30, 40, 30, 40, 50))

    val segmentIterator = SegmentIterator(polylineM)
    segmentIterator.hasNext shouldBe true
    segmentIterator.next() shouldBe Segment(10, 20, 30, 20, 30, 40)

    segmentIterator.hasNext shouldBe true
    segmentIterator.next() shouldBe Segment(20, 30, 40, 30, 40, 50)

    segmentIterator.hasNext shouldBe false
  }

  it should "match 2 parts with 3,2 segments" in {

    val polylineM = new PolylineMType(0.0, 0.0, 0.0, 0.0,
      Array(4, 3),
      Array(
        10, 20, 30,
        20, 30, 40,
        30, 40, 50,
        40, 50, 60,
        20, 30, 40,
        30, 40, 50,
        40, 50, 60))

    val segmentIterator = SegmentIterator(polylineM)

    segmentIterator.hasNext shouldBe true
    segmentIterator.next() shouldBe Segment(10, 20, 30, 20, 30, 40)

    segmentIterator.hasNext shouldBe true
    segmentIterator.next() shouldBe Segment(20, 30, 40, 30, 40, 50)

    segmentIterator.hasNext shouldBe true
    segmentIterator.next() shouldBe Segment(30, 40, 50, 40, 50, 60)

    segmentIterator.hasNext shouldBe true
    segmentIterator.next() shouldBe Segment(20, 30, 40, 30, 40, 50)

    segmentIterator.hasNext shouldBe true
    segmentIterator.next() shouldBe Segment(30, 40, 50, 40, 50, 60)

    segmentIterator.hasNext shouldBe false
  }

  it should "match 3 parts with 1,1,1 segments" in {

    val polylineM = new PolylineMType(0.0, 0.0, 0.0, 0.0,
      Array(2, 2, 2),
      Array(
        10, 20, 30,
        20, 30, 40,
        30, 40, 50,
        40, 50, 60,
        20, 30, 40,
        30, 40, 50))

    val segmentIterator = SegmentIterator(polylineM)

    segmentIterator.hasNext shouldBe true
    segmentIterator.next() shouldBe Segment(10, 20, 30, 20, 30, 40)

    segmentIterator.hasNext shouldBe true
    segmentIterator.next() shouldBe Segment(30, 40, 50, 40, 50, 60)

    segmentIterator.hasNext shouldBe true
    segmentIterator.next() shouldBe Segment(20, 30, 40, 30, 40, 50)

    segmentIterator.hasNext shouldBe false
  }
}
