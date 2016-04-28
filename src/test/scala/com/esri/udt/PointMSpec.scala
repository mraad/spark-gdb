package com.esri.udt

import org.scalatest._

/**
  */
class PointMSpec extends FlatSpec with Matchers {
  it should "serialize/deserialize" in {
    val udt = new PointZMUDT()
    val p = udt.deserialize(udt.serialize(new PointZMType(0.0, 1.0, 2.0, 3.0)))

    p.x shouldBe 0.0
    p.y shouldBe 1.0
    p.z shouldBe 2.0
    p.m shouldBe 3.0

    p should equal(new PointZMType(0.0, 1.0, 2.0, 3.0))
  }
}
