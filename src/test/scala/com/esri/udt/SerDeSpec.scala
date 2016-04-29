package com.esri.udt

import org.scalatest._

import scala.util.Random

/**
  */
final class SerDeSpec extends FlatSpec with Matchers {

  val rand = new Random(System.currentTimeMillis())

  it should "SerDe PointUDT" in {

    val x = rand.nextDouble
    val y = rand.nextDouble

    val udt = new PointUDT
    val p = udt.deserialize(udt.serialize(new PointType(x, y)))

    p match {
      case PointType(px, py) => {
        px shouldBe x
        py shouldBe y
      }
    }
    p should equal(new PointType(x, y))
  }

  it should "SerDe PointZUDT" in {

    val x = rand.nextDouble
    val y = rand.nextDouble
    val z = rand.nextDouble

    val udt = new PointZUDT
    val p = udt.deserialize(udt.serialize(new PointZType(x, y, z)))

    p match {
      case PointZType(px, py, pz) => {
        px shouldBe x
        py shouldBe y
        pz shouldBe z
      }
    }
    p should equal(new PointZType(x, y, z))
  }

  it should "SerDe PointMUDT" in {

    val x = rand.nextDouble
    val y = rand.nextDouble
    val m = rand.nextDouble

    val udt = new PointMUDT
    val p = udt.deserialize(udt.serialize(new PointMType(x, y, m)))

    p match {
      case PointMType(px, py, pm) => {
        px shouldBe x
        py shouldBe y
        pm shouldBe m
      }
    }
    p should equal(new PointMType(x, y, m))
  }

  it should "SerDe PointZMUDT" in {

    val x = rand.nextDouble
    val y = rand.nextDouble
    val z = rand.nextDouble
    val m = rand.nextDouble

    val udt = new PointZMUDT
    val p = udt.deserialize(udt.serialize(new PointZMType(x, y, z, m)))

    p match {
      case PointZMType(px, py, pz, pm) => {
        px shouldBe x
        py shouldBe y
        pz shouldBe z
        pm shouldBe m
      }
    }
    p should equal(new PointZMType(x, y, z, m))
  }

  it should "SerDe PolylineUDT" in {
    val xmin = rand.nextDouble
    val ymin = rand.nextDouble
    val xmax = rand.nextDouble
    val ymax = rand.nextDouble
    val udt = new PolylineUDT
    val p = udt.deserialize(udt.serialize(new PolylineType(xmin, ymin, xmax, ymax, Array(1), Array(xmin, ymin, xmax, ymax))))

    p match {
      case PolylineType(pxmin, pymin, pxmax, pymax, pxyNum, pxyArr) => {
        pxmin shouldBe xmin
        pymin shouldBe ymin
        pxmax shouldBe xmax
        pymax shouldBe ymax
        pxyNum shouldBe Array(1)
        pxyArr shouldBe Array(xmin, ymin, xmax, ymax)
      }
    }
    p should equal(new PolylineType(xmin, ymin, xmax, ymax, Array(1), Array(xmin, ymin, xmax, ymax)))
  }

  it should "SerDe PolylineMUDT" in {
    val xmin = rand.nextDouble
    val ymin = rand.nextDouble
    val xmax = rand.nextDouble
    val ymax = rand.nextDouble
    val m1 = rand.nextDouble
    val m2 = rand.nextDouble
    val udt = new PolylineMUDT
    val p = udt.deserialize(udt.serialize(new PolylineMType(xmin, ymin, xmax, ymax, Array(1), Array(xmin, ymin, m1, xmax, ymax, m2))))

    p match {
      case PolylineMType(pxmin, pymin, pxmax, pymax, pxyNum, pxyArr) => {
        pxmin shouldBe xmin
        pymin shouldBe ymin
        pxmax shouldBe xmax
        pymax shouldBe ymax
        pxyNum shouldBe Array(1)
        pxyArr shouldBe Array(xmin, ymin, m1, xmax, ymax, m2)
      }
    }
    p should equal(new PolylineMType(xmin, ymin, xmax, ymax, Array(1), Array(xmin, ymin, m1, xmax, ymax, m2)))
  }

  it should "SerDe PolygonUDT" in {
    val xmin = rand.nextDouble
    val ymin = rand.nextDouble
    val xmax = rand.nextDouble
    val ymax = rand.nextDouble
    val udt = new PolygonUDT
    val p = udt.deserialize(udt.serialize(new PolygonType(xmin, ymin, xmax, ymax, Array(1), Array(xmin, ymin, xmax, ymax))))

    p match {
      case PolygonType(pxmin, pymin, pxmax, pymax, pxyNum, pxyArr) => {
        pxmin shouldBe xmin
        pymin shouldBe ymin
        pxmax shouldBe xmax
        pymax shouldBe ymax
        pxyNum shouldBe Array(1)
        pxyArr shouldBe Array(xmin, ymin, xmax, ymax)
      }
    }
    p should equal(new PolygonType(xmin, ymin, xmax, ymax, Array(1), Array(xmin, ymin, xmax, ymax)))
  }
}
