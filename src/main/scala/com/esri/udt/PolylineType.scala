package com.esri.udt

import com.esri.core.geometry.Geometry

/**
  * PolylineType
  *
  * @param xyNum each element contains the number of xy pairs to read for a part
  * @param xyArr sequence of xy elements
  */
class PolylineType(val xyNum: Array[Int], val xyArr: Array[Double]) extends GeometryType {
  override def toGeometry(): Geometry = ???

  override def equals(other: Any): Boolean = other match {
    case that: PolylineType =>
      xyNum.sameElements(that.xyNum) && xyArr.sameElements(that.xyArr)
    case _ => false
  }

  override def hashCode(): Int = {
    xyArr.foldLeft(0)((a, b) => {
      val bits = java.lang.Double.doubleToLongBits(b)
      31 * a + (bits ^ (bits >>> 32)).toInt
    })
  }
}

object PolylineType {
  def apply(numParts: Array[Int], coords: Array[Double]) = {
    new PolylineType(numParts, coords)
  }
}
