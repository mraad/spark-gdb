package com.esri.udt

import com.esri.core.geometry.Geometry

/**
  * PolylineType
  *
  * @param xyNum each element contains the number of xy pairs to read for a part
  * @param xyArr sequence of xy elements
  */
class PolylineType(val xmin: Double,
                   val ymin: Double,
                   val xmax: Double,
                   val ymax: Double,
                   val xyNum: Array[Int],
                   val xyArr: Array[Double]
                  ) extends GeometryType {

  @transient override lazy val asGeometry: Geometry = ???

  /*
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
  */

  override def equals(other: Any): Boolean = other match {
    case that: PolylineType =>
      xmin == that.xmin && ymin == that.ymin &&
        xmax == that.xmax && ymax == that.ymax &&
        xyArr.sameElements(that.xyArr)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(xmin, ymin, xmax, ymax)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object PolylineType {
  def apply(xmin: Double,
            ymin: Double,
            xmax: Double,
            ymax: Double,
            xyNum: Array[Int],
            xyArr: Array[Double]
           ) = {
    new PolylineType(xmin, ymin, xmax, ymax, xyNum, xyArr)
  }

  def unapply(p: PolylineType) =
    Some((p.xmin, p.ymin, p.xmax, p.ymax, p.xyNum, p.xyArr))
}
