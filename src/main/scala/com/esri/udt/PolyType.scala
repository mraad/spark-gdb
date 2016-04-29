package com.esri.udt

/**
  */
abstract class PolyType(val xmin: Double,
                        val ymin: Double,
                        val xmax: Double,
                        val ymax: Double,
                        val xyNum: Array[Int],
                        val xyArr: Array[Double]
                       ) extends SpatialType {

  def ==(that: PolyType) = {
    xmin == that.xmin &&
      ymin == that.ymin &&
      xmax == that.xmax &&
      ymax == that.ymax &&
      xyNum.sameElements(that.xyNum) &&
      xyArr.sameElements(that.xyArr)
  }

  override def hashCode(): Int = {
    Seq(xmin, ymin, xmax, ymax).foldLeft(0)((a, b) => {
      val bits = java.lang.Double.doubleToLongBits(b)
      31 * a + (bits ^ (bits >>> 32)).toInt
    })
  }

  override def equals(other: Any): Boolean = other match {
    case that: PolyType => this == that
    case _ => false
  }

  override def toString = "%s%s".format(getClass.getSimpleName, xyArr.mkString("(", ",", ")"))
}
