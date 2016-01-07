package com.esri.udt

import com.esri.core.geometry.Point
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  */
@SQLUserDefinedType(udt = classOf[PointUDT])
class PointType(val x: Double = 0.0, val y: Double = 0.0) extends SpatialType {

  @transient lazy override val asGeometry = new Point(x, y)

  override def equals(other: Any): Boolean = other match {
    case that: PointType => x == that.x && y == that.y
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(x, y).foldLeft(0)((a, b) => {
      val bits = java.lang.Double.doubleToLongBits(b)
      31 * a + (bits ^ (bits >>> 32)).toInt
    })
  }

  override def toString = s"PointType($x, $y)"

}

object PointType {
  def apply(x: Double, y: Double) = new PointType(x, y)

  def unapply(p: PointType) = Some((p.x, p.y))
}
