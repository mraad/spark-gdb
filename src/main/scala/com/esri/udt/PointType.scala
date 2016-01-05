package com.esri.udt

import com.esri.core.geometry.Point
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  */
@SQLUserDefinedType(udt = classOf[PointUDT])
class PointType(val x: Double = 0.0, val y: Double = 0.0) extends GeometryType {

  override def toGeometry() = new Point(x, y)

  def canEqual(other: Any): Boolean = other.isInstanceOf[PointType]

  override def equals(other: Any): Boolean = other match {
    case that: PointType =>
      (that canEqual this) &&
        x == that.x &&
        y == that.y
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(x, y)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"PointType($x, $y)"
}

object PointType {
  def apply(x: Double, y: Double) = new PointType(x, y)
}
