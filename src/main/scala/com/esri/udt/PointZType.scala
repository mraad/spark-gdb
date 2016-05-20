package com.esri.udt

import com.esri.core.geometry._
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  */
@SQLUserDefinedType(udt = classOf[PointZUDT])
class PointZType(val x: Double = 0.0, val y: Double = 0.0, val z: Double = 0.0) extends SpatialType {

  /*@transient lazy override val*/ def asGeometry() = asPoint()

  def asPoint() = {
    new Point(x, y, z)
  }

  def ==(that: PointZType) = this.x == that.x && this.y == that.y && this.z == that.z

  override def equals(other: Any): Boolean = other match {
    case that: PointZType => this == that
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(x, y, z).foldLeft(0)((a, b) => {
      val bits = java.lang.Double.doubleToLongBits(b)
      31 * a + (bits ^ (bits >>> 32)).toInt
    })
  }

  override def toString = s"PointZType($x,$y,$z)"

}

object PointZType {
  def apply(geometry: Geometry) = geometry match {
    case point: Point => new PointZType(point.getX, point.getY, point.getM)
    case _ => throw new RuntimeException(s"Cannot construct PointZType from ${geometry.toString}")
  }

  def apply(x: Double, y: Double, z: Double) = new PointZType(x, y, z)

  def unapply(p: PointZType) = Some((p.x, p.y, p.z))
}
