package com.esri.udt

import com.esri.core.geometry._
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  */
@SQLUserDefinedType(udt = classOf[PointZMUDT])
class PointZMType(val x: Double = 0.0, val y: Double = 0.0, val z: Double = 0.0, val m: Double = 0.0) extends SpatialType {

  /*@transient lazy override val*/ def asGeometry() = asPoint()

  def asPoint() = {
    val p = new Point(x, y, z)
    p.setM(m)
    p
  }

  def ==(that: PointZMType) = this.x == that.x && this.y == that.y && this.z == that.z && this.m == that.m

  override def equals(other: Any): Boolean = other match {
    case that: PointZMType => this == that
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(x, y, z, m).foldLeft(0)((a, b) => {
      val bits = java.lang.Double.doubleToLongBits(b)
      31 * a + (bits ^ (bits >>> 32)).toInt
    })
  }

  override def toString = s"PointZMType($x,$y,$z,$m)"

}

object PointZMType {
  def apply(geometry: Geometry) = geometry match {
    case point: Point => new PointZMType(point.getX, point.getY, point.getZ, point.getM)
    case _ => throw new RuntimeException(s"Cannot construct PointZMType from ${geometry.toString}")

  }

  def apply(x: Double, y: Double, z: Double, m: Double) = new PointZMType(x, y, z, m)

  def unapply(p: PointZMType) = Some((p.x, p.y, p.z, p.m))
}
