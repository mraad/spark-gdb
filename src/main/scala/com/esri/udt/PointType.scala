package com.esri.udt

import com.esri.core.geometry._
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  */
@SQLUserDefinedType(udt = classOf[PointUDT])
class PointType(val x: Double = 0.0, val y: Double = 0.0) extends SpatialType {

  @transient lazy override val asGeometry = new Point(x, y)

  def ==(that: PointType) = this.x == that.x && this.y == that.y

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
  def apply(geometry: Geometry) = geometry match {
    case point: Point => new PointType(point.getX, point.getY)
    case line: Line => {
      val x = (line.getStartX + line.getEndX) / 2.0
      val y = (line.getStartY + line.getEndX) / 2.0
      new PointType(x, y)
    }
    case polyline: Polyline => {
      polyline.getPointCount match {
        case 2 => {
          val arr = polyline.getCoordinates2D
          val p0 = arr(0)
          val p1 = arr(1)
          new PointType((p0.x + p1.x) * 0.5, (p0.y + p1.y) * 0.5)
        }
        case n => {
          val xy = polyline.getXY(n / 2)
          new PointType(xy.x, xy.y)
        }
      }
    }
    case _ => {
      // TODO - Cheap way out here !!!
      val envp = new Envelope2D()
      geometry.queryEnvelope2D(envp)
      new PointType(envp.getCenterX, envp.getCenterY)
    }
  }

  def apply(x: Double, y: Double) = new PointType(x, y)

  def unapply(p: PointType) = Some((p.x, p.y))
}
