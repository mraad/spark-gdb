package com.esri.udt

import com.esri.core.geometry.{Envelope2D, Geometry, Point2D, Polygon}
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  * PolygonType
  *
  * @param xyNum each element contains the number of xy pairs to read for a part
  * @param xyArr sequence of xy elements
  */
@SQLUserDefinedType(udt = classOf[PolygonUDT])
class PolygonType(override val xmin: Double,
                  override val ymin: Double,
                  override val xmax: Double,
                  override val ymax: Double,
                  override val xyNum: Array[Int],
                  override val xyArr: Array[Double])
  extends PolyType(xmin, ymin, xmax, ymax, xyNum, xyArr) {

  @transient override lazy val asGeometry: Geometry = {
    val polygon = new Polygon()
    var i = 0
    xyNum.foreach(p => {
      0 until p foreach (n => {
        val x = xyArr(i)
        i += 1
        val y = xyArr(i)
        i += 1
        n match {
          case 0 => polygon.startPath(x, y)
          case _ => polygon.lineTo(x, y)
        }
      })
    })
    polygon.closeAllPaths()
    polygon
  }
}

object PolygonType {
  def apply(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xyNum: Array[Int], xyArr: Array[Double]) = {
    new PolygonType(xmin, ymin, xmax, ymax, xyNum, xyArr)
  }

  def apply(geometry: Geometry) = geometry match {
    case polygon: Polygon => {
      val envp = new Envelope2D()
      polygon.queryEnvelope2D(envp)
      val pathCount = polygon.getPathCount
      val xyNum = (0 until pathCount).map(pathIndex => polygon.getPathSize(pathIndex)).toArray
      val point2D = new Point2D()
      val numPoints = polygon.getPointCount
      val xyArr = new Array[Double](numPoints * 2)
      var i = 0
      // TODO - use fold
      (0 until numPoints).foreach(pointIndex => {
        polygon.getXY(pointIndex, point2D)
        xyArr(i) = point2D.x
        i += 1
        xyArr(i) = point2D.y
        i += 1
      })
      new PolygonType(envp.xmin, envp.ymin, envp.xmax, envp.ymax, xyNum, xyArr)
    }
    case _ => throw new RuntimeException(s"Cannot create instance of PolygonType from $geometry")
  }

  def unapply(p: PolygonType) =
    Some((p.xmin, p.ymin, p.xmax, p.ymax, p.xyNum, p.xyArr))
}
