package com.esri.udt

import com.esri.core.geometry._
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  * PolylineMType
  *
  * @param xyNum each element contains the number of xy pairs to read for a part
  * @param xyArr sequence of xy elements
  */
@SQLUserDefinedType(udt = classOf[PolylineMUDT])
class PolylineMType(override val xyNum: Array[Int], override val xyArr: Array[Double])
  extends PolyType(xyNum, xyArr) {

  @transient override lazy val asGeometry: Geometry = {
    val polyline = new Polyline()
    var i = 0
    xyNum.foreach(p => {
      0 until p foreach (n => {
        val x = xyArr(i)
        i += 1
        val y = xyArr(i)
        i += 1
        val m = xyArr(i)
        i += 1
        n match {
          case 0 => val p = new Point(x, y); p.setM(m); polyline.startPath(p)
          case _ => val p = new Point(x, y); p.setM(m); polyline.lineTo(p)
        }
      })
    })
    polyline
  }
}

object PolylineMType {
  def apply(xyNum: Array[Int], xyArr: Array[Double]) = {
    new PolylineMType(xyNum, xyArr)
  }

  def apply(geometry: Geometry) = geometry match {
    case multiPath: MultiPath => {
      val pathCount = multiPath.getPathCount
      val xyNum = (0 until pathCount).map(pathIndex => multiPath.getPathSize(pathIndex)).toArray
      val numPoints = multiPath.getPointCount
      val point = new Point()
      val xyArr = new Array[Double](numPoints * 3)
      (0 until numPoints).foldLeft(0)((i, pointIndex) => {
        multiPath.getPoint(pointIndex, point)
        xyArr(i) = point.getX
        xyArr(i + 1) = point.getY
        xyArr(i + 2) = point.getM
        i + 3
      })
      new PolylineMType(xyNum, xyArr)
    }
    case _ => throw new RuntimeException(s"Cannot create instance of PolylineMType from $geometry")
  }

  def unapply(p: PolylineMType) =
    Some((p.xyNum, p.xyArr))
}
