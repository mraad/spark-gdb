package com.esri.udt

import com.esri.core.geometry._
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  * PolylineType
  *
  * @param xyNum each element contains the number of xy pairs to read for a part
  * @param xyArr sequence of xy elements
  */
@SQLUserDefinedType(udt = classOf[PolylineUDT])
class PolylineType(override val xmin: Double,
                   override val ymin: Double,
                   override val xmax: Double,
                   override val ymax: Double,
                   override val xyNum: Array[Int],
                   override val xyArr: Array[Double]
                  ) extends PolyType(xmin, ymin, xmax, ymax, xyNum, xyArr) {

  @transient override lazy val asGeometry: Geometry = {
    val polyline = new Polyline()
    var i = 0
    xyNum.foreach(p => {
      0 until p foreach (n => {
        val x = xyArr(i)
        i += 1
        val y = xyArr(i)
        i += 1
        n match {
          case 0 => polyline.startPath(x, y)
          case _ => polyline.lineTo(x, y)
        }
      })
    })
    polyline
  }

  override def equals(other: Any): Boolean = other match {
    case that: PolylineType => equalsType(that)
    case _ => false
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

  def apply(geometry: Geometry) = geometry match {
    case line: Line => {
      val envp = new Envelope2D()
      line.queryEnvelope2D(envp)
      val xyNum = Array(1)
      val xyArr = Array(line.getEndX, line.getStartY, line.getEndX, line.getEndY)
      new PolylineType(envp.xmin, envp.ymin, envp.xmax, envp.ymax, xyNum, xyArr)
    }
    case multiPath: MultiPath => {
      val envp = new Envelope2D()
      multiPath.queryEnvelope2D(envp)
      val pathCount = multiPath.getPathCount
      val xyNum = (0 until pathCount).map(pathIndex => multiPath.getPathSize(pathIndex)).toArray
      val point2D = new Point2D()
      val numPoints = multiPath.getPointCount
      val xyArr = new Array[Double](numPoints * 2)
      var i = 0
      (0 until numPoints).map(pointIndex => {
        multiPath.getXY(pointIndex, point2D)
        xyArr(i) = point2D.x
        i += 1
        xyArr(i) = point2D.y
        i += 1
      })
      new PolylineType(envp.xmin, envp.ymin, envp.xmax, envp.ymax, xyNum, xyArr)
    }
    case _ => throw new RuntimeException(s"PolylineType::cannot create instance from $geometry")
  }


  def unapply(p: PolylineType) =
    Some((p.xmin, p.ymin, p.xmax, p.ymax, p.xyNum, p.xyArr))
}
