package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow

/**
  */
class PolygonUDT extends PolyUDT[PolygonType] {
  /*
    override def sqlType: DataType = StructType(Seq(
      StructField("xmin", DoubleType, false),
      StructField("ymin", DoubleType, false),
      StructField("xmax", DoubleType, false),
      StructField("ymax", DoubleType, false),
      StructField("xyNum", ArrayType(IntegerType, false), false),
      StructField("xyArr", ArrayType(DoubleType, false), false)
    ))
  */

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case PolygonType(xmin, ymin, xmax, ymax, xyNum, xyArr) => {
        /*
                val row = new GenericMutableRow(6)
                row.setDouble(0, xmin)
                row.setDouble(1, ymin)
                row.setDouble(2, xmax)
                row.setDouble(3, ymax)
                row.update(4, new GenericArrayData(xyNum.map(_.asInstanceOf[Any])))
                row.update(5, new GenericArrayData(xyArr.map(_.asInstanceOf[Any])))
                row
        */
        serialize(xmin, ymin, xmax, ymax, xyNum, xyArr)
      }
    }
  }

  override def deserialize(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xyNum: Array[Int], xyArr: Array[Double]) = {
    PolygonType(xmin, ymin, xmax, ymax, xyNum, xyArr)
  }

  /*
    override def deserialize(datum: Any): PolygonType = {
      datum match {
        case row: InternalRow => {
          val xmin = row.getDouble(0)
          val ymin = row.getDouble(1)
          val xmax = row.getDouble(2)
          val ymax = row.getDouble(3)
          val xyNum = row.getArray(4).toIntArray()
          val xyArr = row.getArray(5).toDoubleArray()
          PolygonType(xmin, ymin, xmax, ymax, xyNum, xyArr)
        }
      }
    }
  */

  override def userClass: Class[PolygonType] = classOf[PolygonType]

  override def pyUDT: String = "com.esri.udt.PolygonUDT"

  override def typeName: String = "polygon"

  override def equals(o: Any): Boolean = {
    o match {
      case v: PolygonUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[PolygonUDT].getName.hashCode()

  override def asNullable: PolygonUDT = this

}
