package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
  */
class PolylineUDT extends UserDefinedType[PolylineType] {
  override def sqlType: DataType = StructType(Seq(
    StructField("xmin", DoubleType, false),
    StructField("ymin", DoubleType, false),
    StructField("xmax", DoubleType, false),
    StructField("ymax", DoubleType, false),
    StructField("xyNum", ArrayType(IntegerType, false), false),
    StructField("xyArr", ArrayType(DoubleType, false), false)
  ))

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case PolylineType(xmin, ymin, xmax, ymax, xyNum, xyArr) => {
        val row = new GenericMutableRow(6)
        row.setDouble(0, xmin)
        row.setDouble(1, ymin)
        row.setDouble(2, xmax)
        row.setDouble(3, ymax)
        row.update(4, new GenericArrayData(xyNum.map(_.asInstanceOf[Any])))
        row.update(5, new GenericArrayData(xyArr.map(_.asInstanceOf[Any])))
        row
      }
    }
  }

  override def deserialize(datum: Any): PolylineType = {
    datum match {
      case row: InternalRow => {
        val xmin = row.getDouble(0)
        val ymin = row.getDouble(1)
        val xmax = row.getDouble(2)
        val ymax = row.getDouble(3)
        val xyNum = row.getArray(4).toIntArray()
        val xyArr = row.getArray(5).toDoubleArray()
        PolylineType(xmin, ymin, xmax, ymax, xyNum, xyArr)
      }
    }
  }

  override def userClass: Class[PolylineType] = classOf[PolylineType]

  override def pyUDT: String = "com.esri.udt.PolylineUDT"

  override def typeName: String = "polyline"

  override def equals(o: Any): Boolean = {
    o match {
      case v: PolylineUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[PolylineUDT].getName.hashCode()

  override def asNullable: PolylineUDT = this

}
