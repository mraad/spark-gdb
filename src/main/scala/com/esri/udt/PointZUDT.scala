package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
  */
class PointZUDT extends UserDefinedType[PointZType] {

  override def sqlType: DataType = StructType(Seq(
    StructField("x", DoubleType, false),
    StructField("y", DoubleType, false),
    StructField("z", DoubleType, false)
  ))

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case PointZType(x, y, z) => {
        val row = new GenericMutableRow(3)
        row.setDouble(0, x)
        row.setDouble(1, y)
        row.setDouble(2, z)
        row
      }
    }
  }

  override def deserialize(datum: Any): PointZType = {
    datum match {
      case row: InternalRow => PointZType(row.getDouble(0), row.getDouble(1), row.getDouble(2))
    }
  }

  override def userClass: Class[PointZType] = classOf[PointZType]

  override def pyUDT: String = "com.esri.udt.PointZUDT"

  override def typeName: String = "pointZ"

  override def equals(o: Any): Boolean = {
    o match {
      case v: PointZUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[PointZUDT].getName.hashCode()

  override def asNullable: PointZUDT = this

}