package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
  */
class PointZMUDT extends UserDefinedType[PointZMType] {

  override def sqlType: DataType = StructType(Seq(
    StructField("x", DoubleType, false),
    StructField("y", DoubleType, false),
    StructField("z", DoubleType, false),
    StructField("m", DoubleType, false)
  ))

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case PointZMType(x, y, z, m) => {
        val row = new GenericMutableRow(4)
        row.setDouble(0, x)
        row.setDouble(1, y)
        row.setDouble(2, z)
        row.setDouble(3, m)
        row
      }
    }
  }

  override def deserialize(datum: Any): PointZMType = {
    datum match {
      case row: InternalRow => PointZMType(row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3))
    }
  }

  override def userClass: Class[PointZMType] = classOf[PointZMType]

  override def pyUDT: String = "com.esri.udt.PointZMUDT"

  override def typeName: String = "pointZM"

  override def equals(o: Any): Boolean = {
    o match {
      case v: PointZMUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[PointZMUDT].getName.hashCode()

  override def asNullable: PointZMUDT = this

}