package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
  */
abstract class PolyUDT[T] extends UserDefinedType[T] {

  override def sqlType: DataType = StructType(Seq(
    StructField("xyNum", ArrayType(IntegerType, false), false),
    StructField("xyArr", ArrayType(DoubleType, false), false)
  ))

  def serialize(xyNum: Array[Int], xyArr: Array[Double]) = {
    val row = new GenericMutableRow(6)
    /* Spark 1.5
    row.update(4, new GenericArrayData(xyNum.map(_.asInstanceOf[Any])))
    row.update(5, new GenericArrayData(xyArr.map(_.asInstanceOf[Any])))
    */
    /*
        For Spark 1.6
    */
    row.update(0, new org.apache.spark.sql.catalyst.util.GenericArrayData(xyNum.map(_.asInstanceOf[Any])))
    row.update(1, new org.apache.spark.sql.catalyst.util.GenericArrayData(xyArr.map(_.asInstanceOf[Any])))
    row
  }

  def deserialize(xyNum: Array[Int], xyArr: Array[Double]): T

  override def deserialize(datum: Any): T = {
    datum match {
      case row: InternalRow => {
        val xyNum = row.getArray(0).toIntArray()
        val xyArr = row.getArray(1).toDoubleArray()
        deserialize(xyNum, xyArr)
      }
    }
  }

}
