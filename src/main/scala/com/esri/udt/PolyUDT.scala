package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
  */
abstract class PolyUDT[T] extends UserDefinedType[T] {

  override def sqlType: DataType = StructType(Seq(
    StructField("xmin", DoubleType, false),
    StructField("ymin", DoubleType, false),
    StructField("xmax", DoubleType, false),
    StructField("ymax", DoubleType, false),
    StructField("xyNum", ArrayType(IntegerType, false), false),
    StructField("xyArr", ArrayType(DoubleType, false), false)
  ))

  def serialize(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xyNum: Array[Int], xyArr: Array[Double]) = {
    val row = new GenericMutableRow(6)
    /* Spark 1.5
    row.update(4, new GenericArrayData(xyNum.map(_.asInstanceOf[Any])))
    row.update(5, new GenericArrayData(xyArr.map(_.asInstanceOf[Any])))
    */
    /*
        For Spark 1.6
    */
    row.update(0, xmin)
    row.update(1, ymin)
    row.update(2, xmax)
    row.update(3, ymax)
    row.update(4, new org.apache.spark.sql.catalyst.util.GenericArrayData(xyNum.map(_.asInstanceOf[Any])))
    row.update(5, new org.apache.spark.sql.catalyst.util.GenericArrayData(xyArr.map(_.asInstanceOf[Any])))
    row
  }

  def deserialize(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xyNum: Array[Int], xyArr: Array[Double]): T

  override def deserialize(datum: Any): T = {
    datum match {
      case row: InternalRow => {
        val xmin = row.getDouble(0)
        val ymin = row.getDouble(1)
        val xmax = row.getDouble(2)
        val ymax = row.getDouble(3)
        val xyNum = row.getArray(4).toIntArray()
        val xyArr = row.getArray(5).toDoubleArray()
        deserialize(xmin, ymin, xmax, ymax, xyNum, xyArr)
      }
    }
  }

}
