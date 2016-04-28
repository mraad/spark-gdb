package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{DataType, Metadata}

/**
  */
abstract class FieldPolyType[T](name: String,
                                dataType: DataType,
                                nullValueAllowed: Boolean,
                                xOrig: Double,
                                yOrig: Double,
                                xyScale: Double,
                                metadata: Metadata)
  extends FieldBytes(name, dataType, nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val blob = getByteBuffer(byteBuffer)

    val geomType = blob getVarUInt

    val numPoints = blob.getVarUInt.toInt
    val numParts = blob.getVarUInt.toInt

    val xmin = blob.getVarUInt / xyScale + xOrig
    val ymin = blob.getVarUInt / xyScale + yOrig
    val xmax = blob.getVarUInt / xyScale + xmin
    val ymax = blob.getVarUInt / xyScale + ymin

    var dx = 0L
    var dy = 0L

    val xyNum = new Array[Int](numParts)
    val xyArr = new Array[Double](numPoints * 2)

    if (numParts > 1) {
      var i = 0
      var sum = 0
      1 to numParts foreach (partIndex => {
        if (partIndex == numParts) {
          xyNum(i) = numPoints - sum
        } else {
          val numXY = blob.getVarUInt.toInt
          xyNum(i) = numXY
          sum += numXY
          i += 1
        }
      })
      i = 0
      xyNum.foreach(numXY => {
        0 until numXY foreach (n => {
          dx += blob.getVarInt
          dy += blob.getVarInt
          val x = dx / xyScale + xOrig
          val y = dy / xyScale + yOrig
          xyArr(i) = x
          i += 1
          xyArr(i) = y
          i += 1
        })
      })
    }
    else {
      xyNum(0) = numPoints
      var i = 0
      0 until numPoints foreach (n => {
        dx += blob.getVarInt
        dy += blob.getVarInt
        val x = dx / xyScale + xOrig
        val y = dy / xyScale + yOrig
        xyArr(i) = x
        i += 1
        xyArr(i) = y
        i += 1
      })
    }
    createPolyType(xmin, ymin, xmax, ymax, xyNum, xyArr)
  }

  def createPolyType(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xyNum: Array[Int], xyArr: Array[Double]): T
}
