package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{DataType, Metadata}

/**
  */
abstract class FieldPoly3Type[T](name: String,
                                 dataType: DataType,
                                 nullValueAllowed: Boolean,
                                 xOrig: Double,
                                 yOrig: Double,
                                 nOrig: Double,
                                 xyScale: Double,
                                 nScale: Double,
                                 metadata: Metadata)
  extends FieldBytes(name, dataType, nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val blob = getByteBuffer(byteBuffer)

    val geomType = blob.getVarUInt

    val numPoints = blob.getVarUInt.toInt
    val numParts = blob.getVarUInt.toInt

    val xmin = blob.getVarUInt / xyScale + xOrig
    val ymin = blob.getVarUInt / xyScale + yOrig
    val xmax = blob.getVarUInt / xyScale + xmin
    val ymax = blob.getVarUInt / xyScale + ymin

    var dx = 0L
    var dy = 0L

    val xyNum = new Array[Int](numParts)
    val xyArr = new Array[Double](numPoints * 3)

    var i = 0
    if (numParts > 1) {
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
        0 until numXY foreach (_ => {
          dx += blob.getVarInt
          dy += blob.getVarInt
          val x = dx / xyScale + xOrig
          val y = dy / xyScale + yOrig
          xyArr(i) = x
          i += 1
          xyArr(i) = y
          i += 2
        })
      })
    }
    else {
      xyNum(0) = numPoints
      0 until numPoints foreach (_ => {
        dx += blob.getVarInt
        dy += blob.getVarInt
        xyArr(i) = dx / xyScale + xOrig
        i += 1
        xyArr(i) = dy / xyScale + yOrig
        i += 2
      })
    }
    i = 2
    var dn = 0L
    0 until numPoints foreach (_ => {
      dn += blob.getVarInt
      xyArr(i) = dn / nScale + nOrig
      i += 3
    })
    createPolyMType(xyNum, xyArr)
  }

  def createPolyMType(xyNum: Array[Int], xyArr: Array[Double]): T
}
