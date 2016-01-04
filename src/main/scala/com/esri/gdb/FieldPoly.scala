package com.esri.gdb

import java.nio.ByteBuffer

import com.vividsolutions.jts.geom.Coordinate
import org.apache.spark.sql.types.{Metadata, DataType}

/**
  */
abstract class FieldPoly(name: String,
                         dataType: DataType,
                         nullValueAllowed: Boolean,
                         xorig: Double,
                         yorig: Double,
                         xyscale: Double,
                         metadata: Metadata
                        )
  extends FieldGeom(name, dataType, nullValueAllowed, xorig, yorig, xyscale, metadata) {

  protected var dx = 0L
  protected var dy = 0L

  def getCoordinates(byteBuffer: ByteBuffer, numCoordinates: Int) = {
    val coordinates = new Array[Coordinate](numCoordinates)
    0 until numCoordinates foreach (n => {
      dx += byteBuffer.getVarInt
      dy += byteBuffer.getVarInt
      val x = dx / xyscale + xorig
      val y = dy / xyscale + yorig
      coordinates(n) = new Coordinate(x, y)
    })
    coordinates
  }

}
