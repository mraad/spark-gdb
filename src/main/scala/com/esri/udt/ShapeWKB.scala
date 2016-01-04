package com.esri.udt

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import org.apache.spark.sql.types._

/**
  */
class ShapeWKB(shapeName: String, xyTolerance: Double) extends UserDefinedType[GeometryUDT] {

  @transient
  private lazy val geomFact = new GeometryFactory(new PrecisionModel(1.0 / xyTolerance))
  @transient
  private lazy val wkbWriter = new WKBWriter(2)
  @transient
  private lazy val wkbReader = new WKBReader(geomFact)

  override def typeName = shapeName

  override def pyUDT = "com.esri.udt.ShapeWKB"

  override def userClass = classOf[GeometryUDT]

  override def asNullable = this

  override def sqlType = BinaryType

  override def serialize(obj: Any): Array[Byte] = {
    obj match {
      case g: Geometry => wkbWriter.write(g)
      case g: GeometryUDT => wkbWriter.write(g.geometry)
    }
  }

  override def deserialize(datum: Any): GeometryUDT = {
    datum match {
      case bytes: Array[Byte] => GeometryUDT(wkbReader.read(bytes))
      case g: GeometryUDT => g
    }
  }

  override def equals(o: Any): Boolean = {
    o match {
      case v: ShapeWKB => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[ShapeWKB].getName.hashCode()

  override def toString = s"ShapeWKB($shapeName)"
}

case object ShapeWKB {
  def apply(shapeName: String, xyTolerance: Double) = new ShapeWKB(shapeName, xyTolerance)
}
