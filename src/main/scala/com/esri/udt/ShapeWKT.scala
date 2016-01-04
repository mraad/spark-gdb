package com.esri.udt

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.{WKTReader, WKTWriter}
import org.apache.spark.sql.types._

/**
  */
class ShapeWKT(shapeName: String, xyTolerance: Double) extends UserDefinedType[GeometryUDT] {

  @transient
  private lazy val geomFact = new GeometryFactory(new PrecisionModel(1.0 / xyTolerance))
  @transient
  private lazy val writer = new WKTWriter(2)
  @transient
  private lazy val reader = new WKTReader(geomFact)

  override def typeName = shapeName

  override def pyUDT = "com.esri.udt.ShapeWKT"

  override def userClass = classOf[GeometryUDT]

  override def asNullable = this

  override def sqlType = StringType

  override def serialize(obj: Any): String = {
    obj match {
      case g: Geometry => writer.write(g)
      case g: GeometryUDT => writer.write(g.geometry)
    }
  }

  override def deserialize(datum: Any): GeometryUDT = {
    datum match {
      case text: String => GeometryUDT(reader.read(text))
      case g: GeometryUDT => g
    }
  }

  override def equals(o: Any): Boolean = {
    o match {
      case v: ShapeWKT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[ShapeWKT].getName.hashCode()

  override def toString = s"ShapeWKT($shapeName)"
}

case object ShapeWKT {
  def apply(shapeName: String, xyTolerance: Double) = new ShapeWKT(shapeName, xyTolerance)
}
