package com.esri.udt

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.types._

/**
  */
class ShapeJTS(shapeName: String) extends UserDefinedType[GeometryUDT] {

  override def typeName = shapeName

  // override def pyUDT = "com.esri.udt.ShapeJTS"

  override def userClass = classOf[GeometryUDT]

  override def asNullable = this

  override def sqlType = BinaryType

  override def serialize(obj: Any): GeometryUDT = {
    obj match {
      case g: GeometryUDT => g
      case g: Geometry => GeometryUDT(g)
    }
  }

  override def deserialize(datum: Any): GeometryUDT = {
    datum match {
      case g: GeometryUDT => g
    }
  }

  override def equals(o: Any): Boolean = {
    o match {
      case v: ShapeJTS => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[ShapeJTS].getName.hashCode()

  override def toString = s"ShapeJTS($shapeName)"
}

case object ShapeJTS {
  def apply(shapeName: String) = new ShapeJTS(shapeName)
}
