package com.esri.udt

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esri.core.geometry.Point
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  */
@SQLUserDefinedType(udt = classOf[PointUDT])
class PointType(var x: Double = 0.0, var y: Double = 0.0) extends SpatialType with KryoSerializable {

  @transient lazy override val asGeometry = new Point(x, y)

  def canEqual(other: Any): Boolean = other.isInstanceOf[PointType]

  override def equals(other: Any): Boolean = other match {
    case that: PointType =>
      (that canEqual this) &&
        x == that.x &&
        y == that.y
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(x, y)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"PointType($x, $y)"

  // TODO - Worth it ??, pass in precision
  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeDouble(x)
    output.writeDouble(y)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    x = input.readDouble()
    y = input.readDouble()
  }
}

object PointType {
  def apply(x: Double, y: Double) = new PointType(x, y)

  def unapply(p: PointType) =
    Some((p.x, p.y))

}
