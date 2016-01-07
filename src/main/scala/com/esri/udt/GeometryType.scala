package com.esri.udt

import com.esri.core.geometry.Geometry

/**
  */
trait GeometryType extends Serializable {
  val asGeometry: Geometry
}
