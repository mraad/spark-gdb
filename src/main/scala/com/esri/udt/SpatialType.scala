package com.esri.udt

import com.esri.core.geometry.Geometry

trait SpatialType extends Serializable {
  def asGeometry(): Geometry
}
