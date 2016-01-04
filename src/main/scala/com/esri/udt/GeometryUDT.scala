package com.esri.udt

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  */
@SQLUserDefinedType(udt = classOf[ShapeJTS])
case class GeometryUDT(val geometry: Geometry)