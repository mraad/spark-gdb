package com.esri.gdb

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * Provides access to FileGDB data from pure SQL statements.
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider {
  /**
    * Creates a new relation for data store in FileGDB given parameters.
    * Parameters must include 'path' and 'name'.
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]
                             ): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
    * Creates a new relation for data store in FileGDB given parameters and user supported schema.
    * Parameters must include 'path' and 'name'.
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType
                             ): BaseRelation = {
    val path = parameters.getOrElse("path", sys.error("Parameter 'path' must be defined."))
    val name = parameters.getOrElse("name", sys.error("Parameter 'name' must be defined."))
    val numPartitions = parameters.getOrElse("numPartitions", "8").toInt
    GDBRelation(path, name, numPartitions)(sqlContext)
  }
}
