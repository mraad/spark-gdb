package com.esri.gdb

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  */
case class GDBRelation(gdbPath: String,
                       gdbName: String,
                       serde: String,
                       numPartition: Int
                      )(@transient val sqlContext: SQLContext) extends BaseRelation with Logging with TableScan {

  override val schema = inferSchema()

  private def inferSchema() = {
    val sc = sqlContext.sparkContext
    GDBTable.findTable(gdbPath, gdbName, sc.hadoopConfiguration) match {
      case Some(catTab) => {
        val table = GDBTable(gdbPath, catTab.hexName, serde, sc.hadoopConfiguration)
        try {
          table.schema()
        } finally {
          table.close()
        }
      }
      case _ => {
        log.error(s"Cannot find '$gdbName' in $gdbPath, creating an empty schema !")
        StructType(Seq.empty[StructField])
      }
    }
  }

  override def buildScan(): RDD[Row] = {
    GDBRDD(sqlContext.sparkContext, gdbPath, gdbName, serde, numPartition)
  }
}
