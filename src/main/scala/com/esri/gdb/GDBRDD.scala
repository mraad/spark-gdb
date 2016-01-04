package com.esri.gdb

import org.apache.hadoop.conf.Configuration
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

/**
  */
case class GDBRDD(@transient sc: SparkContext, gdbPath: String, gdbName: String, numPartitions: Int) extends RDD[Row](sc, Nil) with Logging {

  @DeveloperApi
  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
    val part = partition.asInstanceOf[GDBPartition]
    val hadoopConf = if (sc == null) new Configuration() else sc.hadoopConfiguration
    val index = GDBIndex(gdbPath, part.hexName, hadoopConf)
    val table = GDBTable(gdbPath, part.hexName, hadoopConf)
    context.addTaskCompletionListener(context => {
      table.close()
      index.close()
    })
    table.rowIterator(index, part.startAtRow, part.numRowsToRead)
  }

  override protected def getPartitions: Array[Partition] = {
    val hadoopConf = if (sc == null) new Configuration() else sc.hadoopConfiguration
    GDBTable.findTable(gdbPath, gdbName, hadoopConf) match {
      case Some(catTab) => {
        val index = GDBIndex(gdbPath, catTab.hexName, hadoopConf)
        try {
          val numRows = index.numRows
          val numRowsPerPartition = (numRows.toDouble / numPartitions).ceil.toInt
          var startAtRow = 0
          (0 until numPartitions).map(i => {
            val endAtRow = startAtRow + numRowsPerPartition
            val numRowsToRead = if (endAtRow <= numRows) numRowsPerPartition else numRows - startAtRow
            val gdbPartition = GDBPartition(i, catTab.hexName, startAtRow, numRowsToRead)
            startAtRow += numRowsToRead
            gdbPartition
          }).toArray
        } finally {
          index.close()
        }
      }
      case _ => {
        log.error(s"Cannot find '$gdbName' in $gdbPath, creating an empty array of Partitions !")
        Array.empty[Partition]
      }
    }
  }
}

private[this] case class GDBPartition(m_index: Int,
                                      val hexName: String,
                                      val startAtRow: Int,
                                      val numRowsToRead: Int
                                     ) extends Partition {
  override def index = m_index
}
