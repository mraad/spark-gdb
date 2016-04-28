package com.esri.app

import com.esri.gdb.{GDBIndex, GDBTable}
import resource._

object GDBApp extends App {
  val gdb = "/Users/mraad_admin/GWorkspace/spark-gdb/src/test/resources/Test.gdb"
  GDBTable.findTable(gdb, "ZMPoints")
    .foreach(catTab => {
      for {
        index <- managed(GDBIndex(gdb, catTab.hexName))
        table <- managed(GDBTable(gdb, catTab.hexName))
      } {
        table.rowIterator(index).foreach(println)
      }
    })
}
