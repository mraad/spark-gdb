package com.esri.app

import com.esri.gdb.{GDBIndex, GDBTable}
import resource._

object GDBApp extends App {
  val gdb = "/Users/mraad_admin/Share/World.gdb"
  GDBTable.findTable(gdb, "Cities")
    .foreach(catTab => {
      for {
        index <- managed(GDBIndex(gdb, catTab.hexName))
        table <- managed(GDBTable(gdb, catTab.hexName))
      } {
        table.rowIterator(index).foreach(println)
      }
    })
}
