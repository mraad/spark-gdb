package com.esri.app

import com.esri.gdb.{GDBIndex, GDBTable}
import resource._

object GDBApp extends App {
  val gdb = "/Volumes/SSD512G/TXData/2014/TXDOT_Roadway_Inventory.gdb"

  /// GDBTable.listTables(gdb).foreach(println)

  doCat

  def doCat: Unit = {
    GDBTable.findTable(gdb, "TXDOT_Roadway_Linework_Routed")
      .foreach(catTab => {
        for {
          index <- managed(GDBIndex(gdb, catTab.hexName))
          table <- managed(GDBTable(gdb, catTab.hexName))
        } {
          val count = table
            .seekIterator(index.iterator())
            .count(m => true)
          println(count)
        }
      })
  }
}
