package com.esri.app

import com.esri.gdb.{GDBIndex, GDBTable}
import resource._

object GDBApp extends App {
  val gdb = "/Users/mraad_admin/Share/VZ/OnlyHighways.gdb"

  GDBTable.listTables(gdb).foreach(println)

  doCat

  def doCat: Unit = {
    GDBTable.findTable(gdb, "Interstates")
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
