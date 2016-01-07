package com.esri.app

import com.esri.gdb._
import com.esri.udt.PointType
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  */
object Main extends App with Logging {

  val (path, name) = args.length match {
    case 2 => (args(0), args(1))
    case _ => throw new IllegalArgumentException("Missing path and name")
  }
  val conf = new SparkConf()
    .setAppName("Main")
    .setMaster("local[*]")
    .set("spark.app.id", "Main")
    .set("spark.ui.enabled", "false")
    .set("spark.ui.showConsoleProgress", "false")
    .registerKryoClasses(Array())

  val sc = new SparkContext(conf)
  try {
    sc.gdbFile("/Users/mraad_admin/Share/World.gdb", "Cities", 1)
      .map(row => {
        row.getAs[PointType](row.fieldIndex("Shape")).asGeometry
      })
      .map(point => {
        (point.getX, point.getY)
      })
      .foreach(println)

    /*
        val sqlContext = new SQLContext(sc)
        val df = sqlContext.read.format("com.esri.gdb")
          .option("path", path)
          .option("name", name)
          .option("numPartitions", "1")
          .load()
        df.printSchema()
        df.registerTempTable(name)
        sqlContext.udf.register("getX", (point: PointType) => point.x)
        sqlContext.udf.register("getY", (point: PointType) => point.y)
        sqlContext.udf.register("plus2", (point: PointType) => PointType(point.x + 2, point.y + 2))
        // df.select("OBJECTID", "X", "Y", "Shape")
        sqlContext.sql(s"select getX(plus2(Shape)),getX(Shape) as y from $name")
          .show(20)
    */
    /*
          .write
          .mode(SaveMode.Overwrite)
          .format("json")
          .save(s"/tmp/$name.json")
    */
  } finally {
    sc.stop()
  }

}
