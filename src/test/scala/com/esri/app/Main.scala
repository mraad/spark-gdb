package com.esri.app

import com.esri.core.geometry.Point
import org.apache.spark.sql.SQLContext
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
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.esri.gdb")
      .option("path", path)
      .option("name", name)
      .option("numPartitions", "1")
      .load()
    df.printSchema()
    df.registerTempTable(name)
    sqlContext.udf.register("getX", (point: Point) => point.getX)
    sqlContext.udf.register("getY", (point: Point) => point.getY)
    // sqlContext.udf.register("buffer", (geom: Geometry, distance: Double) => GeometryUDT(geom.buffer(distance)))
    sqlContext
      .sql(s"select Shape from $name")
      .show()
  } finally {
    sc.stop()
  }

}
