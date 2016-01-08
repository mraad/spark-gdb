package com.esri.app

import com.esri.core.geometry.Polyline
import com.esri.udt.{PointType, PolylineType}
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
    /*
        sc.gdbFile("/Users/mraad_admin/Share/World.gdb", "Cities", 1)
          .map(row => {
            row.getAs[PointType](row.fieldIndex("Shape")).asGeometry
          })
          .map(point => {
            (point.getX, point.getY)
          })
          .foreach(println)
    */

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
    sqlContext.udf.register("line", (point: PointType) => PolylineType({
      val line = new Polyline()
      line.startPath(point.x - 2, point.y - 2)
      line.lineTo(point.x + 2, point.y + 2)
      line
    }
    ))
    sqlContext.sql(s"select line(Shape),getX(Shape)-2 as x from $name")
      .foreach(println)
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
