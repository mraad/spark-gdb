from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import DoubleType

from com.esri.udt import PointType, PointUDT

if __name__ == "__main__":

    conf = SparkConf().setAppName("GDB App")
    sc = SparkContext(conf=conf)
    try:
        sqlContext = SQLContext(sc)

        sqlContext.registerFunction("getX", lambda p: p.x, DoubleType())
        sqlContext.registerFunction("getY", lambda p: p.y, DoubleType())
        sqlContext.registerFunction("plus2", lambda p: PointType(p.x + 2, p.y + 2), PointUDT())

        points = "Points"
        df = sqlContext.read \
            .format("com.esri.gdb") \
            .options(path="../../test/resources/Test.gdb", name=points, numPartitions="1") \
            .load()
        df.printSchema()
        df.registerTempTable(points)
        rows = sqlContext.sql("select plus2(Shape),X,Y from {}".format(points))
        for row in rows.collect():
            print row

        points = "MPoints"
        df = sqlContext.read \
            .format("com.esri.gdb") \
            .options(path="../../test/resources/Test.gdb", name=points, numPartitions="1") \
            .load()
        df.printSchema()
        df.registerTempTable(points)
        rows = sqlContext.sql("select * from {}".format(points))
        for row in rows.collect():
            print row

        points = "ZPoints"
        df = sqlContext.read \
            .format("com.esri.gdb") \
            .options(path="../../test/resources/Test.gdb", name=points, numPartitions="1") \
            .load()
        df.printSchema()
        df.registerTempTable(points)
        rows = sqlContext.sql("select * from {}".format(points))
        for row in rows.collect():
            print row

        points = "ZMPoints"
        df = sqlContext.read \
            .format("com.esri.gdb") \
            .options(path="../../test/resources/Test.gdb", name=points, numPartitions="1") \
            .load()
        df.printSchema()
        df.registerTempTable(points)
        rows = sqlContext.sql("select * from {}".format(points))
        for row in rows.collect():
            print row

        lines = "Lines"
        df = sqlContext.read \
            .format("com.esri.gdb") \
            .options(path="../../test/resources/Test.gdb", name=lines, numPartitions="2") \
            .load()
        df.printSchema()
        df.registerTempTable(lines)
        rows = sqlContext.sql("select * from {}".format(lines))
        for row in rows.collect():
            print row

        polygons = "Polygons"
        df = sqlContext.read \
            .format("com.esri.gdb") \
            .options(path="../../test/resources/Test.gdb", name=polygons, numPartitions="2") \
            .load()
        df.printSchema()
        df.registerTempTable(polygons)
        rows = sqlContext.sql("select * from {}".format(polygons))
        for row in rows.collect():
            print row

            # sqlContext \
            #    .sql("select * from {}".format(gdb_name)) \
            #    .write \
            #    .format("json") \
            #    .save("/tmp/{}.json".format(gdb_name))

    finally:
        sc.stop()
