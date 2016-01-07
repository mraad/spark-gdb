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
        df_points = sqlContext.read \
            .format("com.esri.gdb") \
            .options(path="../../test/resources/Test.gdb", name=points, numPartitions="1") \
            .load()
        df_points.printSchema()
        df_points.registerTempTable(points)
        rows = sqlContext.sql("select plus2(Shape),X,Y from {}".format(points))
        for row in rows.collect():
            print row

        lines = "Lines"
        df_lines = sqlContext.read \
            .format("com.esri.gdb") \
            .options(path="../../test/resources/Test.gdb", name=lines, numPartitions="2") \
            .load()
        df_lines.printSchema()
        df_lines.registerTempTable(lines)
        rows = sqlContext.sql("select * from {}".format(lines))
        for row in rows.collect():
            print row

            # sqlContext \
            #    .sql("select * from {}".format(gdb_name)) \
            #    .write \
            #    .format("json") \
            #    .save("/tmp/{}.json".format(gdb_name))

    finally:
        sc.stop()
