from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import DoubleType

if __name__ == "__main__":

    conf = SparkConf().setAppName("GDB App")
    sc = SparkContext(conf=conf)
    try:
        gdb_name = "Points"
        sqlContext = SQLContext(sc)

        df = sqlContext.read \
            .format("com.esri.gdb") \
            .options(path="../../test/resources/Test.gdb", name=gdb_name, numPartitions="1") \
            .load()

        df.printSchema()

        df.registerTempTable(gdb_name)

        sqlContext.registerFunction("getX", lambda p: p.x, DoubleType())
        sqlContext.registerFunction("getY", lambda p: p.y, DoubleType())

        rows = sqlContext.sql("select getX(Shape) as PX,getY(Shape) as PY,X,Y from {}".format(gdb_name))
        for row in rows.collect():
            print row

            # sqlContext \
            #    .sql("select * from {}".format(gdb_name)) \
            #    .write \
            #    .format("json") \
            #    .save("/tmp/{}.json".format(gdb_name))

    finally:
        sc.stop()
