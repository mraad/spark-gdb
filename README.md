# Spark GDB Library

A library for parsing and querying an [Esri File Geodatabase](http://www.esri.com/news/arcuser/0309/files/9reasons.pdf) with Apache Spark.

This work in progress is a pure Scala read-only implementation based on [this](https://github.com/rouault/dump_gdbtable/wiki/FGDB-Spec) reverse engineered specification.
Understanding the internal file structure enables partitioning to perform massive parallel reading.
The reading API is based on the [Hadoop File System API](https://hadoop.apache.org/docs/r2.7.1/api/index.html?org/apache/hadoop/fs/FileSystem.html) enabling the placement of the GDB in HDFS or S3 (not tested) for multi node access.
There is still a lot to be done, but is a good start. Eventually, I will merge this project with my [Ibn Battuta Project](https://github.com/mraad/ibn-battuta).

### TODO (not in specified order)

* ~~Use [Esri Geometry Library](https://github.com/Esri/geometry-api-java) rather than JTS (I love JTS, so many utility functions on they geometry model)~~
* Implement ~~Point~~, Polyline and Polygon as Spatial Type using UDT spec.
* Handle more shapes - multiXXX and with Z and M
* Read default values in field definitions
* Register custom [Kryo](https://github.com/EsotericSoftware/kryo) serializer for shapes (optimization - but worth it :-)
* Perform a scan rather than a seek if the index row count is the same as the table count (should help performance)
* Test XML field type
* Test Blob field type
* Handle Raster (super low priority)

## Building From Source

This project build process is based on [Apache Maven](https://maven.apache.org/)

```bash
mvn install
```

The test data in `src/test/resources/Test.gdb` was generated using the ArcPy Tool `src/test/python/TestToolbox.pyt`

![](media/TestToolbox.png)

Thought the coordinates of the shapes are random, the coordinates values are placed as attributes for testing.
In the case of the `Points` feature class, the x/y coordinate values should match the values in the attributes `X` and `Y` enabling cross checking during testing.

## Using with Spark shell

```bash
$SPARK_HOME/bin/spark-shell --packages com.esri:spark-gdb:0.2
```

```scala
import com.esri.gdb._
import com.vividsolutions.jts.geom.Geometry
sc.gdbFile("src/test/resources/Test.gdb", "Points", numPartitions = 2).map(row => {
  row.getAs[Geometry](row.fieldIndex("Shape")).buffer(1)
}).foreach(println)
```

```scala
val df = sqlContext.read.
    format("com.esri.gdb").
    option("path", "src/test/resources/Test.gdb").
    option("name", "Points").
    load()
df.printSchema()
df.registerTempTable("points")
sqlContext.sql(s"select * from points").show()
```

### Using UDT and UDFs

For the Spatial UDT (User Defined Types), I am following the `VectorUDT` implementation.

In Scala:

```scala
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

sqlContext.sql(s"select getX(plus2(Shape)),getX(Shape) as y from $name")
  .show(20)
```

In Python:

```python
df = sqlContext.read \
    .format("com.esri.gdb") \
    .options(path="../../test/resources/Test.gdb", name=gdb_name, numPartitions="1") \
    .load()

df.printSchema()

df.registerTempTable(gdb_name)

sqlContext.registerFunction("getX", lambda p: p.x, DoubleType())
sqlContext.registerFunction("getY", lambda p: p.y, DoubleType())
sqlContext.registerFunction("plus2", lambda p: PointType(p.x + 2, p.y + 2), PointUDT())

rows = sqlContext.sql("select plus2(Shape),X,Y from {}".format(gdb_name))
for row in rows.collect():
    print row
```

## Testing In HDFS (Yet Another Excuse To Use Docker :-)

We will use [Docker](https://www.docker.com/) to bootstrap a [Cloudera quickstart](https://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/quickstart_docker_container.html) container instance.

I **highly** recommend the installation of the [Docker Toolbox](https://www.docker.com/docker-toolbox) for a Docker quick start.

Compile the project with the `quickstart` profile:
```bash
mvn -Pquickstart clean package
```

Create a local docker enabled machine using 6 cores, 8GB of RAM and with 10 GB of virtual disk:
```bash
docker-machine create\
 --driver virtualbox\
 --virtualbox-cpu-count 6\
 --virtualbox-memory 8192\
 --virtualbox-disk-size 10240\
 --virtualbox-no-vtx-check\
 quickstart
```

On Windows, I had to upgrade my machine instance:
```bash
docker-machine upgrade quickstart
```

Set up the docker environment:
```bash
eval $(docker-machine env quickstart)
```

Start a single node Hadoop instance with ZooKeeper, HDFS, YARN and Spark.
(This is a **4GB** download, so go grab some coffee and walk your dog, it is gonna take a while ! But, you only have to do that once. Ah... the beauty of docker images :-)
We expose the ports for Cloudera Manager (7180), Hue (8888) and NameNode (50070).
And to facilitate the moving of jars and test data from the host into the container, we map the `/Users` host folder onto the container `/Users` folder.
```bash
docker run\
 --privileged=true\
 --hostname=quickstart.cloudera\
 -v /Users:/Users\
 -p 7180:7180\
 -p 8888:8888\
 -p 50070:50070\
 -t -i cloudera/quickstart:latest\
 /usr/bin/docker-quickstart
```

Copy `Test.gdb` to HDFS:

```bash
hadoop fs -mkdir /data
hadoop fs -put /Users/<YOUR_PATH>/spark-gdb/src/test/resources/Test.gdb /data
```

Start A Spark shell:
```bash
spark-shell --jars /Users/<YOUR_PATH>/spark-gdb/target/spark-gdb-0.2.jar
```

Submit a Spark Context job:
```scala
import com.esri.gdb._
import com.esri.udt.PointType
sc.gdbFile("hdfs:///data/Test.gdb", "Points", numPartitions = 2).map(row => {
  row.getAs[PointType](row.fieldIndex("Shape"))
}).foreach(println)
```

Submit a SQL Context job:
```scala
val df = sqlContext.read.
    format("com.esri.gdb").
    option("path", "hdfs:///data/Test.gdb").
    option("name", "Lines").
    option("numPartitions", "2").
    load()
df.registerTempTable("lines")
sqlContext.sql("select * from lines").show()
```

#### Notes to self

set terminal type in windows to enable cursor movement:
```
set term=ansi
```

Start a CM instance:
```
/home/cloudera/cloudera-manager --express
```
