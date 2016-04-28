__all__ = ['PointType', 'PointZType', 'PointMType', 'PointZMType', 'PolylineType', 'PolygonType']

import array
import sys
from pyspark.sql.types import UserDefinedType, StructField, StructType, DoubleType, IntegerType, ArrayType

#
# Copied from Spark VectorUDT
#
if sys.version >= '3':
    basestring = str
    xrange = range
    import copyreg as copy_reg

    long = int
else:
    import copy_reg

if sys.version_info[:2] == (2, 7):
    # speed up pickling array in Python 2.7
    def fast_pickle_array(ar):
        return array.array, (ar.typecode, ar.tostring())


    copy_reg.pickle(array.array, fast_pickle_array)


class PointZUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for PointZ.
    """

    @classmethod
    def sqlType(self):
        return StructType([
            StructField("x", DoubleType(), False),
            StructField("y", DoubleType(), False),
            StructField("z", DoubleType(), False)
        ])

    @classmethod
    def module(cls):
        return "com.esri.udt"

    @classmethod
    def scalaUDT(cls):
        return "com.esri.udt.PointZUDT"

    def serialize(self, obj):
        return obj.x, obj.y, obj.z

    def deserialize(self, datum):
        return PointZType(datum[0], datum[1], datum[2])

    def simpleString(self):
        return "pointZ"


class PointZType(object):
    __UDT__ = PointZUDT()

    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    def __repr__(self):
        return "PointZType({},{},{})".format(self.x, self.y, self.z)

    def __str__(self):
        return "({},{},{})".format(self.x, self.y, self.z)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               other.x == self.x and \
               other.y == self.y and \
               other.z == self.z


class PointMUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for PointM.
    """

    @classmethod
    def sqlType(self):
        return StructType([
            StructField("x", DoubleType(), False),
            StructField("y", DoubleType(), False),
            StructField("m", DoubleType(), False)
        ])

    @classmethod
    def module(cls):
        return "com.esri.udt"

    @classmethod
    def scalaUDT(cls):
        return "com.esri.udt.PointMUDT"

    def serialize(self, obj):
        return obj.x, obj.y, obj.m

    def deserialize(self, datum):
        return PointMType(datum[0], datum[1], datum[2])

    def simpleString(self):
        return "pointM"


class PointMType(object):
    __UDT__ = PointMUDT()

    def __init__(self, x, y, m):
        self.x = x
        self.y = y
        self.m = m

    def __repr__(self):
        return "PointMType({},{},{})".format(self.x, self.y, self.m)

    def __str__(self):
        return "({},{},{})".format(self.x, self.y, self.m)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               other.x == self.x and \
               other.y == self.y and \
               other.m == self.m


class PointZMUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for PointZM.
    """

    @classmethod
    def sqlType(self):
        return StructType([
            StructField("x", DoubleType(), False),
            StructField("y", DoubleType(), False),
            StructField("z", DoubleType(), False),
            StructField("m", DoubleType(), False)
        ])

    @classmethod
    def module(cls):
        return "com.esri.udt"

    @classmethod
    def scalaUDT(cls):
        return "com.esri.udt.PointZMUDT"

    def serialize(self, obj):
        return obj.x, obj.y, obj.z, obj.m

    def deserialize(self, datum):
        return PointZMType(datum[0], datum[1], datum[2], datum[3])

    def simpleString(self):
        return "pointZM"


class PointZMType(object):
    __UDT__ = PointZMUDT()

    def __init__(self, x, y, z, m):
        self.x = x
        self.y = y
        self.z = z
        self.m = m

    def __repr__(self):
        return "PointZMType({},{},{},{})".format(self.x, self.y, self.z, self.m)

    def __str__(self):
        return "({},{},{},{})".format(self.x, self.y, self.z, self.m)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               other.x == self.x and \
               other.y == self.y and \
               other.z == self.z and \
               other.m == self.m


class PointUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for Point.
    """

    @classmethod
    def sqlType(self):
        return StructType([
            StructField("x", DoubleType(), False),
            StructField("y", DoubleType(), False)
        ])

    @classmethod
    def module(cls):
        return "com.esri.udt"

    @classmethod
    def scalaUDT(cls):
        return "com.esri.udt.PointUDT"

    def serialize(self, obj):
        return obj.x, obj.y

    def deserialize(self, datum):
        return PointType(datum[0], datum[1])

    def simpleString(self):
        return "point"


class PointType(object):
    __UDT__ = PointUDT()

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __repr__(self):
        return "PointType({},{})".format(self.x, self.y)

    def __str__(self):
        return "({},{})".format(self.x, self.y)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               other.x == self.x and other.y == self.y


class PolylineUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for Polyline.
    """

    @classmethod
    def sqlType(cls):
        return StructType([
            StructField("xmin", DoubleType(), False),
            StructField("ymin", DoubleType(), False),
            StructField("xmax", DoubleType(), False),
            StructField("ymax", DoubleType(), False),
            StructField("xyNum", ArrayType(IntegerType(), False), False),
            StructField("xyArr", ArrayType(DoubleType(), False), False)])

    @classmethod
    def module(cls):
        return "com.esri.udt"

    @classmethod
    def scalaUDT(cls):
        return "com.esri.udt.PolylineUDT"

    def serialize(self, obj):
        xyNum = [int(i) for i in obj.xyNum]
        xyArr = [float(v) for v in obj.xyArr]
        return obj.xmin, obj.ymin, obj.xmax, obj.ymax, xyNum, xyArr

    def deserialize(self, datum):
        return PolylineType(datum[0], datum[1], datum[2], datum[3], datum[4], datum[5])

    def simpleString(self):
        return "polyline"


class PolylineType(object):
    __UDT__ = PolylineUDT()

    def __init__(self, xmin, ymin, xmax, ymax, xyNum, xyArr):
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax
        self.xyNum = xyNum
        self.xyArr = xyArr

    def __repr__(self):
        return "PolylineType({},{},{},{})".format(self.xmin, self.ymin, self.xmax, self.ymax)

    def __str__(self):
        return "({},{},{},{})".format(self.xmin, self.ymin, self.xmax, self.ymax)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               other.xmin == self.xmin and other.ymin == self.ymin and \
               other.xmax == self.xmax and other.ymax == self.ymax


class PolygonUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for Polygon.
    """

    @classmethod
    def sqlType(cls):
        return StructType([
            StructField("xmin", DoubleType(), False),
            StructField("ymin", DoubleType(), False),
            StructField("xmax", DoubleType(), False),
            StructField("ymax", DoubleType(), False),
            StructField("xyNum", ArrayType(IntegerType(), False), False),
            StructField("xyArr", ArrayType(DoubleType(), False), False)])

    @classmethod
    def module(cls):
        return "com.esri.udt"

    @classmethod
    def scalaUDT(cls):
        return "com.esri.udt.PolygonUDT"

    def serialize(self, obj):
        xyNum = [int(i) for i in obj.xyNum]
        xyArr = [float(v) for v in obj.xyArr]
        return obj.xmin, obj.ymin, obj.xmax, obj.ymax, xyNum, xyArr

    def deserialize(self, datum):
        return PolygonType(datum[0], datum[1], datum[2], datum[3], datum[4], datum[5])

    def simpleString(self):
        return "polygon"


class PolygonType(object):
    __UDT__ = PolygonUDT()

    def __init__(self, xmin, ymin, xmax, ymax, xyNum, xyArr):
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax
        self.xyNum = xyNum
        self.xyArr = xyArr

    def __repr__(self):
        return "PolygonType({},{},{},{})".format(self.xmin, self.ymin, self.xmax, self.ymax)

    def __str__(self):
        return "({},{},{},{})".format(self.xmin, self.ymin, self.xmax, self.ymax)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               other.xmin == self.xmin and other.ymin == self.ymin and \
               other.xmax == self.xmax and other.ymax == self.ymax
