__all__ = ['PointType']

import array
import sys
from pyspark.sql.types import UserDefinedType, StructField, StructType, DoubleType

#
# copied from Spark VectorUDT
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


class PointUDT(UserDefinedType):
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
        return (obj.x, obj.y)

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
        return "PointType(%s,%s)" % (self.x, self.y)

    def __str__(self):
        return "(%s,%s)" % (self.x, self.y)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               other.x == self.x and other.y == self.y
