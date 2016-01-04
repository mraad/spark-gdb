__all__ = ['ShapeWKT']

import array
import sys
from pyspark.sql.types import UserDefinedType, StringType
from shapely import geos

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


class ShapeWKT(UserDefinedType):
    def __init__(self):
        self.writer = geos.WKTWriter(geos.lgeos)
        self.reader = geos.WKTReader(geos.lgeos)

    @classmethod
    def sqlType(self):
        return StringType()

    @classmethod
    def module(cls):
        return "com.esri.udt"

    @classmethod
    def scalaUDT(cls):
        return "com.esri.udt.ShapeWKT"

    def serialize(self, obj):
        return self.writer.write(obj)

    def deserialize(self, datum):
        return self.reader.read(datum)

    def simpleString(self):
        return "shapewkt"
