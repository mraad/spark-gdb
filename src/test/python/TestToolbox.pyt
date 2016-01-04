import datetime
import os
import random

import arcpy


class Toolbox(object):
    def __init__(self):
        self.label = "Test Toolbox"
        self.alias = "Toolbox to generate random point, line and polygon feature classes to test Spark FileGDB"
        self.tools = [PointTool, LineTool, PolygonTool, TypesTool]


class BaseTool(object):
    def __init__(self):
        self.canRunInBackground = False

    def getParameterInfo(self):
        paramPath = arcpy.Parameter(
                name="gdb_path",
                displayName="gdb_path",
                direction="Input",
                datatype="String")
        paramPath.value = "C:\\Temp\\Test.gdb"
        return [paramPath]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def createFeatureClass(self, parameters, name, fc_type):
        path = parameters[0].value
        if not os.path.exists(path):
            head, tail = os.path.split(path)
            arcpy.management.CreateFileGDB(head, tail)

        fc = "{}/{}".format(path, name)
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)
        arcpy.management.CreateFeatureclass(path, name, fc_type, spatial_reference=arcpy.SpatialReference(4326))
        return fc


class TypesTool(BaseTool):
    def __init__(self):
        super(TypesTool, self).__init__()
        self.label = "Generate Types"
        self.description = "Generate Types"

    def execute(self, parameters, messages):
        fc = self.createFeatureClass(parameters, "Types", "POINT")
        arcpy.management.AddField(fc, "A_TEXT", "TEXT", field_alias="A Text", field_length=32)
        arcpy.management.AddField(fc, "A_FLOAT", "FLOAT", field_alias="A Float")
        arcpy.management.AddField(fc, "A_DOUBLE", "DOUBLE", field_alias="A Double")
        arcpy.management.AddField(fc, "A_SHORT", "SHORT", field_alias="A Short")
        arcpy.management.AddField(fc, "A_LONG", "LONG", field_alias="A Long")
        arcpy.management.AddField(fc, "A_DATE", "DATE", field_alias="A Date")
        arcpy.management.AddField(fc, "A_GUID", "GUID", field_alias="A GUID")

        with arcpy.da.InsertCursor(fc, ["SHAPE@XY",
                                        "A_TEXT",
                                        "A_FLOAT", "A_DOUBLE",
                                        "A_SHORT", "A_LONG",
                                        "A_DATE",
                                        "A_GUID"]) as cursor:
            a_date = datetime.datetime(2016, 01, 01, 07, 24, 32)
            cursor.insertRow([(33.8869, 35.5131),
                              "Beirut",
                              33.8869, 35.5131,
                              33, 35,
                              a_date,
                              "{2AA7D58D-2BF4-4943-83A8-457B70DB1871}"])


class PointTool(BaseTool):
    def __init__(self):
        super(PointTool, self).__init__()
        self.label = "Generate Random Points"
        self.description = "Generate Random Points"

    def execute(self, parameters, messages):
        fc = self.createFeatureClass(parameters, "Points", "POINT")
        arcpy.management.AddField(fc, "X", "DOUBLE")
        arcpy.management.AddField(fc, "Y", "DOUBLE")
        arcpy.management.AddField(fc, "RID", "INTEGER")

        with arcpy.da.InsertCursor(fc, ["SHAPE@XY", "X", "Y", "RID"]) as cursor:
            for rid in range(1, 21):
                x = random.uniform(-180, 180)
                y = random.uniform(-90, 90)
                cursor.insertRow([(x, y), x, y, rid])
        return


class LineTool(BaseTool):
    def __init__(self):
        super(LineTool, self).__init__()
        self.label = "Generate Random Lines"
        self.description = "Generate Random Lines"

    def execute(self, parameters, messages):
        fc = self.createFeatureClass(parameters, "Lines", "POLYLINE")
        arcpy.management.AddField(fc, "X1", "DOUBLE")
        arcpy.management.AddField(fc, "Y1", "DOUBLE")
        arcpy.management.AddField(fc, "X2", "DOUBLE")
        arcpy.management.AddField(fc, "Y2", "DOUBLE")
        arcpy.management.AddField(fc, "X3", "DOUBLE")
        arcpy.management.AddField(fc, "Y3", "DOUBLE")
        arcpy.management.AddField(fc, "RID", "INTEGER")

        with arcpy.da.InsertCursor(fc, ["SHAPE@", "X1", "Y1", "X2", "Y2", "X3", "Y3", "RID"]) as cursor:
            for rid in range(1, 21):
                x1 = random.uniform(-180, 0)
                y1 = random.uniform(-90, 0)
                x2 = x1 + random.uniform(0, 20)
                y2 = y1 + random.uniform(0, 20)
                x3 = x2 + random.uniform(0, 50)
                y3 = y2 + random.uniform(0, 50)
                shape = [[x1, y1], [x2, y2], [x3, y3]]
                cursor.insertRow([shape, x1, y1, x2, y2, x3, y3, rid])
        return


class PolygonTool(BaseTool):
    def __init__(self):
        super(PolygonTool, self).__init__()
        self.label = "Generate Random Polygons"
        self.description = "Generate Random Polygons"

    def execute(self, parameters, messages):
        fc = self.createFeatureClass(parameters, "Polygons", "POLYGON")
        arcpy.management.AddField(fc, "X1", "DOUBLE")
        arcpy.management.AddField(fc, "Y1", "DOUBLE")
        arcpy.management.AddField(fc, "X2", "DOUBLE")
        arcpy.management.AddField(fc, "Y2", "DOUBLE")
        arcpy.management.AddField(fc, "RID", "INTEGER")

        with arcpy.da.InsertCursor(fc, ["SHAPE@", "X1", "Y1", "X2", "Y2", "RID"]) as cursor:
            for rid in range(1, 21):
                x1 = random.uniform(-180, 180 - 40)
                y1 = random.uniform(-90, 90 - 40)
                x2 = x1 + random.uniform(1, 40)
                y2 = y1 + random.uniform(1, 40)
                xm = (x1 + x2) / 2.0
                ym = (y1 + y2) / 2.0
                shape = [[x1, y1], [x1, y2], [xm, y2], [xm, ym], [x2, ym], [x2, y1], [x1, y1]]
                cursor.insertRow([shape, x1, y1, x2, y2, rid])
        return
