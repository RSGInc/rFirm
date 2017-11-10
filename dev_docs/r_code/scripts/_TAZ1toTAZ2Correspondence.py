# Author: Justin Culp
# Date: 
#
# Description: 
# Requires: ArcGIS 10.4 Desktop Basic
#

# Import arcpy module
import arcpy, os, csv

# Set workspace environment
from arcpy import env
env.workspace = r"C:\Projects\_Federal\FHWA\14205 - National Freight Model\Data\Working_Freight_Net.gdb"
env.overwriteOutput = True

# Variables: Input
taz_shp = r"C:\Projects\_Federal\FHWA\14205 - National Freight Model\Data\Source\NUMA_Zone_System\NUMA_Polygon.shp"
taz_query = "" #Test query "FID_1 < 1000"
taz_id = "FID_1"

# Variables: Intermediate
delete_files = []
taz_copy = os.path.join(env.workspace, "TAZ_Copy")
taz_points = os.path.join(env.workspace, "TAZ_Points")
taz_sub = os.path.join(env.workspace, "TAZ_Subset")
spat_join = os.path.join(env.workspace, "Points_Subset_SJ")

# Variables: Output
new_csv = r"C:\Projects\_Federal\FHWA\14205 - National Freight Model\Data\Output Data\corresp_taz1_taz2.csv"

codeblock = """def ifNull(field):
  if field == None:
    return -1
  else:
    return field"""

# Geoprocessing steps
def DeleteExtraFields(in_table, keepfields):
    fieldnames = []
    fields = arcpy.ListFields(in_table)
    for field in fields:
        if not field.required:
            fieldnames.append(field.name)
    for keepfield in keepfields:
        fieldnames.remove(keepfield)
    if len(fieldnames)>0:
        arcpy.DeleteField_management(in_table, fieldnames)

def PreProcessInputFiles():
    arcpy.Select_analysis(taz_shp, taz_copy)
    arcpy.AddField_management(taz_copy, "TAZ1", "LONG")
    arcpy.CalculateField_management(taz_copy, "TAZ1", "!"+taz_id+"!", "PYTHON_9.3", "")
    if taz_query != "":
        arcpy.Select_analysis(taz_copy, taz_sub, taz_query)
    else:
        arcpy.Select_analysis(taz_copy, taz_sub)
    DeleteExtraFields(taz_copy, ["TAZ1"])
    DeleteExtraFields(taz_sub, ["TAZ1"])
    delete_files.append(taz_copy)
    delete_files.append(taz_sub)

def CreatePFMZoneFipsCorrespondence():
    header = ["TAZ1", "TAZ2"]
    arcpy.FeatureToPoint_management(taz_copy, taz_points, "INSIDE")
    arcpy.SpatialJoin_analysis(taz_points, taz_sub, spat_join, "JOIN_ONE_TO_ONE", "", "", "HAVE_THEIR_CENTER_IN", "", "")
    arcpy.CalculateField_management(spat_join, "TAZ1_1", "ifNull(!TAZ1_1!)", "PYTHON_9.3", codeblock)
    cursor = arcpy.da.SearchCursor(spat_join, ["TAZ1", "TAZ1_1"])
    with open(new_csv, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',')
        csv_writer.writerow(header)
        for row in cursor:
            csv_string = row[0], row[1],
            csv_writer.writerow(csv_string)
    csvfile.close()
    delete_files.append(taz_points)
    delete_files.append(spat_join)

def Main():
    PreProcessInputFiles()
    CreatePFMZoneFipsCorrespondence()
    for file in delete_files:
        arcpy.Delete_management(file)
    print "*** Success ***"

Main()




 