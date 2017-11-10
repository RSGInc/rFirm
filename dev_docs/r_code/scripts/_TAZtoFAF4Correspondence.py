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
taz_id = "FID_1"
faf_zones = r"C:\Projects\_Federal\FHWA\14205 - National Freight Model\Data\Source\FAF4 Regions\FAF4_REGIONS.shp"
faf_intzones = r"C:\Projects\_Federal\FHWA\14205 - National Freight Model\Data\Source\FAF International Zones\FAF_International_Zones.shp"

# Variables: Intermediate
delete_files = []
faf_merge = os.path.join(env.workspace, "FAF_Merge")
taz_points = os.path.join(env.workspace, "Zone_Points")
faf_proj = os.path.join(env.workspace, "FAF4_Proj")
fafint_proj = os.path.join(env.workspace, "FAF4Int_Proj")
spat_join = os.path.join(env.workspace, "TAZ_FAF4_SJ")

# Variables: Output
new_csv = r"C:\Projects\_Federal\FHWA\14205 - National Freight Model\Data\Output Data\corresp_taz_faf4.csv"

## Subprocesses
codeblock = """def fillFAF(field, value):
  if field == None:
    return value
  else:
    return field"""

# Geoprocessing steps
def PreProcessInputFiles():
    arcpy.FeatureToPoint_management(taz_shp, taz_points, "INSIDE")
    spatial_ref = arcpy.Describe(taz_points).spatialReference
    arcpy.Project_management(faf_zones, faf_proj, spatial_ref)
    arcpy.Project_management(faf_intzones, fafint_proj, spatial_ref)
    arcpy.Merge_management([faf_proj, fafint_proj], faf_merge)
    arcpy.AddField_management(faf_merge, "FAF4", "LONG")
    arcpy.CalculateField_management(faf_merge, "FAF4", "!FAF4_Regio!", "PYTHON_9.3", "")
    arcpy.CalculateField_management(faf_merge, "FAF4", "fillFAF(!FAF4!, !FAFCODE!)", "PYTHON_9.3", codeblock)
    delete_files.append(taz_points)
    delete_files.append(faf_proj)
    delete_files.append(fafint_proj)
    delete_files.append(faf_merge)
   
def CreateTAZToFAF4Correspondence():
    header = ["TAZ", "FAF4",]
    arcpy.SpatialJoin_analysis(taz_points, faf_merge, spat_join, "JOIN_ONE_TO_ONE", "", "", "HAVE_THEIR_CENTER_IN", "", "")
    cursor = arcpy.da.SearchCursor(spat_join, [zone_id, "FAF4"])
    with open(new_csv, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',')
        csv_writer.writerow(header)
        for row in cursor:
            csv_string = row[0], row[1],
            csv_writer.writerow(csv_string)
    csvfile.close()
    delete_files.append(spat_join)


def Main():
    PreProcessInputFiles()
    CreateTAZToFAF4Correspondence()
    for file in delete_files:
        arcpy.Delete_management(file)
    print "*** Success ***"


Main()


 