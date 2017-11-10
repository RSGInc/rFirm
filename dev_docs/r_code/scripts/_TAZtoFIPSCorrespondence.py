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
counties = r"C:\Projects\OR\15086 - Metro Freight Model\Freight\portland_data\GIS\US Counties\tl_2010_us_county10.shp"

# Variables: Intermediate
delete_files = []
taz_points = os.path.join(env.workspace, "Zone_Points")
county_proj = os.path.join(env.workspace, "Counties_Proj")
spat_join = os.path.join(env.workspace, "ZoneCounty_SJ")

# Variables: Output
new_csv = r"C:\Projects\_Federal\FHWA\14205 - National Freight Model\Data\Output Data\corresp_taz_fips.csv"

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

def CreatePFMZoneFipsCorrespondence():
    header = ["TAZ", "StateFIPS", "CountyFIPS", "FIPS",]
    arcpy.FeatureToPoint_management(taz_shp, taz_points, "INSIDE")
    DeleteExtraFields(taz_points, [taz_id])
    spatial_ref = arcpy.Describe(taz_points).spatialReference
    arcpy.Project_management(counties, county_proj, spatial_ref)
    arcpy.SpatialJoin_analysis(taz_points, county_proj, spat_join, "JOIN_ONE_TO_ONE", "", "", "HAVE_THEIR_CENTER_IN", "", "")
    cursor = arcpy.da.SearchCursor(spat_join, [taz_id, "STATEFP10", "COUNTYFP10", "GEOID10"])
    with open(new_csv, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',')
        csv_writer.writerow(header)
        for row in cursor:
            csv_string = row[0], row[1], row[2], row[3],
            csv_writer.writerow(csv_string)
    csvfile.close()
    delete_files.append(taz_points)
    delete_files.append(county_proj)
    delete_files.append(spat_join)

def Main():
    CreatePFMZoneFipsCorrespondence()
    for file in delete_files:
        arcpy.Delete_management(file)
    print "*** Success ***"

Main()




 