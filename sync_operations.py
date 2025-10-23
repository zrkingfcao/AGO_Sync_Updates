import arcpy


def updateDeleted(delete, destination):
    
    whereClause =  """ "GUID" IN (""" +  ','.join("'{}'".format(i) for i in delete) + ")"
    with arcpy.da.UpdateCursor(destination, 'GUID', whereClause) as delCursor:
        for row in delCursor:
            delCursor.deleteRow()

def updateInserted(insert, source, destination):

    whereClause = """ "GUID" IN (""" +  ','.join("'{}'".format(i) for i in insert) + ")"
    srcSelect = arcpy.management.SelectLayerByAttribute(source,"NEW_SELECTION",whereClause)
    ##TODO: Consider field mappings as part of append process
    arcpy.Append_management(srcSelect, destination, "NO_TEST")    