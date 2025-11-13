import arcpy
from datetime import datetime


def updateDeleted(delete, destination):
    ## Break down into chunks if delete is greater than 50 records
    if len(delete) > 50:
        clauses = [delete[d:d + 50] for d in range(0,len(delete),50)] 
        for clause in clauses:
            whereClause =  """ "GUID" IN (""" +  ','.join("'{}'".format(c) for c in clause) + ")"
            with arcpy.da.UpdateCursor(destination, 'GUID', whereClause) as delCursor:
                for row in delCursor:
                    delCursor.deleteRow()
    else:
        whereClause =  """ "GUID" IN (""" +  ','.join("'{}'".format(i) for i in delete) + ")"
        with arcpy.da.UpdateCursor(destination, 'GUID', whereClause) as delCursor:
            for row in delCursor:
                delCursor.deleteRow()

def updateInserted(insert, source, destination):
    ## Break down into chunks if insert is greater than 50 records
    if len(insert)>50:
        clauses = [insert[i:i + 50] for i in range(0,len(insert),50)] 
        for clause in clauses:
            whereClause = """ "GUID" IN (""" +  ','.join("'{}'".format(c) for c in clause) + ")"
            srcSelect = arcpy.management.SelectLayerByAttribute(source,"NEW_SELECTION",whereClause)
            ##TODO: Consider field mappings as part of append process
            arcpy.Append_management(srcSelect, destination, "NO_TEST")          
    else:
        whereClause = """ "GUID" IN (""" +  ','.join("'{}'".format(i) for i in insert) + ")"
        srcSelect = arcpy.management.SelectLayerByAttribute(source,"NEW_SELECTION",whereClause)
        ##TODO: Consider field mappings as part of append process
        arcpy.Append_management(srcSelect, destination, "NO_TEST")    

    with arcpy.da.UpdateCursor(destination, 'UpdateDate', """ "UpdateDate" IS NULL """) as updateCursor:
        for row in updateCursor:
            row[0] = datetime.now()
            updateCursor.updateRow(row)
