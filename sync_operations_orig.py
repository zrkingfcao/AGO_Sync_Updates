import arcpy

def returnInserted(tblName, cursor, ago, whereClause=None):
    
    LastRunDate = "LastRunDate_AGO" if ago else "LastRunDate"
    ## Original
    # if whereClause is None:
    #     queryInserts = f"""SELECT GUID FROM {tblName} 
    #     WHERE GDB_BRANCH_ID = 0 AND
    #     created_date > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
    #     AND GDB_DELETED_BY IS NULL
    #     GROUP BY GUID
    #     """
    # else:
    #     queryInserts = f"""SELECT GUID FROM {tblName} 
    #     WHERE GDB_BRANCH_ID = 0 AND
    #     created_date > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
    #     AND GDB_DELETED_BY IS NULL AND {whereClause}
    #     GROUP BY GUID
    #     """

    ##New Testing 10/14/2025
    ##FIXME: Use match case (Python 3.10 and higher - https://docs.python.org/3/reference/compound_stmts.html#match) instead of if/else
    if tblName == 'TAX': 
        srcTbleName = 'TAXPARCEL_CONDOUNITSTACK_LGIM'
    else: 
        srcTbleName = tblName
    if whereClause is None:
        queryInserts = f"""SELECT GUID FROM {tblName} 
        WHERE GDB_BRANCH_ID = 0 AND
        GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
        AND GDB_DELETED_BY IS NULL
        AND GUID NOT IN (SELECT GUID FROM GIS_READ_ONLY.sde.{srcTbleName})
        GROUP BY GUID
        """
    else:
        queryInserts = f"""SELECT GUID FROM {tblName} 
        WHERE GDB_BRANCH_ID = 0 AND
        GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
        AND GDB_DELETED_BY IS NULL 
        AND GUID NOT IN (SELECT GUID FROM GIS_READ_ONLY.sde.{srcTbleName})
        AND {whereClause}
        GROUP BY GUID
        """

    returnInserts = cursor.execute(queryInserts)
    insertRecs = [row[0] for row in returnInserts]
    return insertRecs

def returnUpdated(tblName, cursor, ago, whereClause=None):
    
    LastRunDate = "LastRunDate_AGO" if ago else "LastRunDate"
    ## Original
    # if whereClause is None:
    #     queryUpdates = f"""SELECT GUID FROM {tblName}  
    #         WHERE GDB_BRANCH_ID = 0 AND
    #         GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
    #         AND created_date < (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
    #         AND GDB_DELETED_BY IS NULL
    #         GROUP BY GUID
    #         """    
    # else:
    #     queryUpdates = f"""SELECT GUID FROM {tblName}  
    #     WHERE GDB_BRANCH_ID = 0 AND
    #     GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
    #     AND created_date < (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
    #     AND GDB_DELETED_BY IS NULL AND {whereClause}
    #     GROUP BY GUID
    #     """ 

    ##New Testing 10/14/2025
    ##FIXME: Use match case (Python 3.10 and higher - https://docs.python.org/3/reference/compound_stmts.html#match) instead of if/else
    if tblName == 'TAX': 
        srcTbleName = 'TAXPARCEL_CONDOUNITSTACK_LGIM'
    else: 
        srcTbleName = tblName
    if whereClause is None:
        queryUpdates = f"""SELECT GUID FROM {tblName}  
            WHERE GDB_BRANCH_ID = 0 AND
            GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
            AND GDB_DELETED_BY IS NULL
            AND GUID IN (SELECT GUID FROM GIS_READ_ONLY.sde.{srcTbleName})
            GROUP BY GUID
            """    
    else:
        queryUpdates = f"""SELECT GUID FROM {tblName}  
        WHERE GDB_BRANCH_ID = 0 AND
        GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
        AND GDB_DELETED_BY IS NULL 
        AND GUID IN (SELECT GUID FROM GIS_READ_ONLY.sde.{srcTbleName})
        AND {whereClause}
        GROUP BY GUID
        """ 

    returnUpdates = cursor.execute(queryUpdates)
    updateRecs = [row[0] for row in returnUpdates] 
    return updateRecs

def returnDeleted(tblName, cursor, ago, whereClause=None):
    LastRunDate = "LastRunDate_AGO" if ago else "LastRunDate"
    ## Original 
    # if whereClause is None:
    #     queryDeletes = f"SELECT DISTINCT(GUID) FROM {tblName} WHERE GDB_BRANCH_ID = 0 AND GDB_DELETED_AT > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')"
    # elif "RetiredByRecord" in whereClause:
    #     queryDeletes = f"SELECT DISTINCT(GUID) FROM {tblName} WHERE GDB_BRANCH_ID = 0 AND GDB_DELETED_AT > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\') OR (RetiredByRecord IS NOT NULL AND last_edited_date > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\'))"
    # else:
    #     queryDeletes = f"SELECT DISTINCT(GUID) FROM {tblName} WHERE GDB_BRANCH_ID = 0 AND GDB_DELETED_AT > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\') AND {whereClause}"

    ##New Testing 10/14/2025
    if whereClause is None:
        queryDeletes = f"SELECT DISTINCT(GUID) FROM {tblName} WHERE GDB_BRANCH_ID = 0 AND GDB_IS_DELETE = 1 AND GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')"
    elif "RetiredByRecord" in whereClause:
        queryDeletes = f"SELECT DISTINCT(GUID) FROM {tblName} WHERE GDB_BRANCH_ID = 0 AND GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\') AND RetiredByRecord IS NOT NULL"
    else:
        queryDeletes = f"SELECT DISTINCT(GUID) FROM {tblName} WHERE GDB_BRANCH_ID = 0 AND GDB_IS_DELETE = 1 AND GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\') AND {whereClause}"


    returnDeletes = cursor.execute(queryDeletes)
    deleteRecs = [row[0] for row in returnDeletes]
    return deleteRecs

def updateDeleted(delete, destination):
    
    whereClause =  """ "GUID" IN (""" +  ','.join("'{}'".format(i) for i in delete) + ")"
    with arcpy.da.UpdateCursor(destination, 'GUID', whereClause) as cursor:
        for row in cursor:
            cursor.deleteRow()

def updateInserted(insert, source, destination):

    whereClause = """ "GUID" IN (""" +  ','.join("'{}'".format(i) for i in insert) + ")"
    srcSelect = arcpy.management.SelectLayerByAttribute(source,"NEW_SELECTION",whereClause)
    ##TODO: Consider field mappings as part of append process
    arcpy.Append_management(srcSelect, destination, "NO_TEST")    