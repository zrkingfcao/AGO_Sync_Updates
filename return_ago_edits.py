import sys


def returnEditedTables(cursor, ago):
    try:
        LastRunDate = "LastRunDate_AGO" if ago else "LastRunDate"
        queryGeomTables = f"""
            SELECT TR.table_name FROM sde.SDE_branch_tables_modified TM
            JOIN sde.SDE_table_registry TR
            ON TM.registration_id = TR.registration_id
            JOIN sde.LastEditUpdate LE ON TR.table_name = LE.TableName
            WHERE TM.branch_id = 0
            GROUP BY TR.table_name, LE.TableName, LE.{LastRunDate}
            HAVING LE.{LastRunDate} < MAX(TM.edit_moment)
            """  
        resultsGeomTables = cursor.execute(queryGeomTables)
        editTables = {}
        for i in resultsGeomTables:
            editTables[i[0]] = []

        rightTables = returnUpdatedJoin(cursor,True)
        if len(rightTables) > 0:
            for k,v in rightTables.items():
                if k in editTables:
                    editTables[k] += v
                else:
                    editTables[k] = [v]
        #print(editTables)
        return editTables   
    except Exception as e:
        import traceback
        tb = sys.exc_info()[2]
        print(f"ReturnAgoEdits.py returnEditedTables function exception on line {tb.tb_lineno} {e}")

def returnEdits(tblName, cursor, ago, whereClause=None):
    LastRunDate = "LastRunDate_AGO" if ago else "LastRunDate"
    if whereClause is None:
        queryEdits = f"""SELECT GUID FROM {tblName} 
        WHERE GDB_BRANCH_ID = 0 AND
        GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
        AND GDB_DELETED_BY IS NULL
        GROUP BY GUID
        """
    else:
        queryEdits = f"""SELECT GUID FROM {tblName} 
        WHERE GDB_BRANCH_ID = 0 AND
        GDB_FROM_DATE > (SELECT {LastRunDate} FROM sde.LastEditUpdate WHERE TableName = \'{tblName}\')
        AND GDB_DELETED_BY IS NULL 
        AND {whereClause}
        GROUP BY GUID
        """

    returnEdits = cursor.execute(queryEdits)
    editRecs = [row[0] for row in returnEdits]
    return editRecs    

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

def returnUpdatedJoin(cursor, ago):
    try:
        LastRunDate = "LastRunDate_AGO" if ago else "LastRunDate"
        createTemp = f"""
        DECLARE @LEN INT
        DECLARE @CURRENTTABLE NVARCHAR(100)
        DECLARE @RIGHTTABLE NVARCHAR(100)
        DECLARE @LEFTJOINKEY NVARCHAR(75)
        DECLARE @RIGHTJOINKEY NVARCHAR(75)
        DECLARE @LASTUPDATE DATETIME
        DECLARE @COUNT INT
        SET @COUNT = 0

        DECLARE @EDITEDTABLES TABLE(
            TABLENAME NVARCHAR(50),
            JOINTABLE NVARCHAR(50),
            LEFTJOINKEY NVARCHAR(75),
            RIGHTJOINKEY NVARCHAR(75),
            LASTRUNDATE DATETIME,
            ARRAYINDEX INT IDENTITY(1,1)
            )

        INSERT INTO @EDITEDTABLES(TABLENAME, JOINTABLE, LEFTJOINKEY, RIGHTJOINKEY,LASTRUNDATE) 
            SELECT TableName, JoinTable, LeftJoinKey, RightJoinKey, LastRunDate_AGO FROM LastEditUpdate
            WHERE JoinTable IS NOT NULL

        SELECT @LEN = COUNT(*) FROM  @EDITEDTABLES

        DROP TABLE IF EXISTS ##TBLEDITS
        CREATE TABLE ##TBLEDITS(
            TABLENAME nvarchar(50),
            GUID nvarchar(50)
        )

        WHILE @COUNT < @LEN
        BEGIN
            DECLARE @updateString NVARCHAR(2000)
            SET @COUNT = @COUNT + 1
            IF (SELECT TABLENAME FROM @EDITEDTABLES WHERE ARRAYINDEX = @COUNT) = 'TAX'
            BEGIN
                SET @CURRENTTABLE = 'GIS_READ_ONLY.sde.TAXPARCEL_CONDOUNITSTACK_LGIM'
                SET @LEFTJOINKEY = 'PARCELID'
            END
            ELSE
            BEGIN
                SET @CURRENTTABLE = (SELECT TABLENAME FROM @EDITEDTABLES WHERE ARRAYINDEX = @COUNT)
                SET @LEFTJOINKEY = (SELECT LEFTJOINKEY FROM @EDITEDTABLES WHERE ARRAYINDEX = @COUNT)
            END
            SET @LASTUPDATE = (SELECT LASTRUNDATE FROM @EDITEDTABLES WHERE ARRAYINDEX = @COUNT)
            SET @RIGHTTABLE = 'GIS_READ_ONLY.sde.' + (SELECT JOINTABLE FROM @EDITEDTABLES WHERE ARRAYINDEX = @COUNT)
            SET @RIGHTJOINKEY = (SELECT RIGHTJOINKEY FROM @EDITEDTABLES WHERE ARRAYINDEX = @COUNT)

            BEGIN
            --ReturnEditGUIDs
            SET  @updateString = N'
            SELECT L.GUID FROM ' + @CURRENTTABLE + ' AS L
            INNER JOIN ' + @RIGHTTABLE + ' AS R ON L.' + @LEFTJOINKEY + '= R.' + @RIGHTJOINKEY +'
            WHERE R.LASTUPDATE AT TIME ZONE ''Eastern Standard Time'' AT TIME ZONE ''UTC'' > ''' +  CONVERT(VARCHAR, @LASTUPDATE) +'''' 
            
            INSERT INTO ##TBLEDITS(GUID)
            EXECUTE sp_executesql @updateString;

            END

            UPDATE ##TBLEDITS
            SET TABLENAME = (SELECT TABLENAME FROM @EDITEDTABLES WHERE ARRAYINDEX = @COUNT)
            WHERE TABLENAME IS NULL

        END
        """   
        queryUpdates = f""" 
        SELECT * FROM ##TBLEDITS
        """
        cursor.execute(createTemp)
        returnUpdates = cursor.execute(queryUpdates)
        updateTables = {}
        for i in returnUpdates:
            if i[1] is not None:
                if len(updateTables) == 0:
                    updateTables[i[0]] = [i[1]]
                else:
                    updateTables[i[0]] += [i[1]]

        return updateTables 
    
    except Exception as e:
        import traceback
        tb = sys.exc_info()[2]
        print(f"ReturnAgoEdits.py returnUpdatedJoin function exception on line {tb.tb_lineno} {e}")