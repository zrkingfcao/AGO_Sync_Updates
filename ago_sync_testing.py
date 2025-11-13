
import arcpy, os, sys, logging, pandas as pd, time, traceback
from datetime import datetime

##testing modules
import signin, ago_cursor, timing, sync_operations, return_ago_edits, return_df_ago_test, getToken, post_sync
##end testing modules

sys.path.insert(0,os.path.join(os.path.dirname(__file__),'\Scripts'))
from Send_Email import SendEmail
from nightly_log import ConfigureLogger

script_filename = os.path.basename(__file__)
(file, ext) = os.path.splitext(script_filename)
ConfigureLogger(file)
log = logging.getLogger(__name__)



def processOperations(connection, ago):
    
    ##This is needed as the return statement in the _returnUpdatedJoin function returns a 1D list however, when instatiated it becomes a 2D list
    def returnDims(lst):
        if not isinstance(lst, list) or len(lst) == 0:
            return 0
        return 1 + returnDims(lst[0])
    
    def returnWhereClause(table, cursor):
        queryWC = f"SELECT WhereClause FROM sde.LastEditUpdate WHERE TableName = \'{table}\'"
        returnWC = cursor.execute(queryWC)
        whereClause = cursor.fetchone()
        if whereClause != '':
            return whereClause[0] 
        else:
            return None          
    
    try:
        cursor = connection[0]
        connSDE = connection[1]
        editedTables = return_ago_edits.returnEditedTables(cursor, ago)
        if editedTables is not None:
            signin.signInAGO()
            #print(f"Dictionary returned using the _returnUpdatedJoin method {editedTables}")
            for k,v in editedTables.items():
                ##TODO: Determine of backup is necessary
                # if k == 'TAX': Backup.createBackupSvc("CondoUnitStack_LGIM", True)
                # else:
                #     Backup.createBackupSvc(k, True)
                ##TODO: Update list of tables as needed
                if k in ['TAX']:
                    connection = ago_cursor.returnCursor("GIS_CADASTRAL_EDITING", "parceladmin")
                    cursor = connection[0]
                else:
                    connection = ago_cursor.returnCursor("GIS_EDITING", "sde")
                    cursor = connection[0]
                
                whereClause = returnWhereClause(k, cursor)
                # insert = return_ago_edits.returnInserted(k, cursor, ago, whereClause)
                # update = return_ago_edits.returnUpdated(k, cursor, ago, whereClause)
                edits = return_ago_edits.returnEdits(k, cursor, ago, whereClause)

                if k == 'TAX':
                    df = return_df_ago_test.returnDF('CondoUnitStack_LGIM')
                else:
                    df = return_df_ago_test.returnDF(k)
                source = df.iat[1,1]

                token = getToken.generateToken()
                destination = f"{df.iat[2,1]}?token={token}"


                guids = []
                with arcpy.da.SearchCursor(destination, 'GUID') as guidCursor:
                    for row in guidCursor:
                        guids.append(row[0][1:-1].upper())

                insert = list(set(edits) - set(guids))
                update = list(set(edits) & set(guids))
                delete = return_ago_edits.returnDeleted(k, cursor, ago, whereClause)
                ##FIXME: Use return_ago_edits.checkEditingErrors() to identify edits made causing feature to fall outside of where clause parameters, omit where clauses
                ## that are only comprised of 'RetiredByRecord IS NULL' as these features will be passed to the delete list via the necessary query. If these are found
                ## in the AGO service, they will need to be removed as they are no longer considered valid features
                if whereClause is not None and whereClause != 'RetiredByRecord IS NULL':
                    editErrors = return_ago_edits.checkEditingErrors(k, cursor, ago, whereClause)
                    if editErrors is not None:
                        delete =  list(dict.fromkeys(list((set(editErrors) & set(guids))) + delete)) 
                log.info(f"Total Insert Records before processing: {len(insert)}, Total Update Records before processing: {len(update)}, Total Delete Records before processing: {len(delete)}")

                ##Check if values in insert can also be found in delete list, if true, remove the value from insert to avoid unneccesary processing
                if len (insert) > 0 and len(delete) > 0: 
                    insert = [i for i in insert if i not in delete]      
                ##TODO: Remove when done testing LGIM updates, will not be necessary to track data movement once POC is working properly
                now = datetime.now().strftime("%Y_%m_%d_%H%M")
                adminDir = f"E:\Scripts\BranchVersionReplication\AdminRecords\AGO_Sync_{now}"
                if not os.path.exists(adminDir):
                    os.makedirs(adminDir)
                adminFile = os.path.join(adminDir,f"{k}_{now}.txt")
                with open(adminFile, "w") as f:
                    f.write(f"Left table Updates:\nInsert - {insert}\nUpdate - {update}\nDelete - {delete}\nRight Table Updates:\n{v}")
                ##END TODO
                
                #print(f"Insert - {insert}\nUpdate - {update}\nDelete - {delete}")
                ##Match Case available in Python 3.10 only
                # match k:
                #     case 'TAX':
                #         df = ReturnDf.returnDF('CondoUnitStack_LGIM')
                #     case _:
                #         df = ReturnDf.returnDF(k)

                ##Adjust insert and delete tables to include update values as these will be treated as delete from destination table followed by update from source table
                if len(update)>0 or len(v)>0:
                    dims = returnDims(v)
                    if dims == 2: v = v[0]
                    ##TODO: Remove when done testing LGIM updates, will not be necessary to track data movement once POC is working properly
                    # with open(adminFile, "a") as f:
                    #     f.write(f"\nRight Table Updates:\n{v}")
                    ##END TODO
                    #print(f"Left Table Updates: {update}\nRight Table Updates:\n{v}")
                    
                    if len(v) > 0 and len(update) > 0:
                        log.info(f"Left table update count: {len(update)}, Right table update count: {len(v)}")
                        insert = list(set(insert + v + update)) 
                        delete = list(set(delete + v + update))
                    if len(v) > 0 and len(update) == 0:
                        log.info(f"No left table updates, Right table update count: {len(v)}")
                        insert = list(set(insert + v)) 
                        delete = list(set(delete + v))
                    if len(v) == 0 and len(update) > 0:
                        log.info(f"No right table updates, Left table update count: {len(update)}")
                        insert = list(set(insert + update)) 
                        delete = list(set(delete + update))                       
        
                    log.info(f"Total Insert Records after processing: {len(insert)}, Total Delete Records after processing: {len(delete)}")

                    if len(delete)>0:
                        sync_operations.updateDeleted(delete, destination)
                        #print(f"{k} Removed: {delete} from {destination}")
                    if len(insert)>0:
                        sync_operations.updateInserted(insert, source, destination)
                       # print(f"{k} Inserted: {insert} into {destination} from {source}")

                success = post_sync.checkSync(source, destination)
                print(f"Source & Destination Comparison: {success}")
                if success == True: 
                    if k == 'TAX': 
                        log.info(f"Finished updating CondoUnitStack_LGIM")
                    else:
                        log.info(f"Finished updating {k}")
                else:
                    emailBody = f"Unable to successfully update {k} as a mismatch in records was detected please check Source: {source} and Destination: {destination}"
                    log.error(emailBody)
                    subject = f"AGO Sync process encountured discrepancies on {k}" 
                    print(emailBody)
                    SendEmail(subject, emailBody)
        
                # if success == False:
                #     Backup.restoreBackup(k) 
                ##TODO: Consider using below in place of the restoreBackup function (NOTE: IF WORKS AS DESIRED,
                #  THE CODE ABOVE AFTER THE else STATEMENT WILL NEED TO BE REMOVED IN PLACE OF WHAT IS BELOW):
                    # backupDir = f"E:\Scripts\BranchVersionReplication\Restore\{k}_AGO_Sync_Failure_{now}"
                    # if not os.path.exists(backupDir):
                    #     os.makedirs(backupDir)
                    # arcpy.management.CreateFileGDB(backupDir, f"{k}_Copy.gdb")
                    # arcpy.conversion.ExportFeatures(destination, os.path.join(backupDir,f"{k}_Copy.gdb\\{k}_{now}"))
                    # arcpy.TruncateTable_management(destination)
                    # arcpy.Append_management(source, destination, "NO_TEST")
                    # emailBody = f"{k} was updated using truncate - append as a mismatch in records was detected following update process"
                    # log.error(emailBody)
                    # subject = f"AGO Sync process failed, {k} was updted using truncate - append\nUse {os.path.join(backupDir,f"{k}_Copy.gdb\\{k}_{now}") to determine point of failure}" 
                    # print(emailBody)
                    # SendEmail(subject, emailBody)


                if k in ['TAX']:
                    cursor.close()
                    connSDE.close()
                    connection = ago_cursor.returnCursor("GIS_CADASTRAL_EDITING", "sde")
                    cursor = connection[0]
                    connSDE = connection[1]
                updateLastRun = f"UPDATE sde.LastEditUpdate SET LastRunDate_AGO = GETUTCDATE() WHERE TableName = '{k}'"
                updateLastUpdate = f"UPDATE sde.LastEditUpdate SET UpdateDate_AGO = GETUTCDATE() WHERE TableName = '{k}'"
                readOnlySuccess = f"UPDATE sde.LastEditUpdate SET AGOSuccess = 1 WHERE TableName = '{k}'"
                cursor.execute(updateLastRun)
                cursor.execute(updateLastUpdate)
                cursor.execute(readOnlySuccess)

        else:
            print("No updates to process")
            updateLastRun = f"UPDATE LastEditUpdate SET LastRunDate_AGO = GETUTCDATE()"
            cursor.execute(updateLastRun)

        connSDE.commit()
        cursor.close()
        connSDE.close()

    except Exception as e:
        tb = sys.exc_info()[2]
        emailBody = f"Exception {e} on line {tb.tb_lineno} {tb}"
        print(emailBody)
        log.exception(emailBody)
        subject = "AGO Sync process failed due to exception" 
        #SendEmail(subject, emailBody)
     
if __name__ == '__main__':
    start = time.time() 

    print("Running AGO...")
    #connection = ago_cursor.returnCursor("GIS_EDITING", "sde")
    connection = ago_cursor.returnCursor("GIS_CADASTRAL_EDITING", "sde")
    processOperations(connection, True)

    end = time.time()
    elapsed = timing.timer(start,end)
    print(f"Time to complete: {elapsed[0]} min {elapsed[1]} sec")
    log.info(f"AGO Sync completed in {elapsed[0]} min {elapsed[1]} sec")
