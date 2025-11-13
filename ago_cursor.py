import configparser, pyodbc, logging

log = logging.getLogger("\\gisprocessp01.co.franklin.oh.us\e$\Logs\DataSynchronization_Driver.log")

##FIXME: Consider parameter to check if sde or parceladmin login is needed for GIS_CASTRAL_EDITING.
def returnCursor(DB, login):
    try:
        configFile = configparser.ConfigParser()
        configFile.read(r'\\gisprocessp01.co.franklin.oh.us\e$\Scripts\ChangeDetection\config.ini')
        sqlServerPrd = configFile.get('Prd','sqlServer')
        if login.lower() == "sde":
            sqlServerUser = configFile.get('SDE','username')
            sqlServerPassword = configFile.get('SDE','password')
        else:
            sqlServerUser = configFile.get('ParcelAdmin','username')
            sqlServerPassword = configFile.get('ParcelAdmin','password')

        connectionString = "DRIVER=ODBC Driver 18 for SQL Server;SERVER={};DATABASE={};UID={};PWD={};Encrypt=YES;TrustServerCertificate=YES".format(sqlServerPrd,DB,sqlServerUser,sqlServerPassword)
        connSDE = pyodbc.connect(connectionString)
        cursor = connSDE.cursor()
        return [cursor,connSDE]
    
    except Exception as e:
        import traceback, sys
        tb = sys.exc_info()[2]
        log.exception(f"returnCursor.py returnCursor function exception on line {tb.tb_lineno} {e}")
