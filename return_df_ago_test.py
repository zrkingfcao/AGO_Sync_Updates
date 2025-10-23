import sys, os, pandas as pd, logging
sys.path.insert(0,os.path.join(os.path.dirname(__file__),'\Scripts'))
from Send_Email import SendEmail

log = logging.getLogger("\\gisprocessp01.co.franklin.oh.us\e$\Logs\DataSynchronization_Driver.log")

def returnDF(sheetName):
    try:
        fieldMapper = r'\\gisprocessp01.co.franklin.oh.us\e$\Scripts\BranchVersionReplication\Sync_FieldMappings.xlsx'
        if not os.path.isfile(fieldMapper):
            print("Cannot find Field Mapping Excel file")
            subject = "Replication failed, field mapping spreadsheet not found"
            emailBody = f"Cannot find Field Mapping Excel file {fieldMapper}"
            SendEmail(subject, emailBody)
            log.exception(f"{subject} {emailBody}")
            exit()
            log.exception(emailBody)
        else:
            df = pd.read_excel(fieldMapper, sheet_name=None, header=None,engine='openpyxl')
            if sheetName in df:
                df = pd.read_excel(fieldMapper, sheet_name=sheetName, header=None,engine='openpyxl').fillna('') 
                return df

            else:
                print(f"{sheetName} cannot be found in {fieldMapper}")  
                subject = "Replication failed, field mapping work sheet not found"
                emailBody = f"{sheetName} cannot be found in {fieldMapper}"
                SendEmail(subject, emailBody)
                log.exception(f"{subject} {emailBody}")
                exit()
            
    except Exception as e:
        import traceback
        tb = sys.exc_info()[2]
        log.exception(f"returnDf.py returnDF function exception on line {tb.tb_lineno} {e}")




