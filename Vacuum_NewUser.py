from Vacuum_Global import Settings
from Vacuum_Global import SQLConnect

import pandas as pd

#self.DF.loc[self.DF['Gs_SrvType'] == 'LL',['Source_TBL','Source_ID','Gs_SrvID']]

class NewUser:
    Errors = None

    def __init__(self, file, upload_date):
        self.file = file
        self.upload_date = upload_date