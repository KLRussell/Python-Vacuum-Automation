from Vacuum_Global import Settings
from Vacuum_Global import SQLConnect

import pandas as pd

#self.DF.loc[self.DF['Gs_SrvType'] == 'LL',['Source_TBL','Source_ID','Gs_SrvID']]

class Seeds:
    Errors = None

    def __init__(self, Cost_Type, DF, folder_name, upload_date):
        self.Cost_Type = Cost_Type
        self.DF = DF
        self.folder_name = folder_name
        self.upload_date = upload_date