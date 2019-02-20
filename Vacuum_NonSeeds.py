from Vacuum_Global import Settings
from Vacuum_Global import SQLConnect

import pandas as pd

#self.DF.loc[self.DF['Gs_SrvType'] == 'LL',['Source_TBL','Source_ID','Gs_SrvID']]

class NonSeeds:
    Errors = None

    def __init__(self, DF, folder_name, upload_date):
        self.DF = DF
        self.folder_name = folder_name
        self.upload_date = upload_date