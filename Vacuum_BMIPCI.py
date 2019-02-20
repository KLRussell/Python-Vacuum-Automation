from Vacuum_Global import Settings
from Vacuum_Global import SQLConnect

import pandas as pd

#self.DF.loc[self.DF['Gs_SrvType'] == 'LL',['Source_TBL','Source_ID','Gs_SrvID']]

class BMIPCI:
    Errors = None

    def __init__(self, action, DF, upload_date):
        self.action = action
        self.DF = DF
        self.upload_date = upload_date

    def Validate(self):
        SQL = SQLConnect('sql')

        if self.action == 'Map':
            '''data = self.DF.loc[self.DF['Gs_SrvType'] == 'LL']
            placeholders = ', '.join(['%s'] * len(data))
            columns = ', '.join(data.keys())
            sqlstr = 'create table #tmpTBL ({})'.format(columns)
            self.Errors = SQL.query("")
            '''
            print(self.DF.loc[self.DF['Gs_SrvType'] == 'LL'])

        SQL.close()

