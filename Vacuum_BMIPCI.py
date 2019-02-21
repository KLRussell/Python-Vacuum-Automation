from Vacuum_Global import Settings
from Vacuum_Global import SQLConnect

import pandas as pd

#self.DF.loc[self.DF['Gs_SrvType'] == 'LL',['Source_TBL','Source_ID','Gs_SrvID']]

class BMIPCI:
    Errors = pd.DataFrame()

    def __init__(self, action, DF, upload_date):
        self.action = action
        self.DF = DF
        self.upload_date = upload_date

    def Append_Errors(self, DF):
        if not DF.empty:
            if self.Errors.empty:
                self.Errors = DF
            else:
                self.Errors.append(DF)

    def Validate(self):
        def CheckMapped(SQL, ASQL, Gs_SrvType, sqltbl, Col):
            SQL.connect()
            ASQL.connect()

            DF_Results = pd.DataFrame()

            data = self.DF.loc[self.DF['Gs_SrvType'] == Gs_SrvType]

            if not data.empty:
                ASQL.upload(data, 'mytbl')

                DF_Results = SQL.query('''
                        select
                            A.*
                        from mytbl As A
                        left join {0}.{1} As B
                        on
                            A.Gs_SrvID = B.{2}

                        where
                            B.{2} is null'''.format(Settings['Cus_Inv_Schema'],sqltbl,Col)
                    )

                DF_Results['Error_Columns'] = 'Gs_SrvType, Gs_SrvID'
                DF_Results['Error_Message'] = 'Gs_SrvID in {0} is not found in {1}.{2}'.format(Gs_SrvType,Settings['Cus_Inv_Schema'],sqltbl)
                ASQL.execute("drop table mytbl")

            SQL.close()
            ASQL.close()

            return DF_Results

        SQL = SQLConnect('sql')
        ASQL = SQLConnect('alch')

        if self.action == 'Map':
            self.Append_Errors(CheckMapped(SQL, ASQL, 'LL', 'ORDER_LOCAL_NEW', 'ORD_WTN'))
            self.Append_Errors(CheckMapped(SQL, ASQL, 'BRD', 'ORDER_BROADBAND', 'ORD_BRD_ID'))
            self.Append_Errors(CheckMapped(SQL, ASQL, 'DED', 'ORDER_DEDICATED', 'CUS_DED_ID'))
            self.Append_Errors(CheckMapped(SQL, ASQL, 'LD', 'ORDER_1PLUS', 'ORD_WTN'))
            self.Append_Errors(CheckMapped(SQL, ASQL, 'TF', 'ORDER_800', 'ORD_POTS_ANI_BIL'))
            print(self.Errors)

        if not self.Errors.empty:
            return True
