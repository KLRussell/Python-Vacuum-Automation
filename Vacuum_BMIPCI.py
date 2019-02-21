from Vacuum_Global import Settings
from Vacuum_Global import SQLConnect

import pandas as pd

#self.DF.loc[self.DF['Gs_SrvType'] == 'LL',['Source_TBL','Source_ID','Gs_SrvID']]

class BMIPCI:
    Errors = []

    def __init__(self, action, DF, upload_date):
        self.action = action
        self.DF = DF
        self.upload_date = upload_date

    def Validate(self):
        def CheckMapped(SQL, Gs_SrvType, sqltbl, Col):
            SQL.connect()
            ASQL.connect()

            data = self.DF.loc[self.DF['Gs_SrvType'] == Gs_SrvType]

            if not data.empty:
                ASQL.upload(data, 'mytbl')

                self.Errors.append(
                    [
                    SQL.query('''
                        select
                            A.*
                        from mytbl As A
                        left join {0}.{1} As B
                        on
                            A.Gs_SrvID = B.{2}

                        where
                            B.{2} is null'''.format(Settings['Cus_Inv_Schema'],sqltbl,Col)
                    ), 'Gs_SrvType, Gs_SrvID'
                    , 'Gs_SrvID in {0} is not found in {1}.{2}'.format(Gs_SrvType,Settings['Cus_Inv_Schema'],sqltbl)
                    ]
                )

                ASQL.execute("drop table mytbl")

            SQL.close()
            ASQL.close()

        SQL = SQLConnect('sql')
        ASQL = SQLConnect('alch')

        if self.action == 'Map':
            CheckMapped(SQL, 'LL', 'ORDER_LOCAL_NEW', 'ORD_WTN')
            CheckMapped(SQL, 'BRD', 'ORDER_BROADBAND', 'ORD_BRD_ID')
            CheckMapped(SQL, 'DED', 'ORDER_DEDICATED', 'CUS_DED_ID')
            CheckMapped(SQL, 'LD', 'ORDER_1PLUS', 'ORD_WTN')
            CheckMapped(SQL, 'TF', 'ORDER_800', 'ORD_POTS_ANI_BIL')

        if not self.Errors:
            return True
