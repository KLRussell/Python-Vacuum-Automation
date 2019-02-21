from Vacuum_Global import Settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import Append_Errors

import pandas as pd

#self.DF.loc[self.DF['Gs_SrvType'] == 'LL',['Source_TBL','Source_ID','Gs_SrvID']]

class BMIPCI:
    Success = False

    def __init__(self, action, DF, upload_date):
        self.action = action
        self.DF = DF
        self.upload_date = upload_date

    def CheckMapped(self, Gs_SrvType, Col):
        DF_Results = pd.DataFrame()
        data = self.DF.loc[self.DF['Gs_SrvType'] == Gs_SrvType]

        if not data.empty:
            SQL = SQLConnect('sql')
            ASQL = SQLConnect('alch')
            SQL.connect()
            ASQL.connect()

            ASQL.upload(data, 'mytbl')

            DF_Results = SQL.query('''
                select
                    A.*
                from mytbl As A
                left join {0} As B
                on
                    A.Gs_SrvID = B.{1}

                where
                    B.{1} is null'''.format(Settings[Gs_SrvType],Col)
            )

            DF_Results['Error_Columns'] = 'Gs_SrvType, Gs_SrvID'
            DF_Results['Error_Message'] = 'Gs_SrvID in {0} is not found in {1}.{2}'.format(Gs_SrvType,Settings['Cus_Inv_Schema'],sqltbl)

            ASQL.execute("drop table mytbl")

            SQL.close()
            ASQL.close()

        Append_Errors(DF_Results)

        if data.empty:
            self.Success = False
        elif DF_Results.empty:
            self.Success = True
        elif not len(data.index) == len(DF_Results.index):
            self.Success = True

    def CheckDispute(self, Source_TBL):
        data = self.DF.loc[self.DF['Source_TBL'] == Source_TBL]

        if data:
            ASQL = SQLConnect('alch')
            SQL = SQLConnect('sql')
            SQL.connect()
            ASQL.connect()

            ASQL.upload(data, 'mytbl')

            if Source_TBL == 'BMI':
                DF_Results = ASQL.query('''
                    select
                        A.*
                    from mytbl As A
                    left join {0} As B
                    on
                        A.Source_ID = B.BMI_ID

                    where
                        B.Invoice_Date > eomonth(dateadd(month, -1, A.Invoice_Date))
                            and
                        B.Amount > 0
                            and
                        B.BMI_ID is null'''.format(Settings['MRC'])

                )
            elif Source_TBL == 'PCI':
                DF_Results = ASQL.query('''
                    select
                        A.*
                    from mytbl As A
                    left join {0} As B
                    on
                        A.Source_ID = B.{1}

                    where
                        B.{1} is null'''.format(Settings['PaperCost'])
                )

            SQL.close()
            ASQL.close()

    def Validate(self):
        self.Success = False

        if self.action == 'Map':
            self.CheckMapped('LL', 'ORD_WTN')
            self.CheckMapped('BRD', 'ORD_BRD_ID')
            self.CheckMapped('DED', 'CUS_DED_ID')
            self.CheckMapped('LD', 'ORD_WTN')
            self.CheckMapped('TF', 'ORD_POTS_ANI_BIL')
        elif self.action == 'Dispute':
            self.CheckDispute('STC', 'BMI')
            self.CheckDispute('STC', 'PCI')
            self.CheckDispute('Email', 'BMI')
            self.CheckDispute('Email', 'PCI')

        if self.Success:
            return True
