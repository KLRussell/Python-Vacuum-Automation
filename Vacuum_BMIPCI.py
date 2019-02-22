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
            DF_Results['Error_Message'] = 'Gs_SrvID in {0} is not found in {1}'.format(Gs_SrvType,Settings[Gs_SrvType])

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

            SQL.execute('''
                update A
                set A.Start_Date = dateadd(day, (-1 * B.Dispute_Limit) + 15, getdate())
                from mytbl As A
                inner join {0} As B
                on
                    A.BAN = B.BAN

                where
                    A.Start_Date is null
            '''.format(Settings['Limitations']))

            SQL.execute('''
                update A
                set A.Start_Date = eomonth(dateadd(month, -1, A.Start_Date))
                from mytbl As A
                where
                    A.Start_Date is not null
            ''')

            if Source_TBL == 'BMI':
                DF_Results = SQL.query('''
                    with
                        TMP
                    As
                    (
                        select
                            A.*

                        from mytbl As A
                        left join {0} As B
                        on
                            A.Source_ID = B.BMI_ID
                                and
                            B.Invoice_Date > A.Start_Date
                                and
                            B.Amount > 0

                        where
                            B.BDT_MRC_ID is null
                    ),
                        TMP2
                    As
                    (
                        select
                            A.BMI_ID,
                            B.BDT_MRC_ID,
                            B.Invoice_Date

                        from {1} As A
                        inner join {2} As B
                        on
                            A.Vendor = B.Vendor
                                and
                            A.BAN = B.BAN
                                and
                            A.WTN = B.BTN
                                and
                            A.Circuit_ID = B.Circuit_ID

                        where
                            B.Amount > 0
                    )

                    select
                        A.*
                    from TMP As A
                    left join TMP2 As B
                    on
                        A.Source_ID = B.BMI_ID
                            and
                        A.Start_Date < B.Invoice_Date

                    where
                        B.BDT_OCC_ID is null'''.format(Settings['MRC'],Settings['BMI'],Settings['OCC'])
                )

                DF_Results['Error_Columns'] = 'Source_TBL, Source_ID'
                DF_Results['Error_Message'] = 'Valid cost for Source_ID of {0} is not found in MRC or OCC tables'.format(Source_TBL)

            elif Source_TBL == 'PCI':
                DF_Results = SQL.query('''
                    select
                        A.*
                    from mytbl As A
                    left join {0} As B
                    on
                        A.Source_ID = B.PCI_ID
                            and
                        B.Bill_Date > A.Start_Date
                            and
                        (B.MRC > 0 or B.FRAC > 0 or B.NRC > 0)

                    where
                        B.Seed is null'''.format(Settings['PaperCost'])
                )

                DF_Results['Error_Columns'] = 'Source_TBL, Source_ID'
                DF_Results['Error_Message'] = 'Valid cost for Source_ID of {0} is not found in Papercost'.format(Source_TBL)

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

    def CheckProv(self):
        ASQL = SQLConnect('alch')
        SQL = SQLConnect('sql')
        SQL.connect()
        ASQL.connect()

        ASQL.upload(self.DF, 'mytbl')

        ASQL.execute("drop table mytbl")

        DF_Results = SQL.query('''
            select
                A.*,
                iif(B.MACNUM is null,'Macnum','Source_TBL, Source_ID') As Error_Columns,
                iif(B.MACNUM is null,'This Macnum does not exist in CS','This Source_TBL and Source_ID is already pending in Send to Prov table') As Error_Message

            from mytbl As A
            left join {0} As B
            on
                A.Macnum = B.Macnum
            left join {1} As C
            on
                A.Source_TBL = C.Source_TBL
                    and
                A.Source_ID = C.Source_ID

            where
                B.MACNUM is null
                    or
                (
                    C.Root_Cause is null
                        and
                    C.Prov_Note is null
                        and
                    C.New_Root_Cause is null
                        and
                    C.Audit_ID is not null
                )'''.format(Settings['Cust_File'], Settings['Send_To_Prov'])
        )

        SQL.close()
        ASQL.close()

        Append_Errors(DF_Results)

        if DF_Results.empty:
            self.Success = True
        elif not len(self.DF.index) == len(DF_Results.index):
            self.Success = True

    def CheckLV(self):
        ASQL = SQLConnect('alch')
        SQL = SQLConnect('sql')
        SQL.connect()
        ASQL.connect()

        ASQL.upload(self.DF, 'mytbl')

        DF_Results = SQL.query('''
            select
                A.*
            from mytbl As A
            left join {0} As B
            on
                A.Macnum = B.Macnum

            where
                B.MACNUM is null'''.format(Settings['Cust_File'])
        )

        DF_Results['Error_Columns'] = 'Macnum'
        DF_Results['Error_Message'] = 'This Macnum does not exist in CS'

        ASQL.execute("drop table mytbl")

        SQL.close()
        ASQL.close()

        Append_Errors(DF_Results)

        if DF_Results.empty:
            self.Success = True
        elif not len(self.DF.index) == len(DF_Results.index):
            self.Success = True

    def CheckDN(self):
        ASQL = SQLConnect('alch')
        SQL = SQLConnect('sql')
        SQL.connect()
        ASQL.connect()

        ASQL.upload(self.DF, 'mytbl')

        DF_Results = SQL.query('''
        ''')

        DF_Results['Error_Columns'] = 'Macnum'
        DF_Results['Error_Message'] = 'This Macnum does not exist in CS'

        ASQL.execute("drop table mytbl")

        SQL.close()
        ASQL.close()

        Append_Errors(DF_Results)

        if DF_Results.empty:
            self.Success = True
        elif not len(self.DF.index) == len(DF_Results.index):
            self.Success = True

    def Validate(self):
        self.Success = False

        if self.action == 'Map':
            self.CheckMapped('LL', 'ORD_WTN')
            self.CheckMapped('BRD', 'ORD_BRD_ID')
            self.CheckMapped('DED', 'CUS_DED_ID')
            self.CheckMapped('LD', 'ORD_WTN')
            self.CheckMapped('TF', 'ORD_POTS_ANI_BIL')
        elif self.action == 'Dispute':
            self.CheckDispute('BMI')
            self.CheckDispute('PCI')
        elif self.action == 'Sent to Prov':
            self.CheckProv()
        elif self.action == 'Sent to LV':
            self.CheckLV()
        elif self.action == 'Dispute Note':
            self.CheckDN()

        if self.Success:
            return True
