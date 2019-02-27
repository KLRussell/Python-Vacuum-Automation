from Vacuum_Global import Settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import Append_Errors
from Vacuum_Global import Get_Errors

import pandas as pd, datetime

#self.DF.loc[self.DF['Gs_SrvType'] == 'LL',['Source_TBL','Source_ID','Gs_SrvID']]

class BMIPCI:
    def __init__(self, action, DF, upload_date):
        self.action = action
        self.DF = DF
        self.upload_date = upload_date

    def Update_Map(self, ASQL, Source_TBL, BMB_TBL, BMM_TBL, Unmapped_TBL):
            param = None

            if Source_TBL == 'BMI':
                param = ", C.frameID=B.initials"

            ASQL.execute('''
                insert into {1}
                (
                    gs_srvID,
                    gs_srvType,
                    {0}_ID,
                    Rep
                )
                select
                    A.gs_srvID,
                    A.gs_srvType,
                    A.Source_ID,
                    B.initials

                from mytbl2 As A
                inner join {2} As B
                on
                    A.Comp_Serial = B.Comp_Serial

                where
                    A.BMB = 1
                        and
                    A.Error_Columns is null
                '''.format(Source_TBL, BMB_TBL, Settings['CAT_Emp'])
            )

            ASQL.execute('''
                update C
                    set
                        C.gs_SrvID=iif(A.BMB=1,A.Source_ID,A.gs_SrvID),
                        C.gs_SrvType=iif(A.BMB=1,'BMB',A.gs_SrvType),
                        C.Edit_Date=getdate()
                        {3}

                from mytbl2 As A
                inner join {2} As B
                on
                    A.Comp_Serial = B.Comp_Serial
                inner join {1} As C
                on
                    A.Source_ID = C.{0}_ID

                where
                    A.Error_Columns is null
                '''.format(Source_TBL, BMM_TBL, Settings['CAT_Emp'], param)
            )

            ASQL.execute('''
                insert into {1}
                (
                    {0}_ID,
                    Audit_Result,
                    Rep,
                    Comment,
                    Edit_Date
                )
                select
                    A.Source_ID,
                    'Mapped',
                    B.initials,
                    A.action_comment,
                    getdate()

                from mytbl2 As A
                inner join {2} As B
                on
                    A.Comp_Serial = B.Comp_Serial

                where
                    A.Error_Columns is null
            '''.format(Source_TBL, Unmapped_TBL, Settings['CAT_Emp'])
            )

    def Map(self, Gs_SrvType, Col, Source_TBL):
        DF_Results = pd.DataFrame()
        data = self.DF.loc[(self.DF['Gs_SrvType'] == Gs_SrvType) & (self.DF['Source_TBL'] == Source_TBL)]

        if not data.empty:
            ASQL = SQLConnect('alch')
            ASQL.connect()

            if not 'action_comment' in data.columns:
                data['Action_Comment'] = None

            data['Error_Columns'] = None
            data['Error_Message'] = None

            ASQL.upload(data, 'mytbl')

            if ASQL.query("select object_id('mytbl2')").iloc[0, 0]:
                ASQL.execute("drop table mytbl2")

            ASQL.upload(ASQL.query('''
                with
                    MYTMP
                As
                (
                    select
                        {0},
                        iif(charindex(',',Gs_SrvID)>0,1,0) As BMB,
                        cast
                        (
                            '<head><page><![CDATA['
                                +
                            replace(Gs_SrvID, ',', ']]></page><page><![CDATA[')
                                +
                            ']]></page></head>' as XML
                        ) As tempVar

                    from mytbl
                ),
                    MYTMP2
                As
                (
                    select
                        {0},
                        BMB,
                        MY_Tbl.My_Col.value('.','VARCHAR(max)') Gs_SrvID

                    from MYTMP tempVar
                        cross apply tempvar.nodes('/head/page') MY_Tbl(My_Col)
                )

                select
                    *
                from MYTMP2'''.format(",".join(data.loc[:,data.columns != 'Gs_SrvID'].columns.values))
            ), 'mytbl2')

            ASQL.execute("drop table mytbl;")

            ASQL.execute('''
                update A
                    set
                        A.Error_Columns = 'Gs_SrvType, Gs_SrvID',
                        A.Error_Message = 'Gs_SrvID in {2} is not found in {0}'

                from mytbl2 As A
                left join {0} As B
                on
                    A.Gs_SrvID = B.{1}

                where
                    B.{1} is null'''.format(Settings[Gs_SrvType],Col,Gs_SrvType)
            )

            if Source_TBL == 'BMI':
                self.Update_Map(ASQL, Source_TBL, Settings['BMB'], Settings['BMM'], Settings['Unmapped'])
            else:
                self.Update_Map(ASQL, Source_TBL, Settings['PCI_BMB'], Settings['PCI'], Settings['PCI_Unmapped'])

            ASQL.execute("drop table mytbl2")

            ASQL.close()

            Append_Errors(DF_Results)

        del data, DF_Results

    def GetBatch(self, AsDate=False):
        current_time = datetime.datetime.now()
        last_friday = (current_time.date()
            - datetime.timedelta(days=current_time.weekday())
            + datetime.timedelta(days=4, weeks=-1))
        if AsDate:
            return last_friday
        else:
            return last_friday.__format__("%Y%m%d")

    def Dispute_Seeds(self, ASQL, data, Source, TBL, Seed, on, where, Cost_Type, Record_Type, inner=""):

        Cols = "A." + ", A.".join(data.loc[:,~data.columns.isin(['Action', 'Start_Date'])].columns.values)
        Cols2 = ",".join(data.loc[:,~data.columns.isin(['Action', 'Start_Date'])].columns.values)

        myquery = '''
            select
                {3} As Seed,
                '{5}' As Cost_Type,
                {6} As Record_Type,
                {4}

            from mytbl As A
            {7}
            inner join {0} As B
            on
                {1}
                    and
                {2}'''.format(TBL, on, where, Seed, Cols, Cost_Type, Record_Type, inner)

        if not ASQL.query("select object_id('mytbl2')").iloc[0, 0]:
            myresults = ASQL.query(myquery)

            if not myresults.empty:
                ASQL.upload(myresults, 'mytbl2')
        else:
            ASQL.execute('''
                insert into mytbl2
                (
                    Seed,
                    Cost_Type,
                    Record_Type,
                    {0}
                )
            '''.format(Cols2) + myquery)

    def Dispute(self, Source, Source_Col):
        DF_Results = pd.DataFrame()
        data = self.DF.loc[self.DF['Source_TBL'] == Source]

        if not data.empty:
            print("Processing disputes for {}".format(Source))

            if not 'Start_Date' in data.columns:
                data['Start_Date'] = None
                select = 'Seed'
                data['Start_Date'] = data['Start_Date'].astype('datetime64[D]')

            if not 'USI' in data.columns:
                data['USI'] = None

            if not 'PON' in data.columns:
                data['PON'] = None

            if not 'Action_Comment' in data.columns:
                data['Action_Comment'] = None

            ASQL = SQLConnect('alch')
            ASQL.connect()

            ASQL.upload(data, 'mytbl')

            ASQL.execute('''
                update A
                    set A.Claim_Channel = 'Email'

                from mytbl As A

                where
                    A.Source_TBL = 'PCI'
                    ''')

            ASQL.execute('''
                update A
                    set A.Start_Date = dateadd(day, (-1 * C.Dispute_Limit) + 15, getdate())
                from mytbl As A
                inner join {1} As B
                on
                    A.Source_ID = B.{0}
                inner join {2} As C
                on
                    B.BAN = C.BAN

                where
                    A.Start_Date is null
                '''.format(Source_Col,Settings[Source],Settings['Limitations'])
            )

            ASQL.execute('''
                update A
                    set A.Start_Date = eomonth(dateadd(month, -1, A.Start_Date))
                from mytbl As A

                where
                    A.Start_Date is not null'''
            )

            if ASQL.query("select object_id('mytbl2')").iloc[0, 0]:
                ASQL.execute("drop table mytbl2")

            if Source == 'BMI':
                print("Grabbing MRC Cost")

                self.Dispute_Seeds(
                    ASQL,
                    data,
                    Source,
                    Settings['MRC'],
                    'BDT_MRC_ID',
                    'B.Invoice_Date > A.Start_Date and A.Source_ID = B.BMI_ID',
                    'Amount > 0',
                    'MRC',
                    "'MRC'"
                )

                print("Grabbing OCC Cost")

                self.Dispute_Seeds(
                    ASQL,
                    data,
                    Source,
                    Settings['OCC'],
                    'BDT_OCC_ID',
                    'B.Invoice_Date > A.Start_Date and B.Vendor = C.Vendor and B.BAN = C.BAN and B.BTN = C.WTN and B.Circuit_ID = C.Circuit_ID',
                    'Amount > 0',
                    'OCC',
                    "upper(Activity_Type)",
                    "inner join {0} As C on A.Source_ID = C.BMI_ID".format(Settings['BMI'])
                )
            else:
                print("Grabbing PaperCost MRC Cost")

                self.Dispute_Seeds(
                    ASQL,
                    data,
                    Source,
                    Settings['PaperCost'],
                    'Seed',
                    'B.Bill_Date > A.Start_Date and A.Source_ID = B.PCI_ID',
                    'MRC > 0',
                    'PC-MRC',
                    "'MRC'"
                )

                print("Grabbing PaperCost NRC Cost")

                self.Dispute_Seeds(
                    ASQL,
                    data,
                    Source,
                    Settings['PaperCost'],
                    'Seed',
                    'B.Bill_Date > A.Start_Date and A.Source_ID = B.PCI_ID',
                    'NRC > 0',
                    'PC-NRC',
                    "'NRC'"
                )

                print("Grabbing PaperCost FRAC Cost")

                self.Dispute_Seeds(
                    ASQL,
                    data,
                    Source,
                    Settings['PaperCost'],
                    'Seed',
                    'B.Bill_Date > A.Start_Date and A.Source_ID = B.PCI_ID',
                    'FRAC > 0',
                    'PC-FRAC',
                    "'FRAC'"
                )

            if ASQL.query("select object_id('mytbl2')").iloc[0, 0]:
                if ASQL.query("select object_id('DS')").iloc[0, 0]:
                    ASQL.execute("DROP TABLE DS")

                if ASQL.query("select object_id('DSB')").iloc[0, 0]:
                    ASQL.execute("DROP TABLE DSB")

                if ASQL.query("select object_id('DH')").iloc[0, 0]:
                    ASQL.execute("DROP TABLE DH")

                ASQL.execute("CREATE TABLE DSB (DSB_ID int, Stc_Claim_Number varchar(255))")
                ASQL.execute("CREATE TABLE DS (DS_ID int, DSB_ID int)")
                ASQL.execute("CREATE TABLE DH (DH_ID int, DSB_ID int)")

                print("Disputing {} cost".format(Source))

                ASQL.execute('''
                    insert into {0}
                    (
                        USI,
                        STC_Claim_Number,
                        Source_TBL,
                        Source_ID
                    )

                    OUTPUT
                        INSERTED.DSB_ID,
                        INSERTED.Stc_Claim_Number

                    INTO DSB

                    select
                        USI,
                        '{1}_' + left(Record_Type,1) + cast(Seed as varchar),
                        Source_TBL,
                        Source_ID

                    from mytbl2;'''.format(Settings['Dispute_Staging_Bridge'],self.GetBatch())
                )

                ASQL.execute('''
                    insert into {0}
                    (
                        DSB_ID,
                        Rep,
                        STC_Claim_Number,
                        Dispute_Type,
                        Dispute_Category,
                        Audit_Type,
                        Cost_Type,
                        Cost_Type_Seed,
                        Record_Type,
                        Dispute_Reason,
                        PON,
                        Comment,
                        Confidence,
                        Batch_DT
                    )

                    OUTPUT
                        INSERTED.DS_ID,
                        INSERTED.DSB_ID

                    INTO DS

                    select
                        DSB.DSB_ID,
                        B.Full_Name,
                        '{2}_' + left(A.Record_Type,1) + cast(A.Seed as varchar),
                        A.Claim_Channel,
                        'GRT CNR',
                        'CNR Audit',
                        A.Cost_Type,
                        A.Seed,
                        A.Record_Type,
                        A.Action_Reason,
                        A.PON,
                        A.Action_Comment,
                        A.Confidence,
                        '{3}'

                    from mytbl2 As A
                    inner join {1} As B
                    on
                        A.Comp_Serial = B.Comp_Serial
                    inner join DSB
                    on
                        DSB.Stc_Claim_Number='{2}_' + left(A.Record_Type,1) + cast(A.Seed as varchar)
                '''.format(Settings['DisputeStaging'], Settings['CAT_Emp'], self.GetBatch(), self.GetBatch(True))
                )

                ASQL.execute('''
                    insert into {0}
                    (
                        DSB_ID,
                        Dispute_Category,
                        Display_Status,
                        Date_Submitted,
                        Dispute_Reason,
                        GRT_Update_Rep,
                        Date_Updated,
                        Source_File
                    )

                    OUTPUT
                        INSERTED.DH_ID,
                        INSERTED.DSB_ID

                    INTO DH

                    select
                        DSB.DSB_ID,
                        'GRT CNR',
                        'Filed',
                        getdate(),
                        A.Action_Reason,
                        B.Full_Name,
                        getdate(),
                        'GRT Email: ' + format(getdate(),'yyyyMMdd')

                    from mytbl2 As A
                    inner join {1} As B
                    on
                        A.Comp_Serial = B.Comp_Serial
                    inner join DSB
                    on
                        DSB.Stc_Claim_Number='{2}_' + left(A.Record_Type,1) + cast(A.Seed as varchar)

                    where
                        A.Claim_Channel = 'Email'
                '''.format(Settings['Dispute_History'], Settings['CAT_Emp'], self.GetBatch())
                )

                ASQL.execute('''
                    insert into {0}
                    (
                        DS_ID,
                        DSB_ID,
                        DH_ID,
                        Open_Dispute
                    )
                    select
                        DS.DS_ID,
                        DS.DSB_ID,
                        DH.DH_ID,
                        1

                    from DSB
                    inner join DS
                    on
                        DSB.DSB_ID = DS.DSB_ID
                    left join DH
                    on
                        DSB.DSB_ID = DH.DSB_ID
                '''.format(Settings['Dispute_Fact'])
                )

                DF_Results = ASQL.query('''
                    with
                        MY_TMP
                    As
                    (
                        select distinct
                            Source_TBL,
                            Source_ID

                        from mytbl2
                    )

                    select
                        A.*

                    from mytbl As A
                    left join MY_TMP As B
                    on
                        A.Source_TBL = B.Source_TBL
                            and
                        A.Source_ID = B.Source_ID

                    where
                        B.Source_TBL is null
                '''
                )

                if not DF_Results.empty:
                    DF_Results['Error_Columns'] = 'Source_TBL, Source_ID, Start_Date'
                    DF_Results['Error_Message'] = 'Valid cost was not found for the Source_TBL, Source_ID, Start_Date combo'

                ASQL.execute("drop table mytbl, mytbl2, DS, DSB, DH")
            else:
                print("Warning! No cost found for {} of spreadsheet".format(Source))

                DF_Results = ASQL.query('select * from mytbl')

                DF_Results['Error_Columns'] = 'Source_TBL, Source_ID, Start_Date'
                DF_Results['Error_Message'] = 'Valid cost was not found for the Source_TBL, Source_ID, Start_Date combo'

                ASQL.execute("drop table mytbl")

            ASQL.close()

            Append_Errors(DF_Results)

        del data, DF_Results

    def CheckProv(self):
        ASQL = SQLConnect('alch')
        SQL = SQLConnect('sql')
        SQL.connect()
        ASQL.connect()

        ASQL.upload(self.DF, 'mytbl')

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

        ASQL.execute("drop table mytbl")

        SQL.close()
        ASQL.close()

        Append_Errors(DF_Results)

        del DF_Results

    def CheckLV(self):
        ASQL = SQLConnect('alch')
        SQL = SQLConnect('sql')
        SQL.connect()
        ASQL.connect()

        ASQL.upload(self.DF, 'mytbl')

        DF_Results = SQL.query('''
            select
                A.*,
                iif(B.MACNUM is null,'Macnum','Source_TBL, Source_ID') As Error_Columns,
                iif(B.MACNUM is null,'This Macnum does not exist in CS','This Source_TBL and Source_ID is already pending in Send to LV table') As Error_Message

            from mytbl As A
            left join {0} As B
            on
                A.Macnum = B.Macnum
            left join {1} As C
            on
                A.Source_TBL = C.Source_TBL
                    and
                A.Source_ID = C.Source_ID
                    and
                (
                    C.Is_Rejected is null
                        or
                    C.Sugg_Action is null
                )

            where
                B.MACNUM is null
                    or
                C.STL_ID is not null'''.format(Settings['Cust_File'], Settings['Send_To_LV'])
        )

        ASQL.execute("drop table mytbl")

        SQL.close()
        ASQL.close()

        Append_Errors(DF_Results)

        del DF_Results

    def CheckDN(self):
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
                B.Open_Dispute = 1
                    and
                B.Norm_Dispute_Category = 'GRT CNR'
            left join {1} As C
            on
                B.DSB_ID = C.DSB_ID
                    and
                A.Source_TBL = C.Source_TBL
                    and
                A.Source_ID = C.Source_ID

            where
                B.DF_ID is null
        '''.format(Settings['Dispute_Fact'],Settings['Dispute_Staging_Bridge'])
        )

        DF_Results['Error_Columns'] = 'Source_TBL, Source_ID'
        DF_Results['Error_Message'] = 'There are no open GRT CNR disputes for this Source_TBL & Source_ID combo'

        ASQL.execute("drop table mytbl")

        SQL.close()
        ASQL.close()

        Append_Errors(DF_Results)

        del DF_Results

    def CheckED(self):
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
                B.Open_Dispute = 1
                    and
                B.Norm_Dispute_Category = 'GRT CNR'
            left join {1} As C
            on
                B.DSB_ID = C.DSB_ID
                    and
                A.Source_TBL = C.Source_TBL
                    and
                A.Source_ID = C.Source_ID
            left join {2} As D
            on
                B.DH_ID = D.DH_ID
                    and
                D.Display_Status = 'Denied - Pending'

            where
                D.DH_ID is null
        '''.format(Settings['Dispute_Fact'],Settings['Dispute_Staging_Bridge'], Settings['Dispute_History'])
        )

        DF_Results['Error_Columns'] = 'Source_TBL, Source_ID'
        DF_Results['Error_Message'] = 'There are no open Denied - Pending GRT CNR disputes for this Source_TBL & Source_ID combo'

        ASQL.execute("drop table mytbl")

        SQL.close()
        ASQL.close()

        Append_Errors(DF_Results)

        del DF_Results

    def CheckPD(self):
        ASQL = SQLConnect('alch')
        SQL = SQLConnect('sql')
        SQL.connect()
        ASQL.connect()

        ASQL.upload(self.DF, 'mytbl')

        DF_Results = SQL.query('''
            select
                A.*,
                case
                    when D.DH_ID is null then 'Source_TBL, Source_ID'
                    when isnumeric(A.amount) != 1 then 'Amount'
                    when A.amount <= 0 then 'Amount'
                    when isdate(A.Credit_Invoice_Date) != 1 then 'Credit_Invoice_Date'
                    when A.Credit_Invoice_Date != eomonth(A.Credit_Invoice_Date) then 'Credit_Invoice_Date'
                    when A.Credit_Invoice_Date < getdate() then 'Credit_Invoice_Date'
                end As Error_Columns,
                case
                    when D.DH_ID is null then 'There are no open Denied - Pending or Approved GRT CNR disputes for this Source_ID'
                    when isnumeric(A.amount) != 1 then 'Amount is not numeric'
                    when A.amount <= 0 then 'Amount is <= 0'
                    when isdate(A.Credit_Invoice_Date) != 1 then 'Credit_Invoice_Date is not a date'
                    when A.Credit_Invoice_Date != eomonth(A.Credit_Invoice_Date) then 'Credit_Invoice_Date is not the end of the month'
                    when A.Credit_Invoice_Date < getdate() then 'Credit_Invoice_Date is in the past'
                end As Error_Message

            from mytbl As A
            left join {0} As B
            on
                B.Open_Dispute = 1
                    and
                B.Norm_Dispute_Category = 'GRT CNR'
            left join {1} As C
            on
                B.DSB_ID = C.DSB_ID
                    and
                A.Source_TBL = C.Source_TBL
                    and
                A.Source_ID = C.Source_ID
            left join {2} As D
            on
                B.DH_ID = D.DH_ID
                    and
                D.Display_Status in ('Denied - Pending', 'Approved', 'Partial Approved')

            where
                D.DH_ID is null
                    or
                isnumeric(A.amount) != 1
                    or
                A.amount <= 0
                    or
                isdate(A.Credit_Invoice_Date) != 1
                    or
                A.Credit_Invoice_Date != eomonth(A.Credit_Invoice_Date)
                    or
                A.Credit_Invoice_Date < getdate()
        '''.format(Settings['Dispute_Fact'],Settings['Dispute_Staging_Bridge'],Settings['Dispute_History'])
        )

        ASQL.execute("drop table mytbl")

        SQL.close()
        ASQL.close()

        Append_Errors(DF_Results)

        del DF_Results

    def Process(self):

        if self.action == 'Map':
            self.Map('LL', 'ORD_WTN','BMI')
            self.Map('LL', 'ORD_WTN', 'PCI')
            self.Map('BRD', 'ORD_BRD_ID','BMI')
            self.Map('BRD', 'ORD_BRD_ID', 'PCI')
            self.Map('DED', 'CUS_DED_ID','BMI')
            self.Map('DED', 'CUS_DED_ID', 'PCI')
            self.Map('LD', 'ORD_WTN','BMI')
            self.Map('LD', 'ORD_WTN', 'PCI')
            self.Map('TF', 'ORD_POTS_ANI_BIL','BMI')
            self.Map('TF', 'ORD_POTS_ANI_BIL', 'PCI')
        elif self.action == 'Dispute':
            self.Dispute('BMI', 'BMI_ID')
            self.Dispute('PCI', 'ID')
        elif self.action == 'Send to Prov':
            self.CheckProv()
        elif self.action == 'Send to LV':
            self.CheckLV()
        elif self.action == 'Dispute Note' or self.action == 'Close Disputes':
            self.CheckDN()
        elif self.action == 'Escalate Disputes':
            self.CheckED()
        elif self.action == 'Paid Disputes':
            self.CheckPD()
