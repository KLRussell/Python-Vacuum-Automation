from Vacuum_Global import settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import writelog
from Vacuum_Global import getbatch
from Vacuum_Global import processresults
from Vacuum_Global import validatecol
from Vacuum_Seeds import Seeds
from Vacuum_Global import defaultheader

import os
import time
import pathlib as pl


class BMIPCI:
    asql = None

    def __init__(self, action, df, folder_name):
        self.action = action
        self.df = df
        self.folder_name = folder_name
        self.df = defaultheader(self.df, '''action, gs_srvtype, gs_srvid, action_comment, action_norm_reason
            , action_reason, usi, start_date, claim_channel, confidence, pon, macnum, audit_group, prov_category
            , prov_recommendation, prov_norm_reason, prov_sub_reason, amount_or_days, credit_invoice_date, vendor
            , platform, ban, bill_date, state, billed_amt, dispute_amt, usoc, usoc_desc, cpid, banmaster_id
            , phrase_code, causing_so, error_columns, error_message''')

    def update_map(self, source_tbl, bmb_tbl, bmm_tbl, unmapped_tbl):
            param = None

            if source_tbl == 'BMI':
                param = ", C.frameID=B.initials"

            self.asql.execute('''
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
                '''.format(source_tbl, bmb_tbl, settings['CAT_Emp']))

            self.asql.execute('''
                update C
                    set
                        C.gs_SrvID=iif(A.BMB=1,A.Source_ID,A.gs_SrvID),
                        C.gs_SrvType=iif(A.BMB=1,iif(Source_TBL='PCI','M2M','BMB'),A.gs_SrvType),
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
                '''.format(source_tbl, bmm_tbl, settings['CAT_Emp'], param))

            self.asql.execute('''
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
            '''.format(source_tbl, unmapped_tbl, settings['CAT_Emp']))

    def grab_seeds(self, data, tbl, seed, on, where, cost_type, record_type, inner=""):
        cols = "A." + ", A.".join(data.loc[:, ~data.columns.isin(['Action', 'Start_Date'])].columns.values)

        myquery = '''
            select
                {3} As Seed,
                '{5}' As Cost_Type,
                {6} As Record_Type,
                {4},
                NULL As Error_Columns,
                NULL As Error_Message

            from mytbl As A
            {7}
            inner join {0} As B
            on
                {1}
                    and
                {2}
        '''.format(tbl, on, where, seed, cols, cost_type, record_type, inner)

        if not self.asql.query("select object_id('mytbl2')").iloc[0, 0]:
            myresults = self.asql.query(myquery)

            if not myresults.empty:
                self.asql.upload(myresults, 'myseeds')
        else:
            cols2 = ",".join(data.loc[:, ~data.columns.isin(['Action', 'Start_Date'])].columns.values)

            self.asql.execute('''
                insert into myseeds
                (
                    Seed,
                    Cost_Type,
                    Record_Type,
                    {0},
                    Error_Columns,
                    Error_Message
                )
            '''.format(cols2) + myquery)

    def updatezerorev(self, source, audit_result, comment, dispute=False, action=None):
        if comment and action:
            mycomment = "concat('{1} - ', A.{0})".format(comment, action)
        elif comment:
            mycomment = 'A.{0}'.format(comment)
        elif comment:
            mycomment = "'{0}'".format(action)
        else:
            mycomment = "NULL"

        if source == 'BMI':
            zerorevtbl = settings['ZeroRevenue']
        else:
            zerorevtbl = settings['PCI_ZeroRevenue']

        if dispute:
            self.asql.upload(self.asql.query('''
                select distinct
                    Source_ID,
                    Action_Comment

                from mytbl2

                where
                    Source_TBL = '{0}'
            '''.format(source)), 'mydata')
        else:
            self.asql.upload(self.asql.query('''
                select
                    *
                from mytbl

                where
                    Error_Columns is null
                        and
                    Source_TBL = '{0}'
            '''.format(source)), 'mydata')

        if self.asql.query("select object_id('mydata')").iloc[0, 0]:
            self.asql.execute('''
                INSERT INTO {0}
                (
                    {1}_ID,
                    Invoice_Date,
                    Tag,
                    Audit_Result,
                    Rep,
                    Comment,
                    Edit_Date
                )
                select
                    A.Source_ID,
                    B.Max_CNR_Date,
                    B.Tag,
                    '{4}',
                    C.Initials,
                    A.{5},
                    getdate()

                from mydata As A
                inner join {2} As B
                on
                    B.Source_TBL = '{1}'
                        and
                    A.Source_ID = B.Source_ID
                inner join {3} As C
                on
                    A.Comp_Serial = C.Comp_Serial
            '''.format(zerorevtbl, source, settings['CNR'], settings['CAT_Emp'], audit_result, mycomment))

            self.asql.execute('DROP TABLE mydata')

    def findcsrs(self):
        self.df['Fuzzy_File'] = self.df['Source_TBL'] + '_' + self.df['Source_ID']
        self.df['CSR_File_Name'] = None

        for index, row in self.df.iterrows():
            csr = [None, None]
            files = list(pl.Path(settings['LV_CSR_Dir']).glob('{}*.*'.format(row['Fuzzy_File'])))

            for file in files:
                modified = time.ctime(os.path.getmtime(file))

                if csr[0] is None or csr[0] < modified:
                    csr[0] = modified
                    csr[1] = file

            if csr[1]:
                row['CSR_File_Name'] = os.path.basename(csr[1])

        del self.df['Fuzzy_File']

    def map(self, gs_srvtype, col, source_tbl):
        data = self.df.loc[(self.df['Gs_SrvType'] == gs_srvtype) & (self.df['Source_TBL'] == source_tbl)]

        if not data.empty:
            writelog("Validating {0} maps(s) for {1}".format(len(data.index), source_tbl), 'info')

            self.asql.upload(data, 'mytbl')

            self.asql.upload(self.asql.query('''
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
                from MYTMP2'''.format(",".join(data.loc[:, data.columns != 'Gs_SrvID'].columns.values))), 'mytbl2')

            self.asql.execute("drop table mytbl;")

            self.asql.execute('''
                update A
                    set
                        A.Error_Columns = 'Gs_SrvType, Gs_SrvID',
                        A.Error_Message = Gs_SrvType + ' ' + cast(Gs_SrvID as varchar) + ' is not found in {0}'

                from mytbl2 As A
                left join {0} As B
                on
                    A.Gs_SrvID = B.{1}

                where
                    B.{1} is null'''.format(settings[gs_srvtype], col))

            self.asql.execute('''
                update A
                    A.Error_Columns = 'Gs_SrvType, Gs_SrvID',
                    A.Error_Message = 'BMB table is already mapped to ' + Gs_SrvType + ' ' + cast(Gs_SrvID as varchar)

                from mytbl2 As A
                inner join {0} As B
                on
                    A.Source_ID = B.BMI_ID
                        and
                    A.Gs_SrvType = B.Gs_SrvType
                        and
                    A.Gs_SrvID = B.Gs_SrvID

                where
                    A.BMB = 1'''.format(settings['BMB']))

            self.asql.execute('''
                update A
                    A.Error_Columns = 'Gs_SrvType, Gs_SrvID',
                    A.Error_Message = 'BMB table is already mapped to ' + Gs_SrvType + ' ' + cast(Gs_SrvID as varchar)

                from mytbl2 As A
                inner join {0} As B
                on
                    A.Source_ID = B.PCI_ID
                        and
                    A.Gs_SrvType = B.Gs_SrvType
                        and
                    A.Gs_SrvID = B.Gs_SrvID

                where
                    A.BMB = 1'''.format(settings['PCI_BMB']))

            if source_tbl == 'BMI':
                self.update_map(source_tbl, settings['BMB'], settings['BMM'], settings['Unmapped'])
            else:
                self.update_map(source_tbl, settings['PCI_BMB'], settings['PCI'], settings['PCI_Unmapped'])

            processresults(self.folder_name, self.asql, 'mytbl2', self.action)

        del data

    def dispute(self, source, source_col):
        data = self.df.loc[self.df['Source_TBL'] == source]

        if not data.empty:
            writelog("Validating {0} dispute(s) for {1}".format(len(data.index), source), 'info')

            data['Start_Date'] = data['Start_Date'].astype('datetime64[D]')

            self.asql.upload(data, 'mytbl')

            self.asql.execute('''
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
                '''.format(source_col, settings[source], settings['Limitations']))

            self.asql.execute('''
                update A
                    set A.Start_Date = eomonth(dateadd(month, -1, A.Start_Date))
                from mytbl As A

                where
                    A.Start_Date is not null''')

            if self.asql.query("select object_id('myseeds')").iloc[0, 0]:
                self.asql.execute("drop table myseeds")

            if source == 'BMI':
                writelog("Grabbing MRC & OCC Cost", 'info')

                self.grab_seeds(
                    data,
                    settings['MRC'],
                    'BDT_MRC_ID',
                    'B.Invoice_Date > A.Start_Date and A.Source_ID = B.BMI_ID',
                    'Amount > 0',
                    'MRC',
                    "'MRC'"
                )

                self.grab_seeds(
                    data,
                    settings['OCC'],
                    'BDT_OCC_ID',
                    '''B.Invoice_Date > A.Start_Date and B.Vendor = C.Vendor
                        and B.BAN = C.BAN and B.BTN = C.WTN and B.Circuit_ID = C.Circuit_ID''',
                    'Amount > 0',
                    'OCC',
                    "upper(Activity_Type)",
                    "inner join {0} As C on A.Source_ID = C.BMI_ID".format(settings['BMI'])
                )
            else:
                writelog("Grabbing PaperCost MRC, NRC, and FRAC Cost", 'info')

                self.grab_seeds(
                    data,
                    settings['PaperCost'],
                    'Seed',
                    'B.Bill_Date > A.Start_Date and A.Source_ID = B.PCI_ID',
                    'MRC > 0',
                    'PC-MRC',
                    "'MRC'"
                )

                self.grab_seeds(
                    data,
                    settings['PaperCost'],
                    'Seed',
                    'B.Bill_Date > A.Start_Date and A.Source_ID = B.PCI_ID',
                    'NRC > 0',
                    'PC-NRC',
                    "'NRC'"
                )

                self.grab_seeds(
                    data,
                    settings['PaperCost'],
                    'Seed',
                    'B.Bill_Date > A.Start_Date and A.Source_ID = B.PCI_ID',
                    'FRAC > 0',
                    'PC-FRAC',
                    "'FRAC'"
                )

            if self.asql.query("select object_id('myseeds')").iloc[0, 0]:
                self.updatezerorev('BMI', 'Dispute Review', 'Action_Comment', True, 'New Dispute')
                self.updatezerorev('PCI', 'Dispute Review', 'Action_Comment', True, 'New Dispute')

                self.asql.execute('''
                    with
                        MY_TMP
                    As
                    (
                        select distinct
                            Source_TBL,
                            Source_ID

                        from myseeds
                    )

                    update A
                    set
                        Error_Columns = 'Source_TBL, Source_ID, Start_Date',
                        Error_Message = 'Valid cost was not found for the Source_TBL, Source_ID, Start_Date combo'

                    from mytbl As A
                    left join MY_TMP As B
                    on
                        A.Source_TBL = B.Source_TBL
                            and
                        A.Source_ID = B.Source_ID

                    where
                        B.Source_TBL is null''')

                myobj = Seeds(self.folder_name, self.asql)
                myobj.dispute()
            else:
                writelog("Warning! No cost found for {} of spreadsheet".format(source), 'info')

                self.asql.execute('''
                    update A
                    set
                        Error_Columns = 'Source_TBL, Source_ID, Start_Date',
                        Error_Message = 'Valid cost was not found for the Source_TBL, Source_ID, Start_Date combo'
                    
                    from mytbl
                    where
                        Error_Columns is null
                ''')

            processresults(self.folder_name, self.asql, 'mytbl', self.action)
        del data

    def sendtoprov(self):
        writelog("Validating {0} {1}(s)".format(len(self.df.index), self.action), 'info')

        self.asql.upload(self.df, 'mytbl')

        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'Macnum',
                    A.Error_Message = 'This Macnum does not exist in CS'
            from mytbl As A
            left join {0} As B
            on
                A.Macnum = B.Macnum

            where
                B.MACNUM is null'''.format(settings['Cust_File']))

        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'Source_TBL, Source_ID',
                    A.Error_Message = 'This Source_TBL and Source_ID is already pending in Send to Prov table'

            from mytbl As A
            inner join {0} As B
            on
                A.Source_TBL = B.Source_TBL
                    and
                A.Source_ID = B.Source_ID

            where
                B.Root_Cause is null
                    and
                B.Prov_Note is null
                    and
                B.New_Root_Cause is null'''.format(settings['Send_To_Prov']))

        self.asql.upload(self.asql.query('''
            select distinct
                DATA.*,
                CNR.Vendor,
                CNR.BAN,
                CNR.BTN,
                CNR.WTN,
                CNR.Circuit_ID,
                CNR.MRC_Amount,
                CNR.Max_CNR_Date,
                MRC.Product_Type

            from mytbl As DATA
            inner join {0} As CNR
            on
                DATA.Source_TBL = CNR.Source_TBL
                    and
                DATA.Source_ID = CNR.Source_ID
            left join {1} As MRC
            on
                DATA.Source_TBL = 'BMI'
                    and
                CNR.Max_CNR_Date = MRC.Invoice_Date
                    and
                DATA.Source_ID = MRC.BMI_ID
        '''.format(settings['CNR'], settings['MRC'])), 'mytbl2')

        self.asql.execute('''
            insert into {0}
            (
                Batch,
                Audit_Name,
                Audit_Group,
                CAT_Rep,
                Source_TBL,
                Source_ID,
                Product,
                Vendor,
                BAN,
                BTN,
                WTN,
                Circuit_ID,
                Monthly_Impact,
                Invoice_Date,
                Macnum,
                Associated_PON,
                Issue,
                Category,
                Recommended_Action,
                Normalized_Reason,
                Sub_Reason
            )
            select
                eomonth(getdate()),
                'CNR',
                A.Audit_Group,
                B.Initials,
                A.Source_TBL,
                A.Source_ID,
                A.Product_Type,
                A.Vendor,
                A.BAN,
                A.BTN,
                A.WTN,
                A.Circuit_ID,
                A.MRC_Amount,
                A.Max_CNR_Date,
                A.Macnum,
                A.PON,
                A.Action_Reason,
                A.Prov_Category,
                A.Prov_Recommendation,
                A.Prov_Norm_Reason,
                A.Prov_Sub_Reason

            from mytbl2 As A
            inner join {1} As B
            on
                A.Comp_Serial = B.Comp_Serial
            where
                A.Error_Columns is null'''.format(settings['Send_To_Prov'], settings['CAT_Emp']))

        self.updatezerorev('BMI', 'Pending Prov', 'Action_Reason', False, 'Sent to Prov')
        self.updatezerorev('PCI', 'Pending Prov', 'Action_Reason', False, 'Sent to Prov')

        processresults(self.folder_name, self.asql, 'mytbl', self.action)
        self.asql.execute("drop table mytbl2")

    def sendtolv(self):
        writelog("Validating {0} {1}(s)".format(len(self.df.index), self.action), 'info')

        self.findcsrs()
        self.asql.upload(self.df, 'mytbl')

        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'Macnum',
                    A.Error_Message = 'This Macnum does not exist in CS'

            from mytbl As A
            left join {0} As B
            on
                A.Macnum = B.Macnum

            where
                B.Macnum is null
        '''.format(settings['Cust_File']))

        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'Source_TBL, Source_ID',
                    A.Error_Message = 'This Source_TBL and Source_ID is already pending in Send to LV table'

            from mytbl As A
            inner join {0} As B
            on
                A.Source_TBL = B.Source_TBL
                    and
                A.Source_ID = B.Source_ID

            where
                B.Is_Rejected is null
                    or
                B.Sugg_Action is null
        '''.format(settings['Send_To_LV']))

        self.asql.execute('''
            insert into {0}
            (
                Source_TBL,
                Source_ID,
                Vendor,
                BAN,
                BTN,
                WTN,
                Macnum,
                Rep,
                Invoice_Date,
                Comment,
                Batch,
                CSR_File_Name
            )
            select
                A.Source_TBL,
                A.Source_ID,
                C.Vendor,
                C.BAN,
                C.BTN,
                C.WTN,
                A.Macnum,
                B.Initials,
                C.Max_CNR_Date,
                A.Action_Reason,
                '{3}' As Batch,
                A.CSR_File_Name

            from mytbl As A
            inner join {1} As B
            on
                A.Comp_Serial = B.Comp_Serial
            left join {2} As C
            on
                A.Source_TBL = C.Source_TBL
                    and
                A.Source_ID = C.Source_ID

            where
                A.Error_Columns is null
        '''.format(settings['Send_To_LV'], settings['CAT_Emp'], settings['CNR'], getbatch(True, 7, 0)))

        self.updatezerorev('BMI', 'Pending LV', 'Action_Reason', False, 'Sent to LV')
        self.updatezerorev('PCI', 'Pending LV', 'Action_Reason', False, 'Sent to LV')

        processresults(self.folder_name, self.asql, 'mytbl', self.action)

    def adddn(self):
        writelog("Validating {0} {1}(s)".format(len(self.df.index), self.action), 'info')

        self.asql.upload(self.df, 'mytbl')

        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'Source_TBL, Source_ID',
                    A.Error_Message = 'Dispute Notes were already filed for this Source_ID today'

            from mytbl As A
            inner join {0} As B
            on
                A.Source_TBL = B.Source_TBL
                    and
                A.Source_ID = B.Source_ID
            inner join {1} As C
            on
                B.DSB_ID = C.DSB_ID
                    and
                C.Open_Dispute = 1
            inner join {2} As D
            on
                C.DS_ID = D.DS_ID
                    and
                D.Dispute_Category = 'GRT CNR'
            inner join {3} As E
            on
                C.DH_ID = E.DH_ID
            inner join {4} As F
            on
                E.DH_ID = F.DH_ID
                    and
                cast(F.Edit_Date as date) = cast(getdate() as date)
            '''.format(settings['Dispute_Staging_Bridge'], settings['Dispute_Fact'], settings['DisputeStaging'],
                       settings['Dispute_History'], settings['Dispute_Notes']))

        self.asql.upload(self.asql.query('''
            select
                F.DH_ID,
                B.Full_Name,
                A.Action_Norm_Reason,
                A.Action_Reason,
                A.Amount_Or_Days,
                getdate() Edit_Date

            from mytbl A
            inner join {0} As B
            on
                A.Comp_Serial = B.Comp_Serial
            inner join {1} As C
            on
                A.Source_TBL = C.Source_TBL
                    and
                A.Source_ID = C.Source_ID
            inner join {2} As D
            on
                C.DSB_ID = D.DSB_ID
                    and
                D.Open_Dispute = 1
            inner join {3} As E
            on
                D.DS_ID = E.DS_ID
                    and
                E.Dispute_Category = 'GRT CNR'
            inner join {4} As F
            on
                D.DH_ID = F.DH_ID

            where
                A.Error_Columns is null
        '''.format(settings['CAT_Emp'], settings['Dispute_Staging_Bridge'],
                   settings['Dispute_Fact'], settings['DisputeStaging'], settings['Dispute_History'])), 'mytbl2')

        # migrate section to Vacuum_DisputeActions
        if not self.asql.query("select object_id('mytbl2')").iloc[0, 0]:
            self.asql.execute('''
                insert into {0}
                (
                    DH_ID,
                    Logged_By,
                    Norm_Note_Action,
                    Dispute_Note,
                    Days_Till_Action,
                    Edit_Date
                )
                select
                    DH_ID,
                    Full_Name,
                    Action_Norm_Reason,
                    Action_Reason,
                    Amount_Or_Days,
                    Edit_Date
    
                from mytbl
            '''.format(settings['Dispute_Notes']))

            self.asql.execute('''
                WITH
                    MYTMP
                AS
                (
                    SELECT DISTINCT
                        Source_TBL,
                        Source_ID

                    FROM mytbl2
                )

                update A
                    set
                        A.Error_Columns = 'Source_TBL, Source_ID',
                        A.Error_Message = 'There are no open STC filed GRT CNR disputes for this Source_TBL & Source_ID combo'

                FROM mytbl As A
                LEFT JOIN mytbl2 As B
                ON
                    A.Source_TBL = B.Source_TBL
                        AND
                    A.Source_ID = B.Source_ID

                WHERE
                    A.Source_TBL is null
                        and
                    A.Error_Columns is null
            ''')
            # migrate section to Vacuum_DisputeActions END

            self.updatezerorev('BMI', 'Dispute Review', 'Action_Reason', False, 'Dispute Note')
            self.updatezerorev('PCI', 'Dispute Review', 'Action_Reason', False, 'Dispute Note')

            self.asql.execute("drop table mytbl2")
        else:
            self.asql.execute('''
                update A
                    set
                        A.Error_Columns = 'Source_TBL, Source_ID',
                        A.Error_Message = 'There are no open STC filed GRT CNR disputes for this Source_TBL & Source_ID combo'

                FROM mytbl
                
                WHERE
                    Error_Columns is null
            ''')

        processresults(self.folder_name, self.asql, 'mytbl', self.action)

    def addescalate(self):
        writelog("Validating {0} {1}(s)".format(len(self.df.index), self.action), 'info')

        self.asql.upload(self.df, 'mytbl')

        self.asql.upload(self.asql.query('''
            select
                A.*,
                E.DSB_ID,
                E.Dispute_Category,
                B.Dispute_Amount,
                E.STC_Index

            from mytbl As A
            inner join {0} As B
            on
                A.Source_TBL = B.Source_TBL
                    and
                A.Source_ID = B.Source_ID
            inner join {1} As C
            on
                B.DSB_ID = C.DSB_ID
                    and
                C.Open_Dispute = 1
            inner join {2} As D
            on
                C.DS_ID = D.DS_ID
                    and
                D.Dispute_Category = 'GRT CNR'
            inner join {3} As E
            on
                C.DH_ID = E.DH_ID
            
            where
                D.Dispute_Type = 'Email'
                    or
                E.Display_Status = 'Denied - Pending'
        '''.format(settings['Dispute_Staging_Bridge'], settings['Dispute_Fact'], settings['DisputeStaging'],
                   settings['Dispute_History'])), 'mytbl2')

        # migrate section to Vacuum_DisputeActions
        if not self.asql.query("select object_id('mytbl2')").iloc[0, 0]:
            self.asql.execute("CREATE TABLE DH (DH_ID int, DSB_ID int)")

            self.asql.execute('''
                INSERT INTO {0}
                (
                    DSB_ID,
                    Dispute_Category,
                    Display_Status,
                    Date_Submitted,
                    Escalate,
                    Escalate_DT,
                    Escalate_Amount,
                    Dispute_Reason,
                    STC_Index,
                    GRT_Update_Rep,
                    Date_Updated,
                    Source_File
                )
                
                OUTPUT
                    INSERTED.DH_ID,
                    INSERTED.DSB_ID
                
                INTO DH
                
                SELECT
                    A.DSB_ID,
                    A.Dispute_Category,
                    'GRT Escalate',
                    cast(getdate() as date),
                    1,
                    cast(getdate() as date),
                    A.Dispute_Amount,
                    A.Action_Reason,
                    A.STC_Index,
                    B.Full_Name,
                    getdate(),
                    'GRT Status: {2}'
                
                FROM mytbl2 As A
                INNER JOIN {1} As B
                ON
                    A.Comp_Serial = B.Comp_Serial
            '''.format(settings['Dispute_History'], settings['CAT_Emp'], getbatch()))

            self.asql.execute('''
                UPDATE A
                SET
                    A.DH_ID = B.DH_ID
                
                FROM {0} As A
                INNER JOIN DH As B
                ON
                    A.DSB_ID = B.DSB_ID
            '''.format(settings['Dispute_Fact']))

            self.asql.execute('''
                WITH
                    MYTMP
                AS
                (
                    SELECT DISTINCT
                        Source_TBL,
                        Source_ID
                        
                    FROM mytbl2
                )
                
                update A
                    set
                        A.Error_Columns = 'Source_TBL, Source_ID',
                        A.Error_Message = 
                        'There are no open Denied - Pending GRT CNR disputes for this Source_TBL & Source_ID combo'
                
                FROM mytbl As A
                LEFT JOIN mytbl2 As B
                ON
                    A.Source_TBL = B.Source_TBL
                        AND
                    A.Source_ID = B.Source_ID
                
                WHERE
                    A.Source_TBL is null
                        and
                    A.Error_Columns is null
            ''')
            # migrate section to Vacuum_DisputeActions END

            self.updatezerorev('BMI', 'Dispute Review', 'Action_Reason', False, 'GRT Escalate')
            self.updatezerorev('PCI', 'Dispute Review', 'Action_Reason', False, 'GRT Escalate')

            self.asql.execute("drop table mytbl2, DH")
        else:
            self.asql.execute('''
                update A
                    set
                        A.Error_Columns = 'Source_TBL, Source_ID',
                        A.Error_Message = 
                        'There are no open Denied - Pending GRT CNR disputes for this Source_TBL & Source_ID combo'
                
                FROM mytbl
                
                WHERE
                    Error_Columns is null
            ''')

        processresults(self.folder_name, self.asql, 'mytbl', self.action)

    def addpaid(self):
        writelog("Validating {0} {1}(s)".format(len(self.df.index), self.action), 'info')

        self.asql.upload(self.df, 'mytbl')

        validatecol(self.asql, 'mytbl', 'Amount_Or_Days')
        validatecol(self.asql, 'mytbl', 'Credit_Invoice_Date', True)

        self.asql.upload(self.asql.query('''
            select
                A.*,
                E.DSB_ID,
                E.Dispute_Category,
                E.Date_Submitted,
                E.ILEC_Confirmation,
                E.ILEC_Comments,
                E.Credit_Approved,
                E.Denied,
                E.Escalate,
                E.Escalate_DT,
                E.Escalate_Amount,
                E.Dispute_Reason,
                E.STC_Index,
                E.Resolution_Date

            from mytbl As A
            inner join {0} As B
            on
                A.Source_TBL = B.Source_TBL
                    and
                A.Source_ID = B.Source_ID
            inner join {1} As C
            on
                B.DSB_ID = C.DSB_ID
                    and
                C.Open_Dispute = 1
            inner join {2} As D
            on
                C.DS_ID = D.DS_ID
                    and
                D.Dispute_Category = 'GRT CNR'
            inner join {3} As E
            on
                C.DH_ID = E.DH_ID
                
            where
                (
                    (
                        D.Dispute_Type in ('GRT','STC')
                            and
                        E.Display_Status in ('Denied - Pending', 'Approved', 'Partial Approved')
                    )
                        or
                    D.Dispute_Type = 'Email'
                )
                    and
                A.Error_Columns is null
        '''.format(settings['Dispute_Staging_Bridge'], settings['Dispute_Fact'], settings['DisputeStaging'],
                   settings['Dispute_History'])), 'mytbl2')

        # migrate section to Vacuum_DisputeActions
        if not self.asql.query("select object_id('mytbl2')").iloc[0, 0]:
            self.asql.execute("CREATE TABLE DH (DH_ID int, DSB_ID int)")

            self.asql.execute('''
                INSERT INTO {0}
                (
                    DSB_ID,
                    Dispute_Category,
                    Display_Status,
                    Date_Submitted,
                    ILEC_Confirmation,
                    ILEC_Comments,
                    Credit_Approved,
                    Denied,
                    Credit_Received_Amount,
                    Credit_Received_Invoice_Date,
                    Escalate,
                    Escalate_DT,
                    Escalate_Amount,
                    Dispute_Reason,
                    STC_Index,
                    GRT_Update_Rep,
                    Resolution_Date,
                    Date_Updated,
                    Source_File
                )
                OUTPUT
                    INSERTED.DH_ID,
                    INSERTED.DSB_ID
                
                INTO DH
                
                SELECT
                    A.DSB_ID,
                    A.Dispute_Category,
                    'Paid',
                    cast(getdate() as date),
                    A.ILEC_Confirmation,
                    A.ILEC_Comments,
                    A.Credit_Approved,
                    A.Denied,
                    A.Amount_Or_Days,
                    A.Credit_Invoice_Date,
                    A.Escalate,
                    A.Escalate_DT,
                    A.Escalate_Amount,
                    A.Dispute_Reason,
                    A.STC_Index,
                    B.Full_Name,
                    A.Resolution_Date,
                    getdate(),
                    'GRT Status: {2}'
                    
                FROM mytbl2 As A
                INNER JOIN {1} As B
                ON
                    A.Comp_Serial = B.Comp_Serial
            '''.format(settings['Dispute_History'], settings['CAT_Emp'], getbatch()))

            self.asql.execute('''
                UPDATE A
                SET
                    A.DH_ID = B.DH_ID,
                    A.Open_Dispute = 0

                FROM {0} As A
                INNER JOIN DH As B
                ON
                    A.DSB_ID = B.DSB_ID
            '''.format(settings['Dispute_Fact']))

            self.asql.execute('''
                WITH
                    MYTMP
                AS
                (
                    SELECT DISTINCT
                        Source_TBL,
                        Source_ID

                    FROM mytbl2
                )

                UPDATE A
                    SET
                        A.Error_Columns = 'Source_TBL, Source_ID',
                        A.Error_Message = 
                            'There are no open Denied - Pending or Approved GRT CNR disputes for this Source_ID'

                FROM mytbl As A
                LEFT JOIN mytbl2 As B
                ON
                    A.Source_TBL = B.Source_TBL
                        AND
                    A.Source_ID = B.Source_ID

                WHERE
                    A.Source_TBL is null
                        and
                    A.Error_Columns is null
            ''')
            # migrate section to Vacuum_DisputeActions END

            self.updatezerorev('BMI', 'Dispute Review', None, False, 'GRT Paid')
            self.updatezerorev('PCI', 'Dispute Review', None, False, 'GRT Paid')

            self.asql.execute("drop table mytbl2, DH")
        else:
            self.asql.execute('''
                UPDATE A
                    SET
                        A.Error_Columns = 'Source_TBL, Source_ID',
                        A.Error_Message = 
                            'There are no open Denied - Pending or Approved GRT CNR disputes for this Source_ID'

                FROM mytbl As A
                
                WHERE
                    A.Error_Columns is null
            ''')

        processresults(self.folder_name, self.asql, 'mytbl', self.action)

    def sendtoaudit(self):
        self.asql.upload(self.df, 'mytbl')

        self.updatezerorev('BMI', 'Audit Review', None, False, 'Send to Audit')
        self.updatezerorev('PCI', 'Audit Review', None, False, 'Send to Audit')

        self.asql.execute("drop table mytbl")

        writelog("Completed {0} {1} action(s)"
                 .format(len(self.df.index), self.action), 'info')

    def updateother(self):
        self.asql.upload(self.df, 'mytbl')

        self.updatezerorev('BMI', self.action, 'Action_Comment')
        self.updatezerorev('PCI', self.action, 'Action_Comment')

        self.asql.execute("drop table mytbl")

        writelog("Completed {0} {1} action(s)"
                 .format(len(self.df.index), self.action), 'info')

    def process(self):
        writelog("Processing {0} {1} action(s)".format(len(self.df), self.action), 'info')

        self.asql = SQLConnect('alch')
        self.asql.connect()

        if self.action == 'Map':
            self.map('LL', 'ORD_WTN', 'BMI')
            self.map('LL', 'ORD_WTN', 'PCI')
            self.map('BRD', 'ORD_BRD_ID', 'BMI')
            self.map('BRD', 'ORD_BRD_ID', 'PCI')
            self.map('DED', 'CUS_DED_ID', 'BMI')
            self.map('DED', 'CUS_DED_ID', 'PCI')
            self.map('LD', 'ORD_WTN', 'BMI')
            self.map('LD', 'ORD_WTN', 'PCI')
            self.map('TF', 'ORD_POTS_ANI_BIL', 'BMI')
            self.map('TF', 'ORD_POTS_ANI_BIL', 'PCI')
        elif self.action == 'Dispute':
            self.dispute('BMI', 'BMI_ID')
            self.dispute('PCI', 'ID')
        elif self.action == 'Send to Prov':
            self.sendtoprov()
        elif self.action == 'Send to LV':
            self.sendtolv()
        elif self.action == 'Dispute Note' or self.action == 'Close Disputes':
            self.adddn()
        elif self.action == 'Escalate Disputes':
            self.addescalate()
        # Need to add action Closed here and use core elements from Vacuum_DisputeActions
        elif self.action == 'Paid Disputes':
            self.addpaid()
        elif self.action == 'Send to Audit':
            self.sendtoaudit()
        else:
            self.updateother()

        self.asql.close()
