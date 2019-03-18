from Vacuum_Global import settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import getbatch
from Vacuum_Global import validatecol
from Vacuum_Global import processresults
from Vacuum_Global import writelog
from Vacuum_Global import defaultheader

import pandas as pd


class Seeds:
    args = dict()

    def __init__(self, folder_name, asql=None, df=pd.DataFrame()):
        self.folder_name = folder_name
        self.df = df
        self.setdefaults()
        self.asql = asql

    def setdefaults(self):
        self.args['DSB_Cols'] = '''Vendor, Platform, BAN, STC_Claim_Number, Bill_Date, USI, Dispute_Amount
            , BanMaster_ID, Source_TBL, Source_ID'''

        self.args['DC_Cols'] = '''DSB_ID, Dispute_Type, Vendor, Platform, State, BAN, Bill_Date, USI, Source_TBL
            , Source_ID, STC_Claim_Number, #_Of_Escalations, Status, Display_Status, Date_Submitted
            , Norm_Dispute_Category, Dispute_Category, Audit_Type, Disputer, Comment, Dispute_Amount, Dispute_Reason
            , Date_Updated, Batch, Edit_Date, Source_File, DH_ID
        '''

        if self.df.empty:
            self.args['DC_Sel'] = '''DSB.DSB_ID, DS.Dispute_Type, A.Vendor, A.Platform, A.State, A.BAN, A.Bill_Date
            , A.USI, A.Source_TBL, A.Source_ID, DSB.STC_Claim_Number, 0, 'Open'
            , case when DH.Display_Status is not null then DH.Display_Status else 'Pending Review' end
            , getdate(), 'GRT CNR', 'GRT CNR', 'CNR Audit', DS.Rep, DS.Comment, A.Dispute_Amt, DS.Dispute_Reason
            , getdate(), DS.Batch_DT, getdate(), DH.Source_File, DH.DH_ID
                    '''
            self.args['email'] = "A.Source_TBL = 'PCI'"
            self.args['DSB_Sel'] = '''case when A.Vendor is not null then A.Vendor else BM.BDT_Vendor end
                , case when A.Platform is not null then A.Platform else PM.Platform end, A.BAN
                , '{0}_' + left(A.Record_Type,1) + cast(A.Seed as varchar), A.Bill_Date, A.USI, A.Dispute_Amt
                , case when A.BanMaster_ID is not null then A.BanMaster_ID else BM.ID end, A.Source_TBL, A.Source_ID
                '''.format(getbatch())
            self.args['DSB_On'] = '''
                        DSB.Stc_Claim_Number='{0}_' + left(A.Record_Type,1) + cast(A.Seed as varchar)
                        '''.format(getbatch())
            self.args['DS_Cols'] = '''DSB_ID, Rep, Dispute_Type, Cost_Type, Cost_Type_Seed, State, USOC, USOC_Desc
                , CPID,  Record_Type, Dispute_Category, Audit_Type, STC_Claim_Number, Bill_Date, Billed_Amt
                , Claimed_Amt, Dispute_Reason, Billed_Phrase_Code, Causing_SO, PON, Comment, Confidence, Batch_DT
                ,Edit_DT'''
            self.args['DS_Sel'] = '''DSB.DSB_ID, B.Full_Name, A.Claim_Channel, A.Cost_Type, A.Seed, A.State, A.USOC
                , A.USOC_Desc, CPID, A.Record_Type, 'GRT CNR', 'CNR Audit'
                , '{0}_' + left(A.Record_Type,1) + cast(A.Seed as varchar), A.Bill_Date, A.Billed_Amt, A.Dispute_Amt
                , A.Action_Reason, A.Phrase_Code, A.Causing_SO, A.PON, A.Action_Comment, A.Confidence, '{1}', getdate()
                '''.format(getbatch(), getbatch(True))
            self.args['DH_Cols'] = '''DSB_ID, Dispute_Category, Display_Status, Date_Submitted, Dispute_Reason
                , GRT_Update_Rep, Date_Updated, Source_File, Edit_DT'''
            self.args['DH_Sel'] = '''DSB.DSB_ID, 'GRT CNR', 'Filed', getdate(), A.Action_Reason, B.Full_Name
                , getdate(), 'GRT Status: ' + format(getdate(),'yyyyMMdd'), getdate()'''
            self.args['DH_Whr'] = "A.Claim_Channel = 'Email'"
            self.args['email2'] = "A.Claim_Channel = 'Email'"
        else:
            self.args['DC_Sel'] = '''DSB.DSB_ID, DS.Dispute_Type, A.Vendor, A.Platform, A.State, A.BAN, A.Bill_Date
                , A.USI, A.Source_TBL, A.Source_ID, DSB.STC_Claim_Number, 0, 'Open'
                , case when DH.Display_Status is not null then DH.Display_Status else 'Pending Review' end
                , getdate(), A.Dispute_Category, A.Dispute_Category, A.Audit_Type, DS.Rep, DS.Comment, A.Dispute_Amt
                , DS.Dispute_Reason, getdate(), DS.Batch_DT, getdate(), DH.Source_File, DH.DH_ID
                    '''
            self.args['email'] = "A.Dispute_Status = case when A.Dispute_Status is not null then A.Dispute_Status " \
                                 "else 'Filed' end"
            self.args['email2'] = "A.Dispute_Type = 'Email'"
            self.args['DSB_Sel'] = '''case when A.Vendor is not null then A.Vendor else BM.BDT_Vendor end
                , case when A.Platform is not null then A.Platform else PM.Platform end, A.BAN
                , '{0}_' + left(A.Record_Type,1) + cast(A.Cost_Type_Seed as varchar), A.Bill_Date, A.USI, A.Dispute_Amt
                , case when A.BanMaster_ID is not null then A.BanMaster_ID else BM.ID end, A.Source_TBL, A.Source_ID
                '''.format(getbatch())
            self.args['DSB_On'] = '''
                DSB.Stc_Claim_Number='{0}_' + left(A.Record_Type,1) + cast(A.Cost_Type_Seed as varchar)
                '''.format(getbatch())
            self.args['DS_Cols'] = '''DSB_ID, Rep, Dispute_Type, Cost_Type, Cost_Type_Seed, State, USOC, USOC_Desc
            , CPID, Record_Type, Dispute_Category, Audit_Type, STC_Claim_Number, Bill_Date, Billed_Amt
            , Claimed_Amt, Dispute_Reason, Billed_Phrase_Code, Causing_SO, PON, CLLI, Usage_Rate, MOU, Jurisdiction
            , Short_Paid, Comment, Confidence, Batch_DT, Edit_DT'''
            self.args['DS_Sel'] = '''DSB.DSB_ID, B.Full_Name, A.Dispute_Type, A.Cost_Type, A.Cost_Type_Seed, A.State
            , A.USOC, A.USOC_Desc, A.CPID, A.Record_Type, A.Dispute_Category, A.Audit_Type
            , '{0}_' + left(A.Record_Type,1) + cast(A.Cost_Type_Seed as varchar), A.Bill_Date, A.Billed_Amt
            , A.Dispute_Amt, A.Dispute_Reason, A.Phrase_Code, A.Causing_SO, A.PON, A.CLLI, A.Usage_Rate, A.MOU
            , A.Jurisdiction, A.Short_Paid, A.Comment, A.Confidence, '{1}', getdate()
            '''.format(getbatch(), getbatch(True))
            self.args['DH_Cols'] = '''DSB_ID, Dispute_Category, Display_Status, Date_Submitted, Dispute_Reason
                , ILEC_Confirmation, ILEC_Comments, Credit_Approved, Credit_Received_Amount
                , Credit_Received_Invoice_Date, GRT_Update_Rep, Date_Updated, Source_File, Edit_DT'''
            self.args['DH_Sel'] = '''DSB.DSB_ID, A.Dispute_Category, A.Dispute_Status, getdate()
                , A.Dispute_Reason, A.ILEC_Confirmation, A.ILEC_Comment, A.Approved_Amt, A.Received_Amt
                , A.Received_Invoice_Date, B.Full_Name, getdate(), 'GRT Status: ' + format(getdate(),'yyyyMMdd')
                , getdate()'''
            self.args['DH_Whr'] = "A.Dispute_Type = 'Email' and A.Dispute_Status is null"

            self.df = defaultheader(self.df, '''dispute_type, cost_type, cost_type_seed, dispute_category, audit_type
                , confidence, dispute_reason, record_type, usi, dispute_amt, usoc, usoc_desc, pon, phrase_code
                , causing_so, clli, usage_rate, mou, jurisdiction, short_paid, comment, dispute_status
                , ilec_confirmation, ilec_comment, approved_amt, received_amt, received_invoice_date, vendor, platform
                , ban, bill_date, state, billed_amt, source_tbl, source_id, cpid, banmaster_id, Error_Columns
                , Error_Message''')

    def appenddisputes(self):
        if self.asql.query("select object_id('myseeds')").iloc[0, 0]\
                and self.asql.query("select count(*) from myseeds").iloc[0, 0] > 0:
            if self.asql.query("select object_id('DS')").iloc[0, 0]:
                self.asql.execute("DROP TABLE DS")

            if self.asql.query("select object_id('DSB')").iloc[0, 0]:
                self.asql.execute("DROP TABLE DSB")

            if self.asql.query("select object_id('DH')").iloc[0, 0]:
                self.asql.execute("DROP TABLE DH")

            self.asql.execute("CREATE TABLE DSB (DSB_ID int, Stc_Claim_Number varchar(255), BanMaster_ID int)")
            self.asql.execute("CREATE TABLE DS (DS_ID int, DSB_ID int, Dispute_Type varchar(10)"
                              ", Rep varchar(100), Comment varchar(max), Dispute_Reason varchar(max), Batch_DT date)")
            self.asql.execute("CREATE TABLE DH (DH_ID int, DSB_ID int, Display_Status varchar(100)"
                              ", Source_File varchar(255))")

            self.asql.execute('''
                update A
                    set {0}

                from myseeds As A

                where
                    A.Error_Columns is null
                        and
                    {1}
            '''.format(self.args['email2'], self.args['email']))

            self.asql.execute('''
                insert into {0}
                (
                    {1}
                )

                OUTPUT
                    INSERTED.DSB_ID,
                    INSERTED.Stc_Claim_Number,
                    INSERTED.BanMaster_ID

                INTO DSB

                select
                    {2}

                from myseeds A
                left join {3} BM
                on
                    A.BAN = BM.BAN
                        and
                    eomonth(A.Bill_Date) between eomonth(BM.Start_Date) and eomonth(isnull(BM.End_Date, getdate()))
                left join {4} PM
                on
                    BM.PlatformMasterID = PM.ID
                
                where
                    Error_Columns is null;'''.format(settings['Dispute_Staging_Bridge'], self.args['DSB_Cols']
                                                     , self.args['DSB_Sel'], settings['Ban_Master']
                                                     , settings['Platform_Master']))

            self.asql.execute('''
                insert into {0}
                (
                    {2}
                )

                OUTPUT
                    INSERTED.DS_ID,
                    INSERTED.DSB_ID,
                    INSERTED.Dispute_Type,
                    INSERTED.Rep,
                    INSERTED.Comment,
                    INSERTED.Dispute_Reason,
                    INSERTED.Batch_DT

                INTO DS

                select
                    {3}

                from myseeds As A
                inner join {1} As B
                on
                    A.Comp_Serial = B.Comp_Serial
                inner join DSB
                on
                    {4}
                    
                where
                    A.Error_Columns is null
            '''.format(settings['DisputeStaging'], settings['CAT_Emp'], self.args['DS_Cols']
                       , self.args['DS_Sel'], self.args['DSB_On']))

            self.asql.execute('''
                insert into {0}
                (
                    {2}
                )

                OUTPUT
                    INSERTED.DH_ID,
                    INSERTED.DSB_ID,
                    INSERTED.Display_Status,
                    INSERTED.Source_File

                INTO DH

                select
                    {3}

                from myseeds As A
                inner join {1} As B
                on
                    A.Comp_Serial = B.Comp_Serial
                inner join DSB
                on
                    {4}

                where
                    A.Error_Columns is null
                        and
                    {5}
            '''.format(settings['Dispute_History'], settings['CAT_Emp'], self.args['DH_Cols']
                       , self.args['DH_Sel'], self.args['DSB_On'], self.args['DH_Whr']))

            self.asql.execute('''
                insert into {0}
                (
                    {1},
                    Norm_Vendor,
                    Norm_Platform
                )
                select
                    {2},
                    VM.Vendor,
                    PM.Platform

                from DSB
                inner join DS
                on
                    DSB.DSB_ID = DS.DSB_ID
                inner join myseeds A
                on
                    {3}
                left join DH
                on
                    DSB.DSB_ID = DH.DSB_ID
                left join {4} BM
                on
                    DSB.BanMaster_ID = BM.ID
                left join {5} PM
                on
                    BM.PlatformMasterID = PM.ID
                left join {6} VM
                on
                    BM.VendorMasterID = VM.ID
            '''.format(settings['Dispute_Current'], self.args['DC_Cols'], self.args['DC_Sel'], self.args['DSB_On']
                       , settings['Ban_Master'], settings['Platform_Master'], settings['Vendor_Master']))

            self.asql.execute('drop table DS, DSB, DH')

    def grabseedinfo(self, table, seed, cost_type, params, params2=''):
        if self.df.empty:
            myseed = 'Seed'
        else:
            myseed = 'Cost_Type_Seed'

        self.asql.execute('''
            update A
                set
                    A.Error_Columns = iif(B.{1} is null,'Cost_Type, Cost_Type_Seed',NULL),
                    A.Error_Message = iif(B.{1} is null,'Cost_Type_Seed does not exist',NULL),
                    {3}

            from myseeds as A
            left join {0} as B
            on
                A.{5} = B.{1}
            {4}

            where
                A.Error_Columns is null
                    and
                A.Cost_Type = '{2}'
        '''.format(table, seed, cost_type, params, params2, myseed))

    def dispute(self):
        if not self.asql:
            writelog("Processing {0} New Seed Disputes".format(len(self.df)), 'info')

            self.asql = SQLConnect('alch')
            self.asql.connect()
            self.asql.upload(self.df, 'myseeds')

            validatecol(self.asql, 'myseeds', 'Dispute_Amt')
            validatecol(self.asql, 'myseeds', 'Approved_Amt')
            validatecol(self.asql, 'myseeds', 'Received_Amt')
            validatecol(self.asql, 'myseeds', 'Received_Invoice_Date', True)

        for cost_type in settings['Seed-Cost_Type'].split(', '):
            if 'PC-' in cost_type:
                self.grabseedinfo(settings['PaperCost'], 'seed', cost_type, '''
                    A.Record_Type='{0}',
                    A.Vendor=B.Vendor,
                    A.BAN=B.BAN,
                    A.Bill_Date=B.Bill_Date,
                    A.State=B.State,
                    A.Billed_Amt=B.{0},
                    A.dispute_amt=isnull(A.dispute_amt,B.{0}),
                    A.USI=CASE
                        when isnull(A.USI,'') != '' then A.USI
                        WHEN isnull(B.WTN,'') != '' THEN B.WTN
                        WHEN isnull(B.Circuit_ID,'') != '' THEN B.Circuit_ID
                        WHEN isnull(B.BTN,'') != '' THEN B.BTN
                    END,
                    A.Source_TBL=isnull(A.Source_TBL, 'PCI'),
                    A.Source_ID=isnull(A.Source_ID, B.PCI_ID)
                '''.format(cost_type.split('-')[1]))
            elif 'MRC' == cost_type:
                self.grabseedinfo(settings[cost_type], 'bdt_{0}_id'.format(cost_type), cost_type, '''
                    A.Record_Type='{0}',
                    A.Vendor=B.Vendor,
                    A.Platform=B.Platform,
                    A.BAN=B.BAN,
                    A.Bill_Date=B.Bill_Date,
                    A.State=B.State,
                    A.USOC=case
                        when A.USOC is not null then A.USOC
                        else B.USOC
                    end,
                    A.USOC_Desc=case
                        when A.USOC_Desc is not null then A.USOC_Desc
                        else B.USOC_Description
                    end,
                    A.Billed_Amt=B.Amount,
                    A.dispute_amt=isnull(A.dispute_amt,B.Amount),
                    A.Source_TBL=isnull(A.Source_TBL, 'BMI'),
                    A.Source_ID=isnull(A.Source_ID, B.BMI_ID),
                    A.USI=CASE
                        when isnull(A.USI,'') != '' then A.USI
                        WHEN isnull(B.WTN,'') != '' THEN B.WTN
                        WHEN isnull(B.Circuit_ID,'') != '' THEN B.Circuit_ID
                        WHEN isnull(B.BTN,'') != '' THEN B.BTN
                    END,
                    A.CPID = C.CPID,
                    A.BANMaster_ID = C.BANMasterID
                    '''.format(cost_type), '''
                    left join {0} As C
                    on
                        B.BDT_MRC_ID = C.BDT_MRC_ID
                            and
                        B.Invoice_Date = C.Invoice_Date
                      '''.format(settings['MRC_CMP']))
            elif 'OCC' == cost_type:
                self.grabseedinfo(settings[cost_type], 'bdt_{0}_id'.format(cost_type), cost_type, '''
                    A.Record_Type=upper(B.Activity_Type),
                    A.Vendor=B.Vendor,
                    A.Platform=B.Platform,
                    A.BAN=B.BAN,
                    A.Bill_Date=B.Bill_Date,
                    A.State=B.State,
                    A.USOC=case
                        when A.USOC is not null then A.USOC
                        else B.USOC
                    end,
                    A.USI=case
                        when isnull(A.USI,'') != '' then A.USI
                        WHEN isnull(B.BTN,'') != '' THEN B.BTN
                        WHEN isnull(B.Circuit_ID,'') != '' THEN B.Circuit_ID
                    END,
                    A.USOC_Desc=case
                        when A.USOC_Desc is not null then A.USOC_Desc
                        else B.USOC_Description
                    end,
                    A.Billed_Amt=B.Amount,
                    A.dispute_amt=isnull(A.dispute_amt, B.Amount),
                    A.Phrase_Code=case
                        when A.Phrase_Code is not null then A.Phrase_Code
                        else B.Phrase_Code
                    end,
                    A.Causing_SO=case
                        when A.Causing_SO is not null then A.Causing_SO
                        else B.SO
                    end,
                    A.PON=case
                        when A.PON is not null then A.PON
                        else B.PON
                    end
                ''')
            elif 'TAS' == cost_type:
                self.grabseedinfo(settings[cost_type], 'bdt_{0}_id'.format(cost_type), cost_type, '''
                    A.Record_Type='{0}',
                    A.Vendor=B.Vendor,
                    A.Platform=B.Platform,
                    A.BAN=B.BAN,
                    A.Bill_Date=B.Bill_Date,
                    A.State=B.State,
                    A.Billed_Amt=B.Total_Amount,
                    A.dispute_amt=isnull(A.dispute_amt, B.Total_Amount),
                    A.Jurisdiction=case
                        when A.Jurisdiction is not null then A.Jurisdiction
                        else B.Jurisdiction_Phrase
                    end
                '''.format(cost_type))
            elif 'USAGE' == cost_type:
                self.grabseedinfo(settings[cost_type], 'bdt_{0}_id'.format(cost_type), cost_type, '''
                    A.Record_Type='{0}',
                    A.Vendor=B.Vendor,
                    A.Platform=B.Platform,
                    A.BAN=B.BAN,
                    A.Bill_Date=B.Bill_Date,
                    A.State=B.State,
                    A.Billed_Amt=B.Amount,
                    A.dispute_amt=isnull(A.dispute_amt,B.Amount),
                    A.USI=case when isnull(A.USI,'') != '' then A.USI else B.BTN end,
                    A.Usage_Rate=case
                        when A.Usage_Rate is not null then A.Usage_Rate
                        else B.Usage_Rate
                    end,
                    A.Jurisdiction=case
                        when A.Jurisdiction is not null then A.Jurisdiction
                        else B.Jurisdiction
                    end
                '''.format(cost_type))
            elif 'LPC' == cost_type:
                self.grabseedinfo(settings[cost_type], 'bdt_invoice_id', cost_type, '''
                    A.Record_Type='{0}',
                    A.Vendor=B.Vendor,
                    A.Platform=B.Platform,
                    A.BAN=B.BAN,
                    A.Bill_Date=B.Bill_Date,
                    A.State=B.State,
                    A.Billed_Amt=B.Late_Payment_Charges,
                    A.dispute_amt=isnull(A.dispute_amt,B.Late_Payment_Charges)
                '''.format(cost_type))
            elif 'ADJ' == cost_type:
                self.grabseedinfo(settings[cost_type], 'bdt_pad_id', cost_type, '''
                    A.Record_Type='{0}',
                    A.Vendor=B.Vendor,
                    A.Platform=B.Platform,
                    A.BAN=B.BAN,
                    A.Bill_Date=B.Bill_Date,
                    A.State=B.State,
                    A.Billed_Amt=B.Amount,
                    A.dispute_amt=isnull(A.dispute_amt,B.Amount),
                    A.Phrase_Code=case
                        when A.Phrase_Code is not null then A.Phrase_Code
                        else B.Phrase_Code
                    end
                '''.format(cost_type))

        self.appenddisputes()

        if not self.df.empty:
            processresults(self.folder_name, self.asql, 'myseeds', 'New Seed Disputes')
            self.asql.close()

