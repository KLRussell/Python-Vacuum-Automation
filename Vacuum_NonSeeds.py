from Vacuum_Global import settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import getbatch
from Vacuum_Global import validatecol
from Vacuum_Global import processresults
from Vacuum_Global import writelog
from Vacuum_Global import defaultheader

import random


class NonSeeds:
    asql = None

    def __init__(self, df, folder_name):
        self.df = df
        self.folder_name = folder_name
        self.df = defaultheader(self.df, '''dispute_type, stc_claim_number, record_type, ban, bill_date, billed_amt, dispute_amt
            , dispute_category, audit_type, confidence, dispute_reason, usi, state, usoc, usoc_desc, pon, phrase_code
            , causing_so, clli, usage_rate, mou, jurisdiction, short_paid, comment, dispute_status, ilec_confirmation
            , ilec_comment, approved_amt, received_amt, received_invoice_date, Error_Columns, Error_Message''')

        self.df['Stc_Claim_Number'] = df['Stc_Claim_Number']\
            .map(lambda x: "{0}_X{1}".format(getbatch(), random.randint(10000000, 10000000000)))
    # df['stc_claim_number'] = "{0}_X{1}".format(getbatch(), random.randint(10000000, 10000000000))

    def appenddisputes(self):
        if self.asql.query("select object_id('mydisputes')").iloc[0, 0]\
                and self.asql.query("select count(*) from mydisputes").iloc[0, 0] > 0:
            if self.asql.query("select object_id('DS')").iloc[0, 0]:
                self.asql.execute("DROP TABLE DS")

            if self.asql.query("select object_id('DSB')").iloc[0, 0]:
                self.asql.execute("DROP TABLE DSB")

            if self.asql.query("select object_id('DH')").iloc[0, 0]:
                self.asql.execute("DROP TABLE DH")

            self.asql.execute("CREATE TABLE DSB (DSB_ID int, Stc_Claim_Number varchar(255), BanMaster_ID int)")
            self.asql.execute("CREATE TABLE DS (DS_ID int, DSB_ID int, Rep varchar(100), Batch_DT date)")
            self.asql.execute("CREATE TABLE DH (DH_ID int, DSB_ID int, Display_Status varchar(100)"
                              ", Source_File varchar(255))")
            self.asql.execute('''
                insert into {0}
                (
                    Vendor,
                    Platform,
                    BAN,
                    STC_Claim_Number,
                    Bill_Date,
                    USI,
                    Dispute_Amount,
                    BanMaster_ID
                )
    
                OUTPUT
                    INSERTED.DSB_ID,
                    INSERTED.Stc_Claim_Number,
                    INSERTED.BanMaster_ID
    
                INTO DSB
    
                select
                    BM.BDT_Vendor,
                    PM.Platform,
                    A.BAN,
                    A.Stc_Claim_Number,
                    A.Bill_Date,
                    A.USI,
                    A.Dispute_Amt,
                    BM.ID
    
                from mydisputes A
                left join {1} BM
                on
                    A.BAN = BM.BAN
                        and
                    eomonth(A.Bill_Date) between eomonth(BM.Start_Date) and eomonth(isnull(BM.End_Date, getdate()))
                left join {2} PM
                on
                    BM.PlatformMasterID = PM.ID
    
                where
                    Error_Columns is null;
            '''.format(settings['Dispute_Staging_Bridge'], settings['Ban_Master'], settings['Platform_Master']))

            self.asql.execute('''
                insert into {0}
                (
                    DSB_ID,
                    Rep,
                    Dispute_Type,
                    State,
                    USOC,
                    USOC_Desc,
                    Record_Type,
                    Dispute_Category,
                    Audit_Type,
                    STC_Claim_Number,
                    Bill_Date,
                    Billed_Amt,
                    Claimed_Amt,
                    Dispute_Reason,
                    Billed_Phrase_Code,
                    Causing_SO,
                    PON,
                    CLLI,
                    Usage_Rate,
                    MOU,
                    Jurisdiction,
                    Short_Paid,
                    Comment,
                    Confidence,
                    Batch_DT,
                    Edit_DT
                )
    
                OUTPUT
                    INSERTED.DS_ID,
                    INSERTED.DSB_ID,
                    INSERTED.Rep,
                    INSERTED.Batch_DT
    
                INTO DS
    
                select
                    DSB.DSB_ID,
                    B.Full_Name,
                    A.Dispute_Type,
                    A.State,
                    A.USOC,
                    A.USOC_Desc,
                    A.Record_Type,
                    A.Dispute_Category,
                    A.Audit_Type,
                    A.STC_Claim_Number,
                    A.Bill_Date,
                    A.Billed_Amt,
                    A.Dispute_Amt,
                    A.Dispute_Reason,
                    A.Phrase_Code,
                    A.Causing_SO,
                    A.PON,
                    A.CLLI,
                    A.Usage_Rate,
                    A.MOU,
                    A.Jurisdiction,
                    A.Short_Paid,
                    A.Comment,
                    A.Confidence,
                    '{2}',
                    getdate()
    
                from mydisputes As A
                inner join {1} As B
                on
                    A.Comp_Serial = B.Comp_Serial
                inner join DSB
                on
                    DSB.STC_Claim_Number = A.STC_Claim_Number
    
                where
                    A.Error_Columns is null
            '''.format(settings['DisputeStaging'], settings['CAT_Emp'], getbatch(True)))

            self.asql.execute('''
                insert into {0}
                (
                    DSB_ID,
                    Dispute_Category,
                    Display_Status,
                    Date_Submitted,
                    ILEC_Confirmation,
                    ILEC_Comments,
                    Credit_Approved,
                    Credit_Received_Amount,
                    Credit_Received_Invoice_Date,
                    Dispute_Reason,
                    GRT_Update_Rep,
                    Date_Updated,
                    Source_File,
                    Edit_DT
                )

                OUTPUT
                    INSERTED.DH_ID,
                    INSERTED.DSB_ID,
                    INSERTED.Display_Status,
                    INSERTED.Source_File

                INTO DH

                select
                    DSB.DSB_ID,
                    A.Dispute_Category,
                    isnull(A.Dispute_Status,'Filed'),
                    cast(getdate() as date),
                    A.ILEC_Confirmation,
                    A.ILEC_Comment,
                    A.Approved_Amt,
                    A.Received_Amt,
                    A.Received_Invoice_Date,
                    A.Dispute_Reason,
                    B.Full_Name,
                    getdate(),
                    'GRT Email: ' + format(getdate(),'yyyyMMdd'),
                    getdate()

                from mydisputes As A
                inner join {1} As B
                on
                    A.Comp_Serial = B.Comp_Serial
                inner join DSB
                on
                    DSB.STC_Claim_Number = A.STC_Claim_Number
                    
                where
                    A.Error_Columns is null
                        and
                    A.Dispute_Type = 'Email'
            '''.format(settings['Dispute_History'], settings['CAT_Emp']))

            self.asql.execute('''
                insert into {0}
                (
                    DSB_ID,
                    Dispute_Type,
                    Norm_Vendor,
                    Vendor,
                    Norm_Platform,
                    Platform,
                    State,
                    BAN,
                    Bill_Date,
                    USI,
                    STC_Claim_Number,
                    #_Of_Escalations,
                    Status,
                    Display_Status,
                    Date_Submitted,
                    Norm_Dispute_Category,
                    Dispute_Category,
                    Audit_Type,
                    Disputer,
                    Comment,
                    Dispute_Amount,
                    Dispute_Reason,
                    Date_Updated,
                    Batch,
                    Edit_Date,
                    Source_File,
                    DH_ID
                )
                select
                    DSB.DSB_ID,
                    A.Dispute_Type,
                    VM.Vendor,
                    BM.BDT_Vendor,
                    PM.Platform,
                    PM.Platform,
                    A.State,
                    A.BAN,
                    A.Bill_Date,
                    A.USI,
                    A.STC_Claim_Number,
                    0,
                    'Open',
                    case
                        when DH.Display_Status is not null then DH.Display_Status
                        else 'Pending Review'
                    end,
                    getdate(),
                    A.Dispute_Category,
                    A.Dispute_Category,
                    A.Audit_Type,
                    DS.Rep,
                    A.Comment,
                    A.Dispute_Amt,
                    A.Dispute_Reason,
                    getdate(),
                    DS.Batch_DT,
                    getdate(),
                    DH.Source_File,
                    DH.DH_ID

                from DSB
                inner join DS
                on
                    DSB.DSB_ID = DS.DSB_ID
                inner join mydisputes A
                on
                    DSB.STC_Claim_Number = A.STC_Claim_Number
                left join DH
                on
                    DSB.DSB_ID = DH.DSB_ID
                left join {1} BM
                on
                    DSB.BanMaster_ID = BM.ID
                left join {2} PM
                on
                    BM.PlatformMasterID = PM.ID
                left join {3} VM
                on
                    BM.VendorMasterID = VM.ID
            '''.format(settings['Dispute_Current'], settings['Ban_Master'], settings['Platform_Master']
                       , settings['Vendor_Master']))

            self.asql.execute('drop table DS, DSB, DH')

    def dispute(self):
        writelog("Processing {0} New Non-Seed Disputes".format(len(self.df)), 'info')

        self.asql = SQLConnect('alch')
        self.asql.connect()
        self.asql.upload(self.df, 'mydisputes')

        validatecol(self.asql, 'mydisputes', 'Bill_Date', True, True)
        validatecol(self.asql, 'mydisputes', 'Billed_Amt')
        validatecol(self.asql, 'mydisputes', 'Dispute_Amt')
        validatecol(self.asql, 'mydisputes', 'Approved_Amt')
        validatecol(self.asql, 'mydisputes', 'Received_Amt')
        validatecol(self.asql, 'mydisputes', 'Received_Invoice_Date', True)

        self.appenddisputes()
        processresults(self.folder_name, self.asql, 'mydisputes', 'New Non-Seed Disputes')
        self.asql.close()
