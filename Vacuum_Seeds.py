from Vacuum_Global import settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import getbatch
from Vacuum_Global import validatecol
from Vacuum_Global import processresults


class Seeds:
    args = dict()

    def __init__(self, folder_name, df=None):
        self.folder_name = folder_name
        self.df = df
        self.setdefaults()

    def setdefaults(self):
        if self.df.empty:
            self.args['DSB_Cols'] = 'USI, STC_Claim_Number, Source_TBL, Source_ID'
            self.args['DSB_Sel'] = '''USI, '{0}_' + left(Record_Type,1) + cast(Seed as varchar), Source_TBL
                , Source_ID'''.format(getbatch())
            self.args['DSB_On'] = '''DSB.Stc_Claim_Number='{0}_' + left(A.Record_Type,1) + cast(A.Seed as varchar)
                '''.format(getbatch())
            self.args['DS_Cols'] = '''DSB_ID, Rep, STC_Claim_Number, Dispute_Type, Dispute_Category, Audit_Type
                , Cost_Type, Cost_Type_Seed,  Record_Type, Dispute_Reason, PON, Comment, Confidence, Batch_DT'''
            self.args['DS_Sel'] = '''DSB.DSB_ID, B.Full_Name, '{0}_' + left(A.Record_Type,1) + cast(A.Seed as varchar)
                , A.Claim_Channel, 'GRT CNR', 'CNR Audit', A.Cost_Type, A.Seed, A.Record_Type, A.Action_Reason
                , A.PON, A.Action_Comment, A.Confidence, '{1}'
                '''.format(getbatch(), getbatch(True))
            self.args['DH_Cols'] = '''DSB_ID, Dispute_Category, Display_Status, Date_Submitted, Dispute_Reason
                , GRT_Update_Rep, Date_Updated, Source_File'''
            self.args['DH_Sel'] = '''DSB.DSB_ID, 'GRT CNR', 'Filed', getdate(), A.Action_Reason, B.Full_Name
                , getdate(), 'GRT Email: ' + format(getdate(),'yyyyMMdd')'''
            self.args['DH_Whr'] = "A.Claim_Channel = 'Email'"
        else:
            self.args['DSB_Cols'] = 'USI, STC_Claim_Number, Dispute_Amount'
            self.args['DSB_Sel'] = '''USI, '{0}_' + left(Record_Type,1) + cast(Cost_Type_Seed as varchar)
            '''.format(getbatch())
            self.args['DSB_On'] = '''
                DSB.Stc_Claim_Number='{0}_' + left(A.Record_Type,1) + cast(A.Cost_Type_Seed as varchar)
                '''.format(getbatch())
            self.args['DS_Cols'] = '''DSB_ID, Rep, Dispute_Type, Cost_Type, Cost_Type_Seed, USOC, USOC_Desc
            , Record_Type, Dispute_Category, Audit_Type, STC_Claim_Number, Claimed_Amt, Dispute_Reason
            , Billed_Phrase_Code, Causing_SO, PON, CLLI, Usage_Rate, MOU, Jurisdiction, Short_Paid, Comment
            , Comment, Confidence, Batch_DT'''
            self.args['DS_Sel'] = '''DSB.DSB_ID, B.Full_Name, A.Dispute_Type, A.Cost_Type, A.Cost_Type_Seed, A.USOC
            , A.USOC_Desc, A.Record_Type, A.Dispute_Category, A.Audit_Type
            , '{0}_' + left(A.Record_Type,1) + cast(A.Cost_Type_Seed as varchar), A.Dispute_Amt, A.Dispute_Reason
            , A.Phrase_Code, A.Causing_SO, A.PON, A.CLLI, A.Usage_Rate, A.MOU, A.Jurisdiction, A.Short_Paid
            , A.Comment, A.Confidence, '{1}'
            '''.format(getbatch(), getbatch(True))
            self.args['DH_Cols'] = '''DSB_ID, Dispute_Category, Display_Status, Date_Submitted, Dispute_Reason
                , ILEC_Confirmation, ILEC_Comments, Credit_Approved, Credit_Received_Amount
                , Credit_Received_Invoice_Date, GRT_Update_Rep, Date_Updated, Source_File'''
            self.args['DH_Sel'] = '''DSB.DSB_ID, A.Dispute_Category, A.Display_Status, getdate()
                , A.Dispute_Reason, A.ILEC_Confirmation, A.ILEC_Comment, A.Approved_Amt, A.Received_Amt
                , A.Received_Invoice_Date, GRT_Update_Rep, getdate(), 'GRT Email: ' + format(getdate(),'yyyyMMdd')'''
            self.args['DH_Whr'] = "A.Dispute_Type = 'Email'"

            cols = '''dispute_type, cost_type, cost_type_seed, dispute_category, audit_type, confidence, dispute_reason
                            , record_type, usi, dispute_amt, usoc, usoc_desc, pon, phrase_code, causing_so, clli
                            , usage_rate, mou, jurisdiction, short_paid, comment, dispute_status, ilec_confirmation
                            , ilec_comment, approved_amt, received_amt, received_invoice_date'''.split(', ')

            for col in cols:
                if col not in self.df.columns:
                    self.df[col] = None

    def appenddisputes(self, asql):
        if asql.query("select object_id('myseeds')").iloc[0, 0]:
            # writelog("Disputing {} cost".format(source), 'info')
            if asql.query("select object_id('DS')").iloc[0, 0]:
                asql.execute("DROP TABLE DS")

            if asql.query("select object_id('DSB')").iloc[0, 0]:
                asql.execute("DROP TABLE DSB")

            if asql.query("select object_id('DH')").iloc[0, 0]:
                asql.execute("DROP TABLE DH")

            asql.execute("CREATE TABLE DSB (DSB_ID int, Stc_Claim_Number varchar(255))")
            asql.execute("CREATE TABLE DS (DS_ID int, DSB_ID int)")
            asql.execute("CREATE TABLE DH (DH_ID int, DSB_ID int)")

            asql.execute('''
                update A
                    set A.Claim_Channel = 'Email'

                from myseeds As A

                where
                    A.Error_Columns is null
                        and
                    A.Source_TBL = 'PCI'
            ''')

            asql.execute('''
                insert into {0}
                (
                    {1}
                )

                OUTPUT
                    INSERTED.DSB_ID,
                    INSERTED.Stc_Claim_Number

                INTO DSB

                select
                    {2}

                from myseeds
                
                where
                    Error_Columns is null;'''.format(settings['Dispute_Staging_Bridge'], self.args['DSB_Cols'], self.args['DSB_Sel']))

            asql.execute('''
                insert into {0}
                (
                    {2}
                )

                OUTPUT
                    INSERTED.DS_ID,
                    INSERTED.DSB_ID

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

            asql.execute('''
                insert into {0}
                (
                    {2}
                )

                OUTPUT
                    INSERTED.DH_ID,
                    INSERTED.DSB_ID

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

            asql.execute('''
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
            '''.format(settings['Dispute_Fact']))

            asql.execute('drop table myseeds, DS, DSB, DH')

    def dispute(self, asql=None):
        if not asql:
            # NEED TO POPULATE RECORD_TYPE, VALIDATE ERRORS, AND APPEND ERRORS TO VIRTUAL LIST
            asql = SQLConnect('alch')
            asql.connect()
            asql.upload(self.df, 'myseeds')

            validatecol(asql, 'myseeds', 'Dispute_Amount')
            validatecol(asql, 'myseeds', 'Approved_Amt')
            validatecol(asql, 'myseeds', 'Received_Amt')
            validatecol(asql, 'myseeds', 'Received_Invoice_Date', True)

            for cost_type in settings['Seed-Cost_Type']:
                if 'PC-' in cost_type:
                    table = settings['PaperCost']
                    seed = 'seed'
                elif 'LPC' == cost_type:
                    table = settings[cost_type]
                    seed = 'invoice'
                else:
                    table = settings[cost_type]
                    seed = 'bdt_{0}_id'.format(cost_type)

                if 'PC-' in cost_type:
                    record_type = cost_type.split('-')[1]
                elif cost_type == 'OCC':
                    record_type = 'B.Activity_Type'
                else:
                    record_type = "'{0}'".format(cost_type)

                asql.execute('''
                    update A
                        set
                            A.Error_Columns = iif(B.{1} is null,'Cost_Type, Cost_Type_Seed',NULL),
                            A.Error_Message = iif(B.{1} is null,'Cost_Type_Seed does not exist',NULL),
                            A.Record_Type = {3}
                            
                    from myseeds as A
                    left join {0} as B
                    on
                        A.Cost_Type_Seed = B.{1}
                    
                    where
                        A.Error_Columns is null
                            and
                        A.Cost_Type = '{2}'
                '''.format(table, seed, cost_type, record_type))

            self.appenddisputes(asql)
            asql.close()
        else:
            self.appenddisputes(asql)
