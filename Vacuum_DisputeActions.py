from Vacuum_Global import settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import validatecol
from Vacuum_Global import processresults
from Vacuum_Global import writelog
from Vacuum_Global import defaultheader


class DisputeActions:
    def __init__(self, action, folder_name, asql=None, df=None):
        self.action = action
        self.df = df
        self.folder_name = folder_name
        self.asql = asql

        self.df = defaultheader(self.df, '''dispute_id, action, amount, credit_invoice_date, action_norm_reason
            , action_reason, assign_rep, note_tag, attachment, ilec_confirmation, error_columns, error_message''')

    def escalate(self):
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

    def close(self):
        # migrate core elements from Vacuum_Global close function
        self.asql.execute('''

        ''')

    def paid(self):
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

    def denied(self):
        self.asql.execute('''

        ''')

    def approved(self):
        validatecol(self.asql, 'grtactions', 'Amount')

    def disputenote(self):
        self.asql.execute('''
            insert into {0}
            (
                DSB_ID,
                Logged_By,
                Norm_Note_Action,
                Dispute_Note,
                Days_Till_Action,
                Edit_Date
            )
            select
                DSB_ID,
                Full_Name,
                Action_Norm_Reason,
                Action_Reason,
                Amount_Or_Days,
                Edit_Date

            from mydisputes
        '''.format(settings['Dispute_Notes']))



    def process(self):
        if not self.asql:
            writelog("Processing {0} GRT Dispute Actions".format(len(self.df)), 'info')

            self.asql = SQLConnect('alch')
            self.asql.connect()
            self.asql.upload(self.df, 'grtactions')

        if self.action == 'Escalate':
            self.escalate()
        elif self.action == 'Close':
            self.close()
        elif self.action == 'Paid':
            validatecol(self.asql, 'grtactions', 'Amount')
            validatecol(self.asql, 'grtactions', 'Credit_Invoice_Date', True)
            self.paid()
        elif self.action == 'Denied':
            self.denied()
        elif self.action == 'Approved':
            self.approved()
        elif self.action == 'Dispute Note':
            self.disputenote()

        if not self.df.empty:
            processresults(self.folder_name, self.asql, 'mydisputes', 'New Seed Disputes')
            self.asql.close()
