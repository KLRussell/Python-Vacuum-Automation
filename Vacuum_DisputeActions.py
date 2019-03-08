from Vacuum_Global import settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import validatecol
from Vacuum_Global import processresults
from Vacuum_Global import writelog
from Vacuum_Global import defaultheader


class DisputeActions:
    asql = None

    def __init__(self, action, df, folder_name):
        self.action = action
        self.df = df
        self.folder_name = folder_name

        self.df = defaultheader(self.df, '''dispute_id, action, amount, credit_invoice_date, action_norm_reason
            , action_reason, assign_rep, note_tag, attachment, ilec_confirmation, error_columns, error_message''')

    def escalate(self):
        # migrate core elements from Vacuum_Global escalate function
        self.asql.execute('''
            
        ''')

    def close(self):
        # migrate core elements from Vacuum_Global close function
        self.asql.execute('''

        ''')

    def paid(self):
        # migrate core elements from Vacuum_Global paid function
        validatecol(self.asql, 'grtactions', 'Amount')
        validatecol(self.asql, 'grtactions', 'Credit_Invoice_Date', True)

    def denied(self):
        self.asql.execute('''

        ''')

    def approved(self):
        validatecol(self.asql, 'grtactions', 'Amount')

    def disputenote(self):
        # migrate core elements from Vacuum_Global dispute note function
        self.asql.execute('''

        ''')

    def process(self):
        writelog("Processing {0} GRT Dispute Actions".format(len(self.df)), 'info')

        self.asql = SQLConnect('alch')
        self.asql.connect()
        self.asql.upload(self.df, 'grtactions')

        if self.action == 'Escalate':
            self.escalate()
        elif self.action == 'Close':
            self.close()
        elif self.action == 'Paid':
            self.paid()
        elif self.action == 'Denied':
            self.denied()
        elif self.action == 'Approved':
            self.approved()
        elif self.action == 'Dispute Note':
            self.disputenote()

        self.asql.close()
