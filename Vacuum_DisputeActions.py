from Vacuum_Global import settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import validatecol
from Vacuum_Global import processresults
from Vacuum_Global import writelog


class DisputeActions:
    asql = None

    def __init__(self, action, df, folder_name):
        self.action = action
        self.df = df
        self.folder_name = folder_name

        cols = '''dispute_id, action, amount, credit_invoice_date, action_norm_reason, action_reason, assign_rep
            , note_tag, attachment, ilec_confirmation, error_columns, error_message
                    '''.replace(chr(10), '').replace(chr(32), '').split(',')

        for col in cols:
            if col not in self.df.columns.str.lower():
                self.df[col] = None

    def escalate(self):

    def close(self):

    def paid(self):
        validatecol(self.asql, 'grtactions', 'Amount')
        validatecol(self.asql, 'grtactions', 'Credit_Invoice_Date', True)

    def denied(self):

    def approved(self):
        validatecol(self.asql, 'grtactions', 'Amount')

    def disputenote(self):

    def process(self):
        writelog("Processing {0} New Non-Seed Disputes".format(len(self.df)), 'info')

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
