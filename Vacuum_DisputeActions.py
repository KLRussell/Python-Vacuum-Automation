from Vacuum_Global import settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import validatecol
from Vacuum_Global import processresults
from Vacuum_Global import writelog


class DisputeActions:

    def __init__(self, action, df, folder_name):
        self.action = action
        self.df = df
        self.folder_name = folder_name

    def process(self):
        writelog("Processing {0} New Non-Seed Disputes".format(len(self.df)), 'info')
        asql = SQLConnect('alch')
        asql.connect()
        asql.upload(self.df, 'mydisputes')

        asql.close()
