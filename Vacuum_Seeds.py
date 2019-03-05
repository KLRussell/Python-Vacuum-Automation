from Vacuum_Global import settings
from Vacuum_Global import SQLConnect


class Seeds:

    def __init__(self, cost_type, df, folder_name):
        self.cost_type = cost_type
        self.df = df
        self.folder_name = folder_name

    def mrc(self, dispute_type):
        asql = SQLConnect('alch')
        asql.connect()

        asql.upload(self.df, 'mytbl')

        asql.execute('drop table mytbl')

        asql.close()

    def process(self):
        if self.cost_type == 'MRC':
            self.mrc('GRT')
            self.mrc('Email')

