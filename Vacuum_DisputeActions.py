from Vacuum_Global import settings
from Vacuum_Global import SQLConnect


class DisputeActions:

    def __init__(self, action, df, folder_name):
        self.action = action
        self.df = df
        self.folder_name = folder_name

    def process(self):

