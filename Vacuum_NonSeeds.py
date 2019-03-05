from Vacuum_Global import settings
from Vacuum_Global import SQLConnect


class NonSeeds:
    Errors = None

    def __init__(self, df, folder_name, upload_date):
        self.df = df
        self.folder_name = folder_name
        self.upload_date = upload_date
