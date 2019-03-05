from Vacuum_Global import settings
from Vacuum_Global import SQLConnect


class Seeds:
    Errors = None

    def __init__(self, cost_type, df, folder_name, upload_date):
        self.Cost_Type = cost_type
        self.df = df
        self.folder_name = folder_name
        self.upload_date = upload_date
