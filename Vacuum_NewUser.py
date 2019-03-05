from Vacuum_Global import settings
from Vacuum_Global import SQLConnect


class NewUser:

    def __init__(self, file, upload_date):
        self.file = file
        self.upload_date = upload_date
