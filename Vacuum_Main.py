from time import sleep
from datetime import datetime

import pathlib as pl
import os

SourceDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

UpdatesDir = []
UpdatesDir.append(SourceDir + "\\01_Updates\\01_BMI-PCI")
UpdatesDir.append(SourceDir + "\\01_Updates\\02_Seeds")
UpdatesDir.append(SourceDir + "\\01_Updates\\03_Non-Seeds")
UpdatesDir.append(SourceDir + "\\01_Updates\\04_Dispute-Actions")
UpdatesDir.append(SourceDir + "\\01_Updates\\05_New-User")

class XMLParseClass:
    folder_name = None
    data = None
    upload_date = None

    def __init__(self, folder_name, data, upload_date):
        self.folder_name = folder_name
        self.data = data
        self.upload_date = upload_date

    def ParseXML(self):


def Check_For_Updates():
    for DirPath in UpdatesDir:
        Files = list(pl.Path(DirPath).glob('*.xml'))
        if Files:
            return Files

def Process_Updates(Files):
    XMLObj = None

    for File in Files:
        upload_date = datetime.now()
        folder_name = os.path.basename(os.path.dirname(os.path.dirname(File)))
        data = None

        with open(File,'r') as f:
            data = "".join(str(line) for line in f.readlines())

            if data:
                XMLObj = XMLParseClass(folder_name, data, upload_date)

    if XMLObj:
        XMLObj.ParseXML()

if __name__ == '__main__':
    Has_Updates = None

    while (Has_Updates is None):
        Has_Updates = Check_For_Updates()
        sleep(1)

    Process_Updates(Has_Updates)
