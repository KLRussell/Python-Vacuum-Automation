from time import sleep
from datetime import datetime
from Vacuum_Global import XMLParseClass
from Vacuum_Global import Errors
from Vacuum_Global import Settings
from Vacuum_Global import Get_Errors
from Vacuum_BMIPCI import BMIPCI
from Vacuum_DisputeActions import DisputeActions
from Vacuum_NewUser import NewUser
from Vacuum_NonSeeds import NonSeeds
from Vacuum_Seeds import Seeds
import pathlib as pl
import pandas as pd
import os

def Process_Errors():
    DF = Get_Errors()
    #print(DF)

def Check_For_Updates():
    for DirPath in Settings['UpdatesDir']:
        Files = list(pl.Path(DirPath).glob('*.xml'))
        if Files:
            return Files

def Process_Updates(Files):
    for File in Files:
        upload_date = datetime.now()
        folder_name = os.path.basename(os.path.dirname(File))

        if folder_name == '05_New_User':
            NewUser(File, upload_date)
        else:
            XMLObj = XMLParseClass(File)

            if XMLObj:
                Parsed = XMLObj.ParseXML('./{urn:schemas-microsoft-com:rowset}data/')

                if folder_name == '01_BMI-PCI':
                    for action in Settings['BMIPCI-Action']:
                        DF = Parsed.loc[Parsed['Action'] == action]

                        if not DF.empty:
                            MyObj = BMIPCI(action, DF, upload_date)

                            if MyObj.Validate():
                                MyObj.Process()

                elif folder_name == '02_Seeds':
                    for Cost_Type in Settings['Seed-Cost_Type'].split(', '):
                        Seeds(Cost_Type, Parsed.loc[Parsed['Cost_Type'] == Cost_Type], folder_name, upload_date)

                elif folder_name == '03_Non-Seeds':
                    NonSeeds(Parsed, folder_name, upload_date)

                elif folder_name == '04_Dispute-Actions':
                    for action in Settings['Dispute_Actions-Action']:
                        DisputeActions(action, Parsed.loc[Parsed['Action'] == action], folder_name, upload_date)

if __name__ == '__main__':
    Has_Updates = None

    while (Has_Updates is None):
        Has_Updates = Check_For_Updates()
        sleep(1)

    Process_Updates(Has_Updates)
    Process_Errors()
