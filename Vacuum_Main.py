from time import sleep
from datetime import datetime
from Vacuum_Global import XMLParseClass
from Vacuum_Global import settings
from Vacuum_Global import get_errors
from Vacuum_BMIPCI import BMIPCI
from Vacuum_DisputeActions import DisputeActions
from Vacuum_NewUser import NewUser
from Vacuum_NonSeeds import NonSeeds
from Vacuum_Seeds import Seeds
from PyQt5 import QtWidgets

import pathlib as pl
import os, gc, sys

gc.collect()


def myexithandler():
    os.system('pause')


def process_errors():
    df = get_errors()
    print(df)
    del df


def check_for_updates():
    for DirPath in settings['UpdatesDir']:
        files = list(pl.Path(DirPath).glob('*.xml'))
        if files:
            return files


def process_updates(files):
    for file in files:
        upload_date = datetime.now()
        folder_name = os.path.basename(os.path.dirname(file))

        print("Processing {0}/{1}".format(folder_name,os.path.basename(file)))

        if folder_name == '05_New_User':
            NewUser(file, upload_date)
        else:
            xmlobj = XMLParseClass(file)

            if xmlobj:
                parsed = xmlobj.parsexml('./{urn:schemas-microsoft-com:rowset}data/')

                if folder_name == '01_BMI-PCI':
                    for action in settings['BMIPCI-Action']:
                        df = parsed.loc[parsed['Action'] == action]

                        if not df.empty:
                            myobj = BMIPCI(action, df)
                            myobj.process()

                elif folder_name == '02_Seeds':
                    for Cost_Type in settings['Seed-Cost_Type'].split(', '):
                        myobj = Seeds(Cost_Type, parsed.loc[parsed['Cost_Type'] == Cost_Type], folder_name)
                        myobj.process()

                elif folder_name == '03_Non-Seeds':
                    NonSeeds(parsed, folder_name, upload_date)

                elif folder_name == '04_Dispute-Actions':
                    for action in settings['Dispute_Actions-Action']:
                        DisputeActions(action, parsed.loc[parsed['Action'] == action], folder_name, upload_date)
                del parsed
            del xmlobj
        del upload_date, folder_name


app = QtWidgets.QApplication(sys.argv)
app.aboutToQuit.connect(myexithandler)

if __name__ == '__main__':
    Has_Updates = None

    while Has_Updates is None:
        Has_Updates = check_for_updates()
        sleep(1)

    process_updates(Has_Updates)
    process_errors()

    os.system('pause')

gc.collect()
