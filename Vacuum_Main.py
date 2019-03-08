from time import sleep
from datetime import datetime
from PyQt5 import QtWidgets
from Vacuum_Global import XMLParseClass
from Vacuum_Global import settings
from Vacuum_Global import get_errors
from Vacuum_Global import writelog
from Vacuum_Global import log_filepath
from Vacuum_BMIPCI import BMIPCI
from Vacuum_DisputeActions import DisputeActions
from Vacuum_NewUser import NewUser
from Vacuum_NonSeeds import NonSeeds
from Vacuum_Seeds import Seeds

import pandas as pd
import pathlib as pl
import os
import gc
import sys

gc.collect()


def myexithandler():
    writelog('Exiting Vacuum...', 'warning')


def process_errors():
    df = pd.DataFrame()
    for dirpath in settings['UpdatesDir']:
        df = get_errors(os.path.basename(dirpath))
        if not df.empty:
            writelog('Processing {0} items from Error virtual list'.format(len(df.index)))
    del df


def check_for_updates():
    for dirpath in settings['UpdatesDir']:
        files = list(pl.Path(dirpath).glob('*.xml'))
        if files:
            return files


def process_updates(files):
    writeblank = False

    for file in files:
        upload_date = datetime.now()
        folder_name = os.path.basename(os.path.dirname(file))

        if writeblank:
            writelog("", 'info')

        writelog("Reading file ({0}/{1})".format(folder_name, os.path.basename(file)), 'info')
        writelog("", 'info')

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
                            myobj = BMIPCI(action, df, folder_name)
                            myobj.process()

                elif folder_name == '02_Seeds':
                    myobj = Seeds(folder_name, parsed)
                    myobj.dispute()

                elif folder_name == '03_Non-Seeds':
                    myobj = NonSeeds(parsed, folder_name)
                    myobj.dispute()

                elif folder_name == '04_Dispute-Actions':
                    for action in settings['Dispute_Actions-Action']:
                        DisputeActions(action, parsed.loc[parsed['Action'] == action], folder_name, upload_date)
                del parsed
            del xmlobj
        del upload_date, folder_name
        writeblank = True


app = QtWidgets.QApplication(sys.argv).instance()
app.aboutToQuit.connect(myexithandler)

if __name__ == '__main__':
    if os.path.isfile(log_filepath()):
        writelog('', 'info')

    writelog('Starting Vacuum...', 'info')

    Has_Updates = None

    while Has_Updates is None:
        Has_Updates = check_for_updates()
        sleep(1)

    process_updates(Has_Updates)
    process_errors()

    os.system('pause')

gc.collect()
