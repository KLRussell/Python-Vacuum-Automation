from time import sleep
from datetime import datetime
from PyQt5 import QtWidgets
from Vacuum_Global import XMLParseClass
from Vacuum_Global import XMLAppendClass
from Vacuum_Global import settings
from Vacuum_Global import get_errors
from Vacuum_Global import writelog
from Vacuum_Global import log_filepath
from Vacuum_BMIPCI import BMIPCI
from Vacuum_DisputeActions import DisputeActions
from Vacuum_NewUser import newuser
from Vacuum_NonSeeds import NonSeeds
from Vacuum_Seeds import Seeds

import pandas as pd
import pathlib as pl
import os
import gc
import sys
import random
import datetime

gc.collect()


def myexithandler():
    writelog('Exiting Vacuum...', 'warning')
    gc.collect()


def generatetalk():
    f = open(settings['Vacuum_Talk'], 'r')
    lines = f.readlines()
    talkid = random.randint(0, len(lines) - 1)

    print('The Vacuum: {0}'.format(lines[talkid]))

    f.close()


def process_errors():
    clogged = False
    df = pd.DataFrame()

    for dirpath in settings['UpdatesDir']:
        df = get_errors(os.path.basename(dirpath))

        if not df.empty:
            if not clogged:
                writelog('Vacuum clogged with Errors. Cleaning vacuum...', 'error')
                clogged = True

            for serial in df['Comp_Serial'].unique():
                writelog('Appending {0} item(s) for {1} from Error virtual list'.format(
                    len(df[df['Comp_Serial'] == serial].index), serial), 'warning')

                if not os.path.exists(settings['ErrorsDir'] + '//{}'.format(serial)):
                    os.makedirs(settings['ErrorsDir'] + '//{}'.format(serial))

                myobj = XMLAppendClass(settings['ErrorsDir'] + '//{0}//{1}_E{2}.xml'.format(
                    serial, datetime.datetime.now().__format__("%Y%m%d"), random.randint(10000000, 10000000000)))
                myobj.write_xml(df[df['Comp_Serial'] == serial])

    del df


def check_for_updates():
    for dirpath in settings['UpdatesDir']:
        files = list(pl.Path(dirpath).glob('*.xml'))
        if files:
            return files


def process_updates(files):
    writeblank = False

    for file in files:
        folder_name = os.path.basename(os.path.dirname(file))

        if writeblank:
            writelog("", 'info')

        writelog("Reading file ({0}/{1})".format(folder_name, os.path.basename(file)), 'info')
        writelog("", 'info')

        if folder_name == '05_New_User':
            newuser(file)
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
                    myobj = Seeds(folder_name, None, parsed)
                    myobj.dispute()

                elif folder_name == '03_Non-Seeds':
                    myobj = NonSeeds(parsed, folder_name)
                    myobj.dispute()

                elif folder_name == '04_Dispute-Actions':
                    for action in settings['Dispute_Actions-Action']:
                        df = parsed.loc[parsed['Action'] == action]

                        if not df.empty:
                            myobj = DisputeActions(action=action, folder_name=folder_name, df=df)
                            myobj.process()
                del parsed
            del xmlobj
        del folder_name
        writeblank = True
        os.remove(file)


app = QtWidgets.QApplication(sys.argv).instance()
app.aboutToQuit.connect(myexithandler)

if __name__ == '__main__':
    if os.path.isfile(log_filepath()):
        writelog('', 'info')

    writelog('Starting Vacuum...', 'info')
    continue_flag = False

    while 1 != 0:
        Has_Updates = None
        writelog('Vacuum sniffing floor for crumbs...', 'info')

        while Has_Updates is None:
            Has_Updates = check_for_updates()
            sleep(1)
            rand = random.randint(1, 10000000000)
            if continue_flag:
                writelog('', 'info')
                continue_flag = False
            elif rand % 777 == 0:
                generatetalk()

        process_updates(Has_Updates)
        process_errors()
        continue_flag = True

    # os.system('pause')

gc.collect()
