from time import sleep
from datetime import datetime

import pathlib as pl
import pandas as pd
import xml.etree.ElementTree as ET
import os

SourceDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

SourceCodeDir = SourceDir + "\\03_Source_Code"

UpdatesDir = []
UpdatesDir.append(SourceDir + "\\01_Updates\\01_BMI-PCI")
UpdatesDir.append(SourceDir + "\\01_Updates\\02_Seeds")
UpdatesDir.append(SourceDir + "\\01_Updates\\03_Non-Seeds")
UpdatesDir.append(SourceDir + "\\01_Updates\\04_Dispute-Actions")
UpdatesDir.append(SourceDir + "\\01_Updates\\05_New-User")

Settings = dict()

class XMLParseClass:
    def __init__(self, File):
        try:
            tree = ET.parse(File)
            self.root = tree.getroot()
        except AssertionError as a:
            print('\t[-] {} : Parse failed.'.format(a))
            pass

    def ParseElement(self, element, parsed=None):
        if parsed is None:
            parsed = dict()

        if element.keys():
            for key in element.keys():
                if key not in parsed:
                    parsed[key] = element.attrib.get(key)

                if element.text and element.tag not in parsed:
                    parsed[element.tag] = element.text

        elif element.text and element.tag not in parsed:
            parsed[element.tag] = element.text


        for child in list(element):
            self.ParseElement(child, parsed)
        return parsed

    def ParseXML(self, findpath, dict=False):
        if dict:
            parsed = None

            for item in self.root.findall(findpath):
                parsed = self.ParseElement(item, parsed)

            return parsed
        else:
            parsed = [self.ParseElement(item) for item in self.root.findall(findpath)]
            return pd.DataFrame(parsed)

def Get_Global(Setting_Name):
    for a, b in Settings:
        print(a)


def Load_Settings():
    Settings = dict()
    XMLObj = XMLParseClass(SourceCodeDir + "\\Vacuum_Settings.xml")

    if XMLObj:
        Settings = XMLObj.ParseXML('./Settings/Network/', True)
        Settings = XMLObj.ParseXML('./Settings/Read_Write_TBL/', True)
        Settings = XMLObj.ParseXML('./Settings/Read_TBL/', True)

        DF = XMLObj.ParseXML('./Settings/CAT_Workbook/BMIPCI_Review/Action/')
        Settings = {'BMIPCI-Action': DF.loc[:, 'Action'].values}

        DF = XMLObj.ParseXML('./Settings/CAT_Workbook/Dispute_Actions/Action/')
        Settings = {'Dispute_Actions-Action': DF.loc[:, 'Action'].values}

        return Settings
    else:
        raise ValueError("Unable to load Vacuum_Settings.xml. Please check path and file {0}".format(SourceCodeDir))

def Check_For_Updates():
    for DirPath in UpdatesDir:
        Files = list(pl.Path(DirPath).glob('*.xml'))
        if Files:
            return Files

def Process_Updates(Files):
    for File in Files:
        upload_date = datetime.now()
        folder_name = os.path.basename(os.path.dirname(os.path.dirname(File)))
        XMLObj = XMLParseClass(File)

        if XMLObj:
            Parsed = XMLObj.ParseXML('./{urn:schemas-microsoft-com:rowset}data/')
            #print(Parsed.loc[Parsed['Action'] == 'Open Inquiry'])


if __name__ == '__main__':
    Has_Updates = None
    Settings = Load_Settings()
    print(Get_Global('Dispute_Actions-Action'))
    while (Has_Updates is None):
        Has_Updates = Check_For_Updates()
        sleep(1)

    Process_Updates(Has_Updates)
