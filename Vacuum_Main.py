from time import sleep
from datetime import datetime

import pathlib as pl
import pandas as pd
import xml.etree.ElementTree as ET
import os

SourceDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

UpdatesDir = []
UpdatesDir.append(SourceDir + "\\01_Updates\\01_BMI-PCI")
UpdatesDir.append(SourceDir + "\\01_Updates\\02_Seeds")
UpdatesDir.append(SourceDir + "\\01_Updates\\03_Non-Seeds")
UpdatesDir.append(SourceDir + "\\01_Updates\\04_Dispute-Actions")
UpdatesDir.append(SourceDir + "\\01_Updates\\05_New-User")

class XMLParseClass:
    def __init__(self, folder_name, File, upload_date):
        try:
            tree = ET.parse(File)
            self.folder_name = folder_name
            self.root = tree.getroot()
            self.upload_date = upload_date
        except AssertionError as a:
            print('\t[-] {} : Parse failed.'.format(a))
            pass

    def ParseElement(self, element, parsed=None):
        if parsed is None:
            parsed = dict()

        for key in element.keys():
            if key not in parsed:
                parsed[key] = element.attrib.get(key)

            if element.text and element.tag not in parsed:
                parsed[element.tag] = element.text

        for child in list(element):
            self.ParseElement(child, parsed)
        return parsed

    def ParseXML(self):
        parsed = [self.ParseElement(item) for item in self.root.findall('./{urn:schemas-microsoft-com:rowset}data/')]
        return pd.DataFrame(parsed)

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
        XMLObj = XMLParseClass(folder_name, File, upload_date)

        if XMLObj:
            print(XMLObj.ParseXML())

if __name__ == '__main__':
    Has_Updates = None

    while (Has_Updates is None):
        Has_Updates = Check_For_Updates()
        sleep(1)

    Process_Updates(Has_Updates)
