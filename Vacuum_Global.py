from urllib.parse import quote_plus

import sqlalchemy as sql
import pandas as pd
import xml.etree.ElementTree as ET
import os, pyodbc

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

    def ParseXML(self, findpath, DictVar=False):
        if isinstance(DictVar, dict):
            for item in self.root.findall(findpath):
                DictVar = self.ParseElement(item, DictVar)

            return DictVar
        else:
            parsed = [self.ParseElement(item) for item in self.root.findall(findpath)]
            return pd.DataFrame(parsed)

class SQLConnect:
    def __init__(self,conn_type):
        self.conn_type = conn_type

        if conn_type == 'sql':
            self.connstring = self.sql_server(
                '{SQL Server Native Client 11.0}', 1433, Settings['Server'], Settings['Database'], 'mssql'
                )
            self.engine = sql.create_engine(self.connstring)
        else:
            self.connstring = self.hadoop(Settings['HadoopDSN'])
            self.conn = pyodbc.connect(self.connstring)
            self.cursor = self.conn.cursor()

    def sql_server(self, driver, port, server, database, flavor='mssql'):
        def params(driver, port, server, database):
            return quote_plus (
                'DRIVER={};PORT={};SERVER={};DATABASE={};Trusted_Connection=yes;'
                .format(driver, port, server, database)
            )

        p = params(driver, port, server, database)
        return '{}+pyodbc:///?odbc_connect={}'.format(flavor, p)

    def hadoop(self, DSN):
        return 'DSN={};DATABASE=default;Trusted_Connection=Yes;'.format(DSN)

    def close(self):
        if self.conn_type == 'sql':
            self.engine.dispose()
        else:
            self.cursor.close()
            self.conn.close()

    def upload(self, dataframe, sqltable):
        if self.conn_type == 'sql':
            dataframe.to_sql(
                sqltable.split(".")[1], self.engine,
                schema=sqltable.split(".")[0],
                if_exists='append',
                index=True,
                index_label='linenumber',
                chunksize=1000
            )

    def createtable(self, dataframe, sqltable):
        if self.conn_type == 'sql':
            dataframe.to_sql(
                sqltable,
                self.engine,
                if_exists='replace',
                flavor='mysql',
                index=False
            )

    def query(self, query):
        try:
            if self.conn_type == 'sql':
                data = self.engine.execute(query)
                dtypes = [col.type for col in data.context.compiled.statement.columns]
                return pd.DataFrame(data.fetchall(), columns=data._metadata.keys, dtypes=dtypes)

            else:
                self.cursor.execute(query)
                columns = [column[0] for column in self.cursor.description]
                dtypes = [dtype[1] for dtype in self.cursor.description]
                return pd.DataFrame(self.cursor.fetchall(), columns=columns, dtypes=dtypes)

        except AssertionError as a:
            print('\t[-] {} : SQL Query failed.'.format(a))
            pass

    def execute(self, query):
        try:
            if self.conn_type == 'sql':
                self.engine.execute(query)

            else:
                self.cursor.execute(query)

        except AssertionError as a:
            print('\t[-] {} : SQL Execute failed.'.format(a))
            pass

def init():
    global Settings

def Load_Settings():
    Settings = dict()

    Settings['SourceDir'] = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    Settings['SourceCodeDir'] = Settings['SourceDir'] + "\\03_Source_Code"

    UpdatesDir = []
    UpdatesDir.append(Settings['SourceDir'] + "\\01_Updates\\01_BMI-PCI")
    UpdatesDir.append(Settings['SourceDir'] + "\\01_Updates\\02_Seeds")
    UpdatesDir.append(Settings['SourceDir'] + "\\01_Updates\\03_Non-Seeds")
    UpdatesDir.append(Settings['SourceDir'] + "\\01_Updates\\04_Dispute-Actions")
    UpdatesDir.append(Settings['SourceDir'] + "\\01_Updates\\05_New-User")

    Settings['UpdatesDir'] = UpdatesDir

    XMLObj = XMLParseClass(Settings['SourceCodeDir'] + "\\Vacuum_Settings.xml")

    if XMLObj:
        Settings = XMLObj.ParseXML('./Settings/Network/', Settings)
        Settings = XMLObj.ParseXML('./Settings/Read_Write_TBL/', Settings)
        Settings = XMLObj.ParseXML('./Settings/Read_TBL/', Settings)

        Settings['Seed-Cost_Type'] = XMLObj.ParseXML('./Settings/CAT_Workbook/Seed_Disputes/', Settings)['Cost_Type']

        DF = XMLObj.ParseXML('./Settings/CAT_Workbook/BMIPCI_Review/Action/')
        Settings['BMIPCI-Action'] = DF.loc[:, 'Action'].values

        DF = XMLObj.ParseXML('./Settings/CAT_Workbook/Dispute_Actions/Action/')
        Settings['Dispute_Actions-Action'] = DF.loc[:, 'Action'].values
        return Settings
    else:
        raise ValueError("Unable to load Vacuum_Settings.xml. Please check path and file {0}".format(Settings['SourceCodeDir']))

Settings = dict()
Settings = Load_Settings()
