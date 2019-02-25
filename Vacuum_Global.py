from urllib.parse import quote_plus
from sqlalchemy.orm import sessionmaker

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

#Settings['HadoopDSN']
class SQLConnect:
    session = False

    def __init__(self,conn_type,dsn=None):
        self.conn_type = conn_type

        if conn_type == 'alch':
            self.connstring = self.AlchConnStr(
                '{SQL Server Native Client 11.0}', 1433, Settings['Server'], Settings['Database'], 'mssql'
                )
        elif conn_type == 'sql':
            self.connstring = self.SQLConnStr(Settings['Server'], Settings['Database'])
        elif conn_type == 'dsn':
            self.connstring = self.DSNConnStr(dsn)

    def AlchConnStr(self, driver, port, server, database, flavor='mssql'):
        def params(driver, port, server, database):
            return quote_plus (
                'DRIVER={};PORT={};SERVER={};DATABASE={};Trusted_Connection=yes;'
                .format(driver, port, server, database)
            )

        p = params(driver, port, server, database)
        return '{}+pyodbc:///?odbc_connect={}'.format(flavor, p)

    def SQLConnStr(self, server, database):
        return 'driver={0};server={1};database={2};autocommit=True;Trusted_Connection=yes'.format('{SQL Server}', server, database)

    def DSNConnStr(self, DSN):
        return 'DSN={};DATABASE=default;Trusted_Connection=Yes;'.format(DSN)

    def connect(self):
        if self.conn_type == 'alch':
            self.engine = sql.create_engine(self.connstring)
        else:
            self.conn = pyodbc.connect(self.connstring)
            self.cursor = self.conn.cursor()
            self.conn.commit()

    def close(self):
        if self.conn_type == 'alch':
            self.engine.dispose()
        else:
            self.cursor.close()
            self.conn.close()

    def createsession(self):
        if self.conn_type == 'alch':
            self.engine = sessionmaker(bind=self.engine)
            self.engine = self.engine()
            self.engine._model_changes = {}
            self.session = True

    def createtable(self, dataframe, sqltable):
        if self.conn_type == 'alch' and not self.session:
            dataframe.to_sql(
                sqltable,
                self.engine,
                if_exists='replace',
            )

    def upload(self, dataframe, sqltable):
        if self.conn_type == 'alch' and not self.session:
            mytbl = sqltable.split(".")

            if len(mytbl) > 1:
                dataframe.to_sql(
                    mytbl[1],
                    self.engine,
                    schema=mytbl[0],
                    if_exists='append',
                    index=True,
                    index_label='linenumber',
                    chunksize=1000
                )
            else:
                dataframe.to_sql(
                    mytbl[0],
                    self.engine,
                    if_exists='replace',
                    index=False,
                    chunksize=1000
                )

    def query(self, query):
        try:
            if self.conn_type == 'alch':
                obj = self.engine.execute(query)
                print(obj.rowcount)
                if obj.rowcount > 0:
                    data = obj.fetchall()
                    columns = obj._metadata.keys
                    #dtypes = [col.type for col in data.context.compiled.statement.columns]
                    #print(dtypes)

                    return pd.DataFrame(data, columns=columns)

            else:
                df = pd.io.sql.read_sql(query, self.conn)
                return df

        except AssertionError as a:
            print('\t[-] {} : SQL Query failed.'.format(a))
            pass

    def execute(self, query):
        try:
            if self.conn_type == 'alch':
                self.engine.execute(query)
            else:
                self.cursor.execute(query)


        except AssertionError as a:
            print('\t[-] {} : SQL Execute failed.'.format(a))
            pass

def init():
    global Settings
    global Errors

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

def Append_Errors(DF):
    if not DF.empty:
        Errors.append(DF)

def Get_Errors():
    if Errors:
        return pd.concat(Errors, ignore_index=True, sort=False).drop_duplicates().reset_index(drop=True)

Errors = []
Settings = dict()
Settings = Load_Settings()
