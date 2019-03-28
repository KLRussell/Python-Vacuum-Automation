from urllib.parse import quote_plus
from sqlalchemy.orm import sessionmaker
from pandas.io import sql

import sqlalchemy as mysql
import pandas as pd
import xml.etree.ElementTree as ET
import os
import pyodbc
import datetime
import logging


class XMLParseClass:
    def __init__(self, file):
        try:
            tree = ET.parse(file)
            self.root = tree.getroot()
        except AssertionError as a:
            print('\t[-] {} : Parse failed.'.format(a))
            pass

    def parseelement(self, element, parsed=None):
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
            self.parseelement(child, parsed)
        return parsed

    def parsexml(self, findpath, dictvar=None):
        if isinstance(dictvar, dict):
            for item in self.root.findall(findpath):
                dictvar = self.parseelement(item, dictvar)

            return dictvar
        else:
            parsed = [self.parseelement(item) for item in self.root.findall(findpath)]
            df = pd.DataFrame(parsed)

            return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)


class XMLAppendClass:
    def __init__(self, file):
        self.file = file

    def write_xml(self, df):
        with open(self.file, 'w') as xmlFile:
            xmlFile.write(
                '<?xml version="1.0" encoding="UTF-8"?>\n'
            )
            xmlFile.write('<records>\n')

            xmlFile.write(
                '\n'.join(df.apply(self.xml_encode, axis=1))
            )

            xmlFile.write('\n</records>')

    @staticmethod
    def xml_encode(row):
        xmlitem = ['  <record>']

        for field in row.index:
            xmlitem \
                .append('    <var var_name="{0}">{1}</var>' \
                        .format(field, row[field]))

        xmlitem.append('  </record>')

        return '\n'.join(xmlitem)


class SQLConnect:
    session = False
    engine = None
    conn = None
    cursor = None

    def __init__(self, conn_type, dsn=None):
        self.conn_type = conn_type

        if conn_type == 'alch':
            self.connstring = self.alchconnstr(
                '{SQL Server Native Client 11.0}', 1433, settings['Server'], settings['Database'], 'mssql'
                )
        elif conn_type == 'sql':
            self.connstring = self.sqlconnstr(settings['Server'], settings['Database'])
        elif conn_type == 'dsn':
            self.connstring = self.dsnconnstr(dsn)

    @staticmethod
    def alchconnstr(driver, port, server, database, flavor='mssql'):
        p = quote_plus(
                'DRIVER={};PORT={};SERVER={};DATABASE={};Trusted_Connection=yes;'
                .format(driver, port, server, database))

        return '{}+pyodbc:///?odbc_connect={}'.format(flavor, p)

    @staticmethod
    def sqlconnstr(server, database):
        return 'driver={0};server={1};database={2};autocommit=True;Trusted_Connection=yes'.format('{SQL Server}',
                                                                                                  server, database)

    @staticmethod
    def dsnconnstr(dsn):
        return 'DSN={};DATABASE=default;Trusted_Connection=Yes;'.format(dsn)

    def connect(self):
        if self.conn_type == 'alch':
            self.engine = mysql.create_engine(self.connstring)
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
                obj = self.engine.execute(mysql.text(query))

                if obj._saved_cursor.arraysize > 0:
                    data = obj.fetchall()
                    columns = obj._metadata.keys

                    return pd.DataFrame(data, columns=columns)

            else:
                df = sql.read_sql(query, self.conn)
                return df

        except ValueError as a:
            print('\t[-] {} : SQL Query failed.'.format(a))
            pass

    def execute(self, query):
        try:
            if self.conn_type == 'alch':
                self.engine.execute(mysql.text(query))
            else:
                self.cursor.execute(query)

        except ValueError as a:
            print('\t[-] {} : SQL Execute failed.'.format(a))
            pass


def init():
    global settings
    global errors


def load_settings():
    mysettings = dict()
    updatesdir = []

    mysettings['SourceDir'] = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    mysettings['EventLogDir'] = mysettings['SourceDir'] + "\\02_Event_Log"
    mysettings['SourceCodeDir'] = mysettings['SourceDir'] + "\\03_Source_Code"
    mysettings['ErrorsDir'] = mysettings['SourceDir'] + "\\01_Updates\\06_Errors"

    updatesdir.append(mysettings['SourceDir'] + "\\01_Updates\\01_BMI-PCI")
    updatesdir.append(mysettings['SourceDir'] + "\\01_Updates\\02_Seeds")
    updatesdir.append(mysettings['SourceDir'] + "\\01_Updates\\03_Non-Seeds")
    updatesdir.append(mysettings['SourceDir'] + "\\01_Updates\\04_Dispute-Actions")
    updatesdir.append(mysettings['SourceDir'] + "\\01_Updates\\05_New-User")

    mysettings['UpdatesDir'] = updatesdir

    xmlobj = XMLParseClass(mysettings['SourceDir'] + "\\Vacuum_Settings.xml")

    if xmlobj:
        mysettings = xmlobj.parsexml('./Settings/Network/', mysettings)
        mysettings = xmlobj.parsexml('./Settings/Read_Write_TBL/', mysettings)
        mysettings = xmlobj.parsexml('./Settings/Read_TBL/', mysettings)
        mysettings = xmlobj.parsexml('./Settings/Other/', mysettings)

        mysettings['Seed-Cost_Type'] = \
            xmlobj.parsexml('./Settings/CAT_Workbook/Seed_Disputes/', mysettings)['Cost_Type']

        df = xmlobj.parsexml('./Settings/CAT_Workbook/BMIPCI_Review/Action/')
        mysettings['BMIPCI-Action'] = df.loc[:, 'Action'].values

        df = xmlobj.parsexml('./Settings/CAT_Workbook/Dispute_Actions/Action/')
        mysettings['Dispute_Actions-Action'] = df.loc[:, 'Action'].values
        return mysettings
    else:
        raise ValueError("Unable to load Vacuum_Settings.xml. Please check path and file {0}"
                         .format(mysettings['SourceCodeDir']))


def append_errors(folder_name, df):
    if not df.empty:
        writelog('{} Error(s) found. Appending to virtual list'.format(len(df.index)), 'warning')
        if folder_name not in errors:
            errors[folder_name] = []

        errors[folder_name].append(df)


def get_errors(folder_name):
    if folder_name in errors:
        return pd.concat(errors[folder_name], ignore_index=True, sort=False).drop_duplicates().reset_index(drop=True)
    else:
        return pd.DataFrame()


def log_filepath():
    return os.path.join(settings['EventLogDir'],
                        "{} Event_Log.txt".format(datetime.datetime.now().__format__("%Y%m%d")))


def writelog(message, action='info'):
    filepath = log_filepath()

    logging.basicConfig(filename=filepath,
                        level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')

    print('{0} - {1} - {2}'.format(datetime.datetime.now(), action.upper(), message))

    if action == 'debug':
        logging.debug(message)
    elif action == 'info':
        logging.info(message)
    elif action == 'warning':
        logging.warning(message)
    elif action == 'error':
        logging.error(message)
    elif action == 'critical':
        logging.critical(message)


def getbatch(asdate=False, dayofweek=4, weekoffset=-1):
    current_time = datetime.datetime.now()
    batch = (current_time.date() - datetime.timedelta(days=current_time.weekday()) +
             datetime.timedelta(days=dayofweek, weeks=weekoffset))
    if asdate:
        return batch
    else:
        return batch.__format__("%Y%m%d")


def processresults(folder_name, asql, table, action):
    df_results = asql.query('''
            select
                *
            from {0}
        '''.format(table))

    success = df_results.loc[df_results['Error_Columns'].isnull()]

    if not success.empty:
        writelog("Completed {0} {1} action(s)"
                 .format(len(success.index), action), 'info')

    append_errors(folder_name, df_results.loc[df_results['Error_Columns'].notnull()])
    asql.execute("drop table {}".format(table))

    del df_results, success


def defaultheader(df, columns):
    if not df.empty and columns:
        columns = columns.replace(chr(10), '').replace(chr(32), '').split(',')

        for col in columns:
            if col not in df.columns.str.lower():
                df[col.title()] = None

        for col in df.columns:
            df[col] = df[col].str.strip()

        return df


def validatecol(asql, table, column, isdate=False, isbilldate=False):
    if isdate:
        asql.execute('''
            update A
                set
                    A.Error_Columns = '{0}',
                    A.Error_Message = '{0} is not a date'

            from {1} As A

            where
                A.Error_Columns is null
                    and
                A.{0} is not null
                    and
                isdate(A.{0}) != 1
        '''.format(column, table))

        if isbilldate:
            asql.execute('''
                update A
                    set
                        A.Error_Columns = '{0}',
                        A.Error_Message = '{0} is in the future'
    
                from {1} As A
    
                where
                    A.Error_Columns is null
                        and
                    A.{0} is not null
                        and
                    eomonth(dateadd(month, 1, getdate())) < eomonth(A.{0})
            '''.format(column, table))
        else:
            asql.execute('''
                update A
                    set
                        A.Error_Columns = '{0}',
                        A.Error_Message = '{0} is not the end of the month'
    
                from {1} As A
    
                where
                    A.Error_Columns is null
                        and
                    A.{0} is not null
                        and
                    A.{0} != eomonth(A.{0})
            '''.format(column, table))

            asql.execute('''
                update A
                    set
                        A.Error_Columns = '{0}',
                        A.Error_Message = '{0} is in the past'
    
                from {1} As A
    
                where
                    A.Error_Columns is null
                        and
                    A.{0} is not null
                        and
                    A.{0} < getdate()
            '''.format(column, table))
    else:
        asql.execute('''
            update A
                set
                    A.Error_Columns = '{0}',
                    A.Error_Message = '{0} is not numeric'
    
            from {1} As A
    
            where
                A.Error_Columns is null
                    and
                A.{0} is not null
                    and
                isnumeric(A.{0}) != 1
        '''.format(column, table))

        asql.execute('''
            update A
                set
                    A.Error_Columns = '{0}',
                    A.Error_Message = '{0} is <= 0'
    
            from {1} As A
    
            where
                A.Error_Columns is null
                    and
                A.{0} is not null
                    and
                cast(A.{0} as money) <= 0
        '''.format(column, table))


errors = dict()
settings = load_settings()
