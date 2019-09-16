import pyhdb
import pandas as pd
import json
import pyodbc as pdb

class ViewSchemaExporter:

    def __init__(self, sap_con_string, sql_con_string, sap_view):
        # '{"host":"example.com","port":30015,"user":"user","password":"secret"}'
        self.sap_con_string = sap_con_string
        self.sql_con_string = sql_con_string
        self.schema, self.view = sap_view.split('.')
        self.dtype_map = json.load(open('dtype_map.json'))

    def __str__(self):
        return f"\nSchema: {self.schema}\nView: {self.view}"

    def get_sql_connection(self) -> pdb.Connection:
        print('Connecting to DB')
        return pdb.connect(self.sql_con_string)

    def get_sap_connection(self):
        config = json.loads(self.sap_con_string)
        return pyhdb.connect(
            host=config['host'],
            port=int(config['port']),
            user=config['user'],
            password=config['password']
        )

    @staticmethod
    def create_query(df, tablename):
        return f"IF NOT EXISTS(  SELECT [name] FROM sys.tables WHERE [name] = '{tablename}' ) BEGIN CREATE TABLE [{tablename}] ({','.join(list('['+df['SQL_COLUMN']+'] '+df['SQL_DTYPE']))}) END"

    def get_view_metadata(self) -> pd.DataFrame:
        query = f"select COLUMN_NAME,DATA_TYPE_NAME,LENGTH from SYS.VIEW_COLUMNS where SCHEMA_NAME = '{self.schema}' and VIEW_NAME='{self.view}'"
        df = pd.read_sql(sql=query, con=self.get_sap_connection())
        df.set_index('COLUMN_NAME', inplace=True)
        assert not df.empty, "Metadata Empty"
        return df

    def replicate_sap_view(self):
        df = self.get_view_metadata()

        self.mapping = {x: x for x in list(df.index)}

        df['SQL_DTYPE'] = df['DATA_TYPE_NAME'].apply(lambda x: self.dtype_map[x])
        df['SQL_COLUMN'] = df.index

        con = self.get_sql_connection()
        cur = con.cursor()

        query = ViewSchemaExporter.create_query(df, self.view)
        cur.execute(query)
        cur.commit()
        return query

class TableSchemaExporter:

    def __init__(self, sap_con_string, sql_con_string, sap_table):
        # '{"host":"example.com","port":30015,"user":"user","password":"secret"}'
        self.sap_con_string = sap_con_string
        self.sql_con_string = sql_con_string
        self.schema, self.table = sap_table.split('.')
        self.dtype_map = json.load(open('dtype_map.json'))

    def __str__(self):
        return f"\nSchema: {self.schema}\ntable: {self.table}"

    def get_sql_connection(self) -> pdb.Connection:
        print('Connecting to DB')
        return pdb.connect(self.sql_con_string)

    def get_sap_connection(self):
        config = json.loads(self.sap_con_string)
        return pyhdb.connect(
            host=config['host'],
            port=int(config['port']),
            user=config['user'],
            password=config['password']
        )

    @staticmethod
    def create_query(df, tablename):
        return f"IF NOT EXISTS(  SELECT [name] FROM sys.tables WHERE [name] = '{tablename}' ) BEGIN CREATE TABLE [{tablename}] ({','.join(list('['+df['SQL_COLUMN']+'] '+df['SQL_DTYPE']))}) END"

    def get_table_metadata(self) -> pd.DataFrame:
        query = f"select COLUMN_NAME,DATA_TYPE_NAME,LENGTH from SYS.TABLE_COLUMNS where SCHEMA_NAME = '{self.schema}' and TABLE_NAME='{self.table}'"
        df = pd.read_sql(sql=query, con=self.get_sap_connection())
        df.set_index('COLUMN_NAME', inplace=True)
        assert not df.empty, "Metadata Empty"
        return df

    def replicate_sap_table(self):
        df = self.get_table_metadata()

        self.mapping = {x: x for x in list(df.index)}

        df['SQL_DTYPE'] = df['DATA_TYPE_NAME'].apply(lambda x: self.dtype_map[x])
        df['SQL_COLUMN'] = df.index

        con = self.get_sql_connection()
        cur = con.cursor()

        query = TableSchemaExporter.create_query(df, self.table)
        cur.execute(query)
        cur.commit()
        return query