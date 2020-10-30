# coding=utf-8
"""
This file is for mysql database connection for user to use database operations as dataframe operations
"""

import pandas
import sqlalchemy
import pymysql.connections
import warnings
from .mysqldatabase import MySQLDataBase


class MySQLTable(MySQLDataBase):
    """
    TODO: Make everything multiprocess as well as sequential for debug propose

        :param host:
        :type host:
        :param user:
        :type user:
        :param password:
        :type password:
    """
    
    def __init__(self, host: str, user: str, password: str, db_name: str, table_name: str) -> None:
        if not hasattr(self, "host") or not hasattr(self, "user") or not hasattr(self, "password") or not hasattr(
                self, "db_name"):
            MySQLDataBase.__init__(self, host, user, password, db_name)
        self.table_name = table_name
        self.__sqlalchemy()
    
    @staticmethod
    def __update_insertion_method(meta):
        """

        :param meta:
        :return:
        """
        
        def method(table, conn, keys, data_iter):
            sql_table = sqlalchemy.Table(table.name, meta, autoload=True)
            insert_stmt = sqlalchemy.dialects.mysql.insert(sql_table).values(
                [dict(zip(keys, data)) for data in data_iter])
            upsert_stmt = insert_stmt.on_duplicate_key_update({x.name: x for x in insert_stmt.inserted})
            conn.execute(upsert_stmt)
        
        return method
    
    def __sqlalchemy(self) -> None:
        """
        
        :return:
        """
        self.sqlalchemy_engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.db_name}", isolation_level="AUTOCOMMIT")
        self.conn = self.sqlalchemy_engine.connect()
    
    def populate_table(self, dataframe: pandas.DataFrame, if_exists: str = 'append') -> None:
        """

        :param if_exists:
        :param dataframe:
        :type dataframe:
        """
        dataframe.to_sql(name=self.table_name, con=self.sqlalchemy_engine, if_exists=if_exists, method=None)
    
    def update_table(self, dataframe: pandas.DataFrame, if_exists: str = 'append') -> None:
        """

        :param if_exists:
        :param table_name:
        :param dataframe:
        :return:
        """
        with self.conn.begin():
            meta = sqlalchemy.MetaData(self.conn)
        dataframe.to_sql(name=self.table_name, con=self.sqlalchemy_engine, if_exists=if_exists,
                         method=self.__update_insertion_method(meta))
    
    def get_data_type(self) -> dict:
        """

        :return:
        :rtype:
        """
        self.my_cursor.execute(f"Show fields from {self.table_name}")
        return {i[0]: i[1] for i in self.my_cursor}
    
    def remove_duplicates(self, list_of_columns: list) -> None:
        """

        :param list_of_columns:
        :type list_of_columns:
        """
        warnings.warn(f"Removing duplicate entries from columns {','.join(list_of_columns)}", stacklevel=2)
        for col in list_of_columns:
            self.my_cursor.execute(f"CREATE TABLE copy_of_source_{self.table_name} "
                                   f"SELECT * FROM {self.table_name} GROUP BY({col})")
            self.my_cursor.execute(f"DROP TABLE {self.table_name}")
            self.my_cursor.execute(f"ALTER TABLE copy_of_source_{self.table_name} RENAME TO {self.table_name}")
    
    def set_primary_key(self, column_name: str or list, remove_duplicates=True) -> None:
        """

        :param column_name:
        :type column_name:
        :param remove_duplicates:
        :type remove_duplicates:
        """
        if isinstance(column_name, str):
            column_name = [column_name]
        self.primary_key_columns = ','.join(column_name)
        if remove_duplicates:
            self.remove_duplicates(column_name)
        database_dtype = self.get_data_type()
        columns = [f'{i}(255)' if 'text' in database_dtype[i] else i for i in column_name]
        try:
            self.my_cursor.execute(f"ALTER TABLE {self.table_name} ADD PRIMARY KEY ({','.join(columns)})")
        except pymysql.err.IntegrityError:
            raise UserWarning(f"Duplicate entries in column {','.join(columns)}, "
                              f"remove_duplicates attribute should be true in case of duplicates")
    
    def set_unique_keys(self, column_name: str or list, remove_duplicates=True) -> None:
        """

        :param column_name:
        :type column_name:
        :param remove_duplicates:
        :type remove_duplicates:
        """
        if isinstance(column_name, str):
            column_name = [column_name]
        self.unique_column = ','.join(column_name)
        if remove_duplicates:
            self.remove_duplicates(column_name)
        database_dtype = self.get_data_type()
        columns = [f'{i}(255)' if 'text' in database_dtype[i] else i for i in column_name]
        try:
            self.my_cursor.execute(f"ALTER TABLE {self.table_name} ADD unique ({','.join(columns)})")
        except pymysql.err.IntegrityError:
            raise UserWarning(f"Duplicate entries in column {','.join(columns)},"
                              f" remove_duplicates attribute should be true in case of duplicates")
    
    def sort_table(self, column: str or dict, order="ascending" or "descending") -> None:
        if isinstance(column, str):
            query = f"SELECT * FROM {self.table_name} ORDER BY {column} {order}"
        elif isinstance(column, dict):
            query = f"SELECT * FROM {self.table_name} ORDER BY "
            col_and_order = [[col, c_ord] for col, c_ord in column.items()]
            col_and_order_str = " ,"
            for i in col_and_order:
                col_and_order_str = col_and_order_str.join(i)
            query = query + col_and_order_str
        self.my_cursor.execute(query=query)
    
    def table_filter(self, where: list, select: str or list = None, limit: int = None,
                     chunksize: int = None) -> pandas.DataFrame:
        """
        :return: pandas dataframe
        """
        if select:
            if isinstance(select, list):
                query = "SELECT " + " ,".join(select)
            elif isinstance(select, str):
                query = "SELECT " + select
        else:
            query = "SELECT *"
        query = query + f" FROM {self.table_name}" + " where " + " ,".join(where)
        if limit:
            query = query + f" LIMIT {limit}"
        return pandas.read_sql_query(sql=query, con=self.sqlalchemy_engine, chunksize=chunksize)
    
    def read_table(self, chunksize: int = None):
        """
        TODO: read table from database with column names and without column names (All Columns)
        use yield to achieve iteration over object and commit changes into same table or create new table
        :return: pandas dataframe, if chuck size is given databaseops.MySQL object
        """
        return pandas.read_sql_table(table_name=self.table_name, con=self.sqlalchemy_engine, chunksize=chunksize)
    
    def commit(self):
        """
        TODO: Commit changes to database table
        :return: None
        """
    
    def where(self):
        """
        TODO: create where function with multiple value serach and use dictiory to achive

        """
    
    def apply_on_table(self, function):
        """
        TODO: Create function which will take function as input and perform operations on chunks of data

        :param function:
        :type function:
        """
