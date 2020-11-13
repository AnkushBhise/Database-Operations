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
    The object instance of this class will able to perform multiple operations
    on database table like read, filter, sort, remove_duplicates, update etc.

    Parameters
    ----------
    host : str
        Host name of MySQL database.

    user : str
        User name of MySQL database.

    password : str
        Password for above user name of MySQL database.

    db_name : str
        Any MySQL database name from MySQL database, which you want connect.

    table_name : str
        Table name from above MySQL database, which you want connect.
    """
    
    def __init__(self, host: str, user: str, password: str, db_name: str, table_name: str) -> None:
        if not hasattr(self, "host") or not hasattr(self, "user") or not hasattr(self, "password") or not hasattr(
                self, "db_name"):
            MySQLDataBase.__init__(self, host, user, password, db_name)
        self.table_name = table_name
        self.__sqlalchemy()
    
    @staticmethod
    def __update_insertion_method(meta: sqlalchemy.MetaData):
        """
        This is privet method. Created for internal used only.
        
        Parameters
        ----------
        meta : sqlalchemy.MetaData
            This is the metadata for MySQL database table.

        Returns
        -------
        Method
            It returns the method which will be used into update_table.
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
        This is privet method. Created for internal used only.
        
        Returns
        -------
        None
            But create sqlalchemy_engine.connect inside object instance.
        """
        self.sqlalchemy_engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.db_name}", isolation_level="AUTOCOMMIT")
        self.conn = self.sqlalchemy_engine.connect()
    
    def populate_table(self, dataframe: pandas.DataFrame, if_exists: str = 'append') -> None:
        """
        This method append dataframe data to MySQL table.
        
        Note
        ----
            if_exists : This parameter is about table, Don't confuse it about
            data inside table.
        
        Example
        -------
        This will add the data to database table.
        
        ``self.populate_table(dataframe = Data)``
        
        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data which need to added to table.
        
        if_exists : str
            This parameter will decide what to do if table name
            table_name already exists. Options are 'fail', 'replace' and 'append'.

        Returns
        -------
        None
            It returns nothing.
        """
        dataframe.to_sql(name=self.table_name, con=self.sqlalchemy_engine, if_exists=if_exists, method=None)
    
    def update_table(self, dataframe: pandas.DataFrame, if_exists: str = 'append') -> None:
        """
        This method will replace data in table if it already exist based upon
        duplicate values in primary key of table.
        
        Note
        ----
            It will directly affect the source table with no way of going
            back previous point.
        
            if_exists : This parameter is about table, Don't confuse it about
            data inside table.
        
        Example
        -------
        This will update the data on primary key in database table.
        
        ``self.update_table(dataframe = Data)``
        
        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data which need to added to table.
        
        if_exists : str
            This parameter will decide what to do if table_name already
            exists. Options are 'fail', 'replace' and 'append'.

        Returns
        -------
        None
            This returns nothing
        """
        with self.conn.begin():
            meta = sqlalchemy.MetaData(self.conn)
        dataframe.to_sql(name=self.table_name, con=self.sqlalchemy_engine, if_exists=if_exists,
                         method=self.__update_insertion_method(meta))
    
    def get_data_type(self) -> dict:
        """
        Use this method to get column name and there data type for entire table.
        
        Example
        -------
        This will column name and there data type of table
        
        ``self.get_data_type()``
        
        Returns
        -------
        dict
            Dictionary with column name as key and data type as value.
        """
        self.my_cursor.execute(f"Show fields from {self.table_name}")
        return {i[0]: i[1] for i in self.my_cursor}
    
    def remove_duplicates(self, list_of_columns: list) -> None:
        """
        This method will delete duplicates rows from table based upon give list
        of columns.
        
        Note
        ----
            It will directly affect the source table with no way of going
            back previous point.
            
        Example
        -------
        This will remove duplicate values for list_of_columns
        
        ``self.remove_duplicates(list_of_columns = [column_1, column_2])``
        
        Parameters
        ----------
        list_of_columns : list
            list of columns names which should be used for
            removing duplicate values.

        Returns
        -------
        None
            This returns nothing
        """
        warnings.warn(f"Removing duplicate entries from columns {','.join(list_of_columns)}", stacklevel=2)
        for col in list_of_columns:
            self.my_cursor.execute(f"CREATE TABLE copy_of_source_{self.table_name} "
                                   f"SELECT * FROM {self.table_name} GROUP BY({col})")
            self.my_cursor.execute(f"DROP TABLE {self.table_name}")
            self.my_cursor.execute(f"ALTER TABLE copy_of_source_{self.table_name} RENAME TO {self.table_name}")
    
    def set_primary_key(self, column_name: str or list, remove_duplicates=True) -> None:
        """
        This method will set column_name as primary key for table.
        
        Note
        ----
            By default it remove duplicate value for column_name.
        
        Example
        -------
        This will create composite primary key with columns_name. But if it
        contains duplicate values currently, it will remove duplicate by default.
        If you don't want remove duplicates, pass remove_duplicates=False. But
        in that case if columns have duplicate values, method will fail and will
        raise exception.
        
        ``self.set_primary_key(column_name = ["column_1", "column_2"])``
        
        Parameters
        ----------
        column_name : str or list
            If str, then it will set as primary key. If list, then
            composite primary key will get created.
        
        remove_duplicates : bool
            Either you want delete duplicate value for given
            columns or not. Pass this as False if you don't to remove duplicate on given
            columns, But it will raise exception and primary key will not be set. As
            primary key need to have unique values.

        Returns
        -------
        None
            This returns nothing
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
        This method will set columns to contain only unique values.
        
        Note
        ----
            This method will directly affect source table.
        
        Example
        -------
        This will set columns to have unique values in future. But if it contains
        duplicate values currently, it will fail and raise exception. If you want
        to set columns to have unique values, keep remove_duplicates argument to
        True or don't pass it at all.
        
        ``self.set_unique_keys(column_name = ["column_1", "column_2"], remove_duplicates=False)``
        
        Parameters
        ----------
        column_name : str or list
            Column names from database table which should have unique
            values.
        
        remove_duplicates: bool
            Either you want delete duplicate value for given
            columns or not. Pass this as False, if you don't to remove duplicate on given
            columns, But it will raise exception.

        Returns
        -------
        None
            This returns nothing
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
    
    def sort_table(self, column: str or dict, order="ascending") -> None:
        """
        This method will perform sort operation on source database table.
        
        Note
        ----
            This method will directly affect source table.
        
        
        Example
        -------
        ``self.sort_table(column = "column_1", order="descending")``
        
        Parameters
        ----------
        column : str or dict
            Columns will used for sorting operation.
        
        order : str
            Order of sorting, Which can "ascending" or "descending"

        Returns
        -------
        None
            This returns nothing
        """
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
    
    def table_filter(self, where: list, select: str or list = None,
                     limit: int = None, chunksize: int = None) -> pandas.DataFrame:
        """
        If you want read filtered table use this method.
        
        Example
        -------
        This will return object filtered on where argument and chunksize of 100
        
        ``self.table_filter(where = ["column_Name = value"], select = None, limit = None, chunksize = 100)``
        
        This are examples with multiple where condition.
        
        ``self.table_filter(where = ["column_Name = Value and  column_Name > Value"], chunksize = 100)``
                    
        ``self.table_filter(where = ["column_Name = Value or  column_Name > Value"], chunksize = 100)``
        
        Parameters
        ----------
        where : list
            list of conditions in string format.
        
        select : str
            Will only return data for these columns. If None, will
            return complete data.
        
        limit : int
            Number of rows.
        
        chunksize : int
            Number of rows in one iteration.

        Returns
        -------
        pandas.DataFrame or Object
            pandas.DataFrame if chunksize is None else iterable object.
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
        This method will read table as pandas.DataFrame if chunksize is not given else
        it will return object which can be iterated in for loop every loop will
        given pandas.DataFrame of chunksize.
        
        Example
        -------
        This will return object filtered on where argument and chunksize of 100
        
        ``self.read_table(where = ["column_Name = value"], select = None, limit = None, chunksize = 100)``
        
        Parameters
        ----------
        chunksize : int
            number of row you want read at one time.

        Returns
        -------
        pandas.DataFrame or Iterable object
            pandas.DataFrame or Iterable object which will give pandas.DataFrame
        """
        return pandas.read_sql_table(table_name=self.table_name, con=self.sqlalchemy_engine, chunksize=chunksize)
