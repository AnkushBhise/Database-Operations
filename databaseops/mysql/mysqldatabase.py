# coding=utf-8
"""
This file is for mysql database connection for user to use database operations as dataframe operations
"""

import pymysql.connections
from ..helper import ListConversion


class MySQLDataBase(ListConversion):
    
    def __init__(self, host: str, user: str, password: str, db_name: str) -> None:
        """
        This class creates connection between python and MySQL database.
        If database exists name as db_name, it will create connection to that database.
        Otherwise it will create database name as db_name, will connect to that
        database. Object instance of this class will contain connection link and
        cursor link to given database.
        
        Parameters
        ----------
        host : Host name of MySQL database.
        
        user : User name of MySQL database.
        
        password : Password for above user name of MySQL database.
        
        db_name : Any MySQL database name from MySQL database, which you want
        connect.
        """
        self.db_name = db_name
        self.host = host
        self.user = user
        self.password = password
        self.__initialize_database()
    
    def __initial_conn_db(self, **kwargs: object) -> [pymysql.connections.Connection, pymysql.cursors.Cursor]:
        """
        This is privet method. Created for internal used only.
        
        Parameters
        ----------
        kwargs : Args like database will be passed.

        Returns
        -------
        pymysql.connections.Connection, pymysql.cursors.Cursor
            connection link and cursor link to given database, If database
            argument is not given connection link and cursor link entire MySQL database.
        """
        my_db = pymysql.connect(host=self.host, user=self.user, passwd=self.password, **kwargs)
        my_cursor = my_db.cursor()
        return my_db, my_cursor

    def __initialize_database(self) -> None:
        """
        This is privet method. Created for internal used only.
        This method create connection link and cursor link for given db_name inside
        object instance
        
        Returns
        -------
        None
            This methods returns nothing
        """
        db, cursor = self.__initial_conn_db()
        cursor.execute("Show Databases")
        if self.db_name.lower() not in self.list_of_tuple_to_list([i for i in cursor]):
            cursor.execute(f"Create Database {self.db_name}")
        self.my_db, self.my_cursor = self.__initial_conn_db(database=self.db_name)