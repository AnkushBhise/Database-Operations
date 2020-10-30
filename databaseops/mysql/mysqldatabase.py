# coding=utf-8
"""
This file is for mysql database connection for user to use database operations as dataframe operations
"""

import pymysql.connections
from databaseops.helper import ListConversion


class MySQLDataBase(ListConversion):
    
    def __init__(self, host: str, user: str, password: str, db_name: str) -> None:
        self.db_name = db_name
        self.host = host
        self.user = user
        self.password = password
        self.__initialize_database()
    
    def __initial_conn_db(self, **kwargs: object) -> [pymysql.connections.Connection, pymysql.cursors.Cursor]:
        """

        :param kwargs:
        :type kwargs:
        :return:
        :rtype:
        """
        my_db = pymysql.connect(host=self.host, user=self.user, passwd=self.password, **kwargs)
        my_cursor = my_db.cursor()
        return my_db, my_cursor

    def __initialize_database(self) -> None:
        """

        :param db_name:
        :type db_name:
        """
        db, cursor = self.__initial_conn_db()
        cursor.execute("Show Databases")
        if self.db_name.lower() not in self.list_of_tuple_to_list([i for i in cursor]):
            cursor.execute(f"Create Database {self.db_name}")
        self.my_db, self.my_cursor = self.__initial_conn_db(database=self.db_name)