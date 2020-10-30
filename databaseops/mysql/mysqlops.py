# coding=utf-8
"""
This file is for mysql database connection for user to use database operations as dataframe operations
"""

import pandas
import sqlalchemy
import pymysql.connections
import warnings
from .mysqltable import MySQLTable
from .mysqldatabase import MySQLDataBase
from databaseops.helper import ListConversion


class MySQLOps(MySQLTable, ListConversion):
    """
    TODO: Make everything multiprocess as well as sequential for debug propose
    
        :param host:
        :type host:
        :param user:
        :type user:
        :param password:
        :type password:
    """
    
    def __init__(self, host: str, user: str, password: str, db_name: str) -> None:
        MySQLDataBase.__init__(self, host=host, user=user, password=password, db_name=db_name)
    
    def join_table(self):
        """
        TODO: create join table with foreign key
        use yield to achieve iteration over object and commit changes into same table or create new table
        :return: pandas dataframe if chuck size is given, databaseops.MySQL object
        """
