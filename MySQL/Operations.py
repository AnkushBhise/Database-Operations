# coding=utf-8
from Utils.data_structure_conversion import ListConversion
import pandas
import sqlalchemy
import pymysql.connections
import warnings


class DataBaseOperation(ListConversion):
	"""
	TODO: Make everything multiprocess as well as sequential for debug propose
	
		:param host:
		:type host:
		:param user:
		:type user:
		:param password:
		:type password:
	"""
	
	def __init__(self, host: str, user: str, password: str) -> None:
		self.host = host
		self.user = user
		self.password = password
	
	def __initial_conn_db(self, **kwargs: object) -> [pymysql.connections.Connection, pymysql.cursors.Cursor]:
		"""

		:param kwargs:
		:type kwargs:
		:return:
		:rtype:
		"""
		my_db = pymysql.connect(
			host=self.host,
			user=self.user,
			passwd=self.password,
			**kwargs
		)
		my_cursor = my_db.cursor()
		return my_db, my_cursor
	
	def initialize_database(self, db_name: str) -> None:
		"""

		:param db_name:
		:type db_name:
		"""
		self.db_name = db_name
		db, cursor = self.__initial_conn_db()
		cursor.execute("Show Databases")
		if self.db_name.lower() not in self.list_of_tuple_to_list([i for i in cursor]):
			cursor.execute(f"Create Database {self.db_name}")
		(self.my_db, self.my_cursor) = self.__initial_conn_db(database=self.db_name)
	
	def populate_table(self, table_name: str, dataframe: pandas.DataFrame) -> None:
		"""

		:param dataframe:
		:type dataframe:
		:param table_name:
		:type table_name:
		"""
		self.table_name = table_name
		sqlalchemy_engine = sqlalchemy.create_engine(
			f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.db_name}",
			isolation_level="AUTOCOMMIT")
		dataframe.to_sql(name=self.table_name, con=sqlalchemy_engine, if_exists='append', method='multi')
	
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
			raise UserWarning(f"Duplicate entries in column {','.join(columns)}, remove_duplicates attribute should "
			                  f"be true in case of duplicates")
	
	def where(self):
		"""
		TODO: create where function with multiple value serach and use dictiory to achive

		"""
	
	def apply_on_table(self, function):
		"""
		TODO: Create function which will take function as input and perform operati9ons on chunks of data

		:param function:
		:type function:
		"""
	
	def __read_table_chunks(self):
		"""
		
		:return:
		:rtype:
		"""
	def join_table(self):
		"""
		TODO: create join table with foreign key
		use yield to achieve iteration over object and commit changes into same table or create new table

		"""
	def read_table(self):
		"""
		TODO: read table from database with column names and without column names (All Columns)
		use yield to achieve iteration over object and commit changes into same table or create new table

		"""
