# coding=utf-8
import itertools


class ListConversion:
	"""
	This class is to convert one dtype to other dtype
	"""
	@staticmethod
	def list_of_tuple_to_list(list_of_tuple):
		"""

		:param list_of_tuple: list_of_tuple
		:type list_of_tuple: list
		:return: list
		:rtype: list
		"""
		return list(itertools.chain(*list_of_tuple))
