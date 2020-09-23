# coding=utf-8
"""Setup script for databaseops"""
#TODO: Change the structure of package

import os.path
from setuptools import setup, find_packages

# The directory containing this file
HERE = os.path.abspath(os.path.dirname(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md")) as fid:
	README = fid.read()

# This call to setup() does all the work
setup(
	name="databaseops",
	version="0.1.5",
	description="Work with Database as it is dataframe, No need to remember sql queries",
	long_description=README,
	long_description_content_type="text/markdown",
	url="https://github.com/AnkushBhise/databaseops/",
	author="Ankush Bhise",
	author_email="ankushbhise.18@gmail.com",
	license="MIT",
	classifiers=[
		"License :: OSI Approved :: MIT License",
		"Programming Language :: Python",
		"Programming Language :: Python :: 3",
	],
	packages=find_packages(),
	include_package_data=True,
	install_requires=[
		"pandas", "sqlalchemy", "pymysql"
	]
)
