# coding=utf-8
"""Setup script for database_operations"""

import os.path
from setuptools import setup

# The directory containing this file
HERE = os.path.abspath(os.path.dirname(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md")) as fid:
    README = fid.read()

# This call to setup() does all the work
setup(
    name="database_operations",
    version="0.1.0",
    description="Work with Database as it is dataframe, No need to remember sql queries",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/AnkushBhise/database_operations/",
    author="Ankush Bhise",
    author_email="ankushbhise.18@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    packages=["mysql"],
    include_package_data=True,
    install_requires=[
        "pandas", "sqlalchemy", "pymysql"
    ]
)
