#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-wrike',
    version="0.0.1",
    description='Singer.io tap for extracting data from the Wrike API',
    author='Ognyana Ivanova',
    url='http://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_wrike'],
    install_requires=[
       "singer-python",
       'requests',
       'backoff'
   ],
    entry_points='''
       [console_scripts]
       tap-wrike=tap_wrike:main
    ''',
    packages=['tap_wrike'],
    package_data = {
        "schemas": ["tap_wrike/schemas/*.json"]
    },
    include_package_data=True
)