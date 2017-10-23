#!/usr/bin/env python3

from setuptools import setup

setup(name='tap_retailnext',
      version="0.0.1",
      description='Singer.io tap for extracting data from the RetailNext',
      author='Blueocean Market Intelligence',
      url='',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_retailnext'],
      install_requires=[
          'singer-python==2.1.4',
          'pyRFC3339==1.0'
      ],
      entry_points='''
          [console_scripts]
          tap-retailnext=tap_retailnext:main
      ''',
      packages=['tap_retailnext'],
      include_package_data=True,
)
