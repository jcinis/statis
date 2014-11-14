#!/usr/bin/env python
#from distutils.core import setup
from setuptools import setup, find_packages

setup(name="statis",
      version="0.0.1",
      description="Redis-based timeseries database",
      author="Jessey White-Cinis",
      author_email="j@cin.is",
      url="http://github.com/jcinis/statis",
      packages = find_packages(),
      license = "MIT License",
      install_requires=['simplejson>=2.1.6'],
      keywords="transloadit",
      zip_safe = True,
      tests_require=['nose', 'coverage'])
