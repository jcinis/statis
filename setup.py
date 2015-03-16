#!/usr/bin/env python
#from distutils.core import setup
from setuptools import setup, find_packages

setup(name="statis",
      version="0.0.2",
      description="Flexible time-series stat tracking for redis",
      author="Jessey White-Cinis",
      author_email="j@cin.is",
      url="http://github.com/jcinis/statis",
      packages = find_packages(),
      license = "MIT License",
      install_requires=['redis>=2.10.2'],
      keywords="statis",
      zip_safe = True,
      tests_require=['nose', 'coverage'])
