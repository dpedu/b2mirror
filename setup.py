#!/usr/bin/env python3
from setuptools import setup
from b2mirror import __version__

setup(name='b2mirror',
      version=__version__,
      description='Tool for syc',
      url='http://gitlab.xmopx.net/dave/b2mirror',
      author='dpedu',
      author_email='dave@davepedu.com',
      packages=['b2mirror'],
      scripts=['bin/b2mirror'],
      zip_safe=False,
      install_requires=['b2==0.6.2']
      )
