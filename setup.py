from setuptools import setup, find_packages
import os

version = '0.0.1'

setup(name='sparc.db_splunk',
      version=version,
      description="Splunk database interaction utilities for the SPARC platform",
      long_description=open("README.md").read() + "\n" +
                       open("HISTORY.txt").read(),
      # Get more strings from
      # http://pypi.python.org/pypi?:action=list_classifiers
      classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
      ],
      keywords=['zca'],
      author='David Davis',
      author_email='davisd50@gmail.com',
      url='https://github.com/davisd50/sparc.db_splunk',
      download_url = '',
      license='MIT',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['sparc'],
      include_package_data=True,
      package_data = {
          '': ['*.zcml']
        },
      zip_safe=False,
      install_requires=[
          'setuptools',
          'six',
          'zope.component',
          'zope.interface',
          'splunk-sdk', #dependant on Python 2 as of version 1.6.2
          'sparc.db',
          'sparc.utils'
          # -*- Extra requirements: -*-
      ],
      tests_require=[
          'sparc.testing'
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
