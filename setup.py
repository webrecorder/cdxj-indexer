#!/usr/bin/env python
# vim: set sw=4 et:

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import glob

__version__ = "1.3.0"


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        # should work with setuptools <18, 18 18.5
        self.test_suite = " "

    def run_tests(self):
        import pytest
        import sys
        import os

        errcode = pytest.main(
            [
                "--doctest-modules",
                "./cdxj_indexer",
                "--cov",
                "cdxj_indexer",
                "-v",
                "test/",
            ]
        )
        sys.exit(errcode)


setup(
    name="cdxj_indexer",
    version=__version__,
    author="Ilya Kreymer",
    author_email="ikreymer@gmail.com",
    license="Apache 2.0",
    packages=find_packages(),
    url="https://github.com/webrecorder/cdxj-indexer",
    description="CDXJ Indexer for WARC and ARC files",
    long_description=open("README.rst").read(),
    provides=[
        "cdxj_indexer",
    ],
    install_requires=[
        "warcio",
        "surt",
        # temp fix for requests
        "idna<3.0",
    ],
    zip_safe=True,
    entry_points="""
        [console_scripts]
        cdxj-indexer=cdxj_indexer.main:main
    """,
    cmdclass={"test": PyTest},
    test_suite="",
    tests_require=[
        "pytest",
        "pytest-cov",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities",
    ],
)
