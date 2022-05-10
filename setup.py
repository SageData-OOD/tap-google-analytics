#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-google-analytics",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_google_analytics"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.12.2",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-google-analytics=tap_google_analytics:main
    """,
    packages=["tap_google_analytics"],
    package_data = {
        "schemas": ["tap_google_analytics/schemas/*.json"]
    },
    include_package_data=True,
)
