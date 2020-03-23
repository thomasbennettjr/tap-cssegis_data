#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-cssegis_data",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Thomas Bennett tbennett@talend.com",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_cssegis_data"],
    install_requires=[
        "singer-python==5.2.1",
        "requests"
    ],
    entry_points="""
    [console_scripts]
    tap-cssegis_data=tap_cssegis_data:main
    """,
    packages=["tap_cssegis_data"],
    package_data = {
        "schemas": ["tap_cssegis_data/schemas/*.json"]
    },
    include_package_data=True,
)
