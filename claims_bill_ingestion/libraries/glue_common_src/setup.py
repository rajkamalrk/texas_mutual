#!/usr/bin/env python3
"""
Setup script for glue_commons package
"""
from setuptools import setup, find_packages

# Package metadata
name = 'glue_commons'
version = '0.1.0'
description = 'Common utilities for AWS Glue jobs in claims bill ingestion'

# Package setup
setup(
    name=name,
    version=version,
    description=description
)