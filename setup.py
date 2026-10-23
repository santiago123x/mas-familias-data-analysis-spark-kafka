"""Compatibility shim for older pip versions (e.g. pip 22.0 on Ubuntu 22.04).

All metadata lives in pyproject.toml. This file only enables
`pip install -e .` when pip falls back to the legacy editable mode.
"""

from setuptools import setup

setup()
