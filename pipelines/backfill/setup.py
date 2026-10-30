"""Beam stages this small package for the historical Dataflow worker helpers."""

import os
from pathlib import Path

from setuptools import setup

os.chdir(Path(__file__).resolve().parent)
setup(name='cocoa-backfill-recovery', version='1.1.0', py_modules=['backfill_recovery'],
      python_requires='>=3.11,<3.12')
