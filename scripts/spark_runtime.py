"""Portable Java, Hadoop and Python settings for the supported Spark runtime."""
import atexit
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def configure_runtime():
    from portable_runtime import configure_runtime as discover
    return discover()
