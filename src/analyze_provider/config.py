"""API keys, paths, and constants."""

import os
from pathlib import Path

# BLS API key from environment
BLS_API_KEY: str | None = os.environ.get('BLS_API_KEY')

# Base paths (can be overridden by CLI)
DEFAULT_CACHE_DIR: Path = Path('./cache').resolve()
DEFAULT_OUTPUT_DIR: Path = Path('./output').resolve()

CACHE_DIR: Path = DEFAULT_CACHE_DIR
OUTPUT_DIR: Path = DEFAULT_OUTPUT_DIR

# Reference period bounds
START_MONTH: str = '2019-01'
END_MONTH: str = '2026-01'

# QCEW size class breakpoints (upper bounds of bins: 1-4, 5-9, 10-19, ...)
QCEW_SIZE_BREAKPOINTS: list[int] = [1, 5, 10, 20, 50, 100, 250, 500]
