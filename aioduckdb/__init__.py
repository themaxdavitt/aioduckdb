# Copyright 2022 Amethyst Reese
# Licensed under the MIT license

"""asyncio bridge to the standard sqlite3 module"""

from duckdb import (  # pylint: disable=redefined-builtin
    ConversionException,
    DataError,
    Error,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    paramstyle,
    ProgrammingError,
    InvalidInputException,
    Warning,
)

__author__ = ["Amethyst Reese", 'Salvador Pardiñas']
from .__version__ import __version__
from .core import connect, Connection, Cursor

__all__ = [
    "__version__",
    "paramstyle",
    "register_adapter",
    "register_converter",
    "sqlite_version",
    "sqlite_version_info",
    "connect",
    "Connection",
    "Cursor",
    "Row",
    "Warning",
    "Error",
    "DatabaseError",
    "IntegrityError",
    "ProgrammingError",
    "OperationalError",
    "NotSupportedError",
]
