import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import struct
from typing import cast, Iterable, Optional, Union
from typeguard import typechecked
from arkouda.client import generic_msg, id_to_args, args_to_id
from arkouda.dtypes import structDtypeCodes, NUMBER_FORMAT_STRINGS, float64, int64, \
    DTypes, isSupportedInt, isSupportedNumber, NumericDTypes, SeriesDTypes, \
    int_scalars, numeric_scalars
from arkouda.dtypes import dtype as akdtype
from arkouda.pdarrayclass import pdarray, create_pdarray, check_arr, uncache_array, create_pdarray_with_name, parse_single_value
from arkouda.strings import Strings

__all__ = ["count_frequencies", "move_records", "cumsum"
           ]

def count_frequencies(a: pdarray, b: pdarray, n: int, l: list) -> int:
    cmd = "count_frequencies"
    cmd_args = "{} {} {} {}".format(a.name, b.name, n, l)
    generic_msg(cmd, cmd_args, return_value_needed=False, my_pdarray=[a, b])

def move_records(a: pdarray, b: pdarray, c: pdarray, n: int, l: list) -> int:
    cmd = "move_records"
    cmd_args = "{} {} {} {} {}".format(a.name, b.name, c.name, n, l)
    generic_msg(cmd, cmd_args, return_value_needed=False, my_pdarray=[a, b, c])

def cumsum(a: pdarray):
    cmd = "cumsum"
    cmd_args = "{}".format(a.name)
    generic_msg(cmd, cmd_args, my_pdarray=[a])