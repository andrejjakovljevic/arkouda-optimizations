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
from arkouda.pdarraycreation import from_series

__all__ = ["count_frequencies", "move_records", "cumsum", "remove_duplicates", "make_from_csv"
           ]

def count_frequencies(a: pdarray, b: pdarray, n: int, l: list) -> int:
    cmd = "count_frequencies"
    cmd_args = "{} {} {} {}".format(a.name, b.name, n, l)
    generic_msg(cmd, cmd_args, return_value_needed=False,  my_pdarray=[a, b])

def move_records(a: pdarray, b: pdarray, c: pdarray, n: int, l: list) -> int:
    cmd = "move_records"
    cmd_args = "{} {} {} {} {}".format(a.name, b.name, c.name, n, l)
    generic_msg(cmd, cmd_args, return_value_needed=False, my_pdarray=[a, b, c])

def cumsum(a: pdarray):
    a.properties.clear()
    cmd = "cumsum"
    cmd_args = "{}".format(a.name)
    generic_msg(cmd, cmd_args, return_value_needed=False, my_pdarray=[a])

def remove_duplicates(a: pdarray):
    cmd = "remove_duplicates"
    cmd_args = "{}".format(a.name)
    arr = pdarray(cmd, cmd_args, a.dtype, 0, 1, [0], a.dtype.itemsize)
    repMsg = generic_msg(cmd, cmd_args, create_pdarray=True, return_value_needed=True, arr_id=arr.name, my_pdarray=[arr, a])
    fields = repMsg.split()
    size = int(fields[3])
    arr.size = size 
    arr.shape = [size]
    print("size=",size)
    return arr

def make_from_csv(fileName: str, list_of_converters):
    parse_dates_lst = ['tpep_pickup_datetime','tpep_dropoff_datetime']
    df = pd.read_csv(fileName,
                  converters=list_of_converters, header=0, low_memory=False,
                  parse_dates=parse_dates_lst, infer_datetime_format=True)
    akdict = {}
    for cname in df.keys():
        if df[cname].dtype.name == 'object':
            akdict[cname] = from_series(df[cname],dtype=np.str)
        else:
            akdict[cname] = from_series(df[cname])

    return akdict
