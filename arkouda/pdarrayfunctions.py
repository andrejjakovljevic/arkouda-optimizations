import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import struct
from typing import cast, Iterable, Optional, Union
from typeguard import typechecked
from arkouda.client import generic_msg, id_to_args, args_to_id
from arkouda.dtypes import structDtypeCodes, NUMBER_FORMAT_STRINGS, float64 as akint64, int64 as akfloat64, bool as akbool, \
    DTypes, isSupportedInt, isSupportedNumber, NumericDTypes, SeriesDTypes, \
    int_scalars, numeric_scalars
from arkouda.dtypes import dtype as akdtype
from arkouda.pdarrayclass import pdarray, create_pdarray, check_arr, uncache_array, create_pdarray_with_name, parse_single_value
from arkouda.strings import Strings
from arkouda.pdarraycreation import from_series

__all__ = ["count_frequencies", "move_records", "cumsum", "remove_duplicates", "make_from_csv", "transpose", "triangle_count", "triangle_count_sparse"
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
    return arr

def make_from_csv(fileName: str, listOfTypes, ns: list):
    file = open(fileName)
    s = file.read()
    file.close()
    pdarrays = []
    for i in range(len(ns)):
        cmd = "get_from_csv"
        cmd_args = "{}\n{}\n{}".format(s, listOfTypes, ns[i])
        test = cmd_args.split("\n")[-1]
        myType = akint64
        if (listOfTypes[ns[i]]=='float64'):
            myType = akfloat64
        elif listOfTypes[ns[i]]=='date':
            myType = akint64
        else:
            myType = akbool
        arr = pdarray(cmd, cmd_args, myType, 0, 1, [0], myType.itemsize)
        repMsg = generic_msg(cmd, cmd_args, create_pdarray=True, return_value_needed=True, arr_id = arr.name, my_pdarray=[arr])
        fields = repMsg.split()
        name = fields[1]
        mydtype = fields[2]
        size = int(fields[3])
        arr.size = size
        arr.shape = [size]
        pdarrays.append(arr)
    return pdarrays

def transpose(listOfPdarrays: list) -> list:
    n = len(listOfPdarrays)
    args = str(n)
    for p in listOfPdarrays:
        args +=" "+p.name
    ret = []
    ret_names = []
    cmd = "transpose"
    for i in range(n):
        arr = pdarray(cmd, args, listOfPdarrays[0].dtype, listOfPdarrays[0].size, listOfPdarrays[0].ndim, listOfPdarrays[0].shape, listOfPdarrays[0].itemsize)
        ret.append(arr)
        ret_names.append(arr.name)
    repMsg = generic_msg(cmd, args, create_pdarray=True, arr_id=ret_names, my_pdarray=listOfPdarrays+ret)
    return ret

def triangle_count(listOfPrdarrays: list) -> int:
    n = len(listOfPrdarrays)
    args = str(n)
    for p in listOfPrdarrays:
        args +=" "+p.name
    cmd = "triangle_count"
    repMsg = generic_msg(cmd, args, return_value_needed=True, my_pdarray=listOfPrdarrays)
    k = repMsg.split(' ')[1]
    return int(k)

def triangle_count_sparse(n: int, pda1: pdarray, pda2: pdarray, pda3: pdarray, pda4: pdarray) -> int:
    args = str(n)+" "+pda1.name+" "+pda2.name+" "+pda3.name+" "+pda4.name
    cmd = "triangle_count_sparse"
    repMsg = generic_msg(cmd, args, return_value_needed=True, my_pdarray=[pda1, pda2, pda3, pda4])
    k = repMsg.split(' ')[1]
    return int(k)