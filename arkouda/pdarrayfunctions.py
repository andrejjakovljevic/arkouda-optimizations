import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import struct
from typing import cast, Iterable, Optional, Union
from typeguard import check_type, typechecked
from arkouda.client import generic_msg, id_to_args, args_to_id
from arkouda.dtypes import structDtypeCodes, NUMBER_FORMAT_STRINGS, float64 as akfloat64, int64 as akint64, bool as akbool, \
    DTypes, isSupportedInt, isSupportedNumber, NumericDTypes, SeriesDTypes, \
    int_scalars, numeric_scalars
from arkouda.dtypes import dtype as akdtype
from arkouda.pdarrayclass import pdarray, create_pdarray, check_arr, uncache_array, create_pdarray_with_name, parse_single_value
from arkouda.strings import Strings
from arkouda.pdarraycreation import array, from_series

__all__ = ["count_frequencies", "move_records", "cumsum", "remove_duplicates", "make_from_csv", "transpose", "triangle_count", "triangle_count_sparse",
           "vector_times_matrix", "inverse", "matrix_times_vector", "betwennessCentrality"]

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

def vector_times_matrix(n: int, pda1: pdarray, list_of_arrays: list):
    type1 = "int64"
    type2 = "int64"
    if (pda1.dtype==akfloat64):
        type1="float64"
    if (list_of_arrays[0].dtype==akfloat64):
        type2="float64"
    check_type = akint64
    if (type1=="float64" or type2=="float64"):
        check_type=akfloat64
    if (check_arr(check_type, pda1.size)):
        return vector_times_matrix_store(n, pda1, list_of_arrays, check_type)
    else:
        args = str(n)+" "+type1+" "+type2+" "+pda1.name
        for ar in list_of_arrays:
            args+=" "+ar.name
        cmd = "vector_times_matrix"
        res_type = akint64
        if (type1=="float64" or type2=="float64"):
            res_type= akfloat64
        arr = pdarray(cmd, args, res_type, pda1.size, pda1.ndim, pda1.shape, pda1.dtype.itemsize)
        repMsg = generic_msg(cmd, args, create_pdarray=True, arr_id=arr.name, my_pdarray=[list_of_arrays+[pda1, arr]])
        return arr

def vector_times_matrix_store(n: int, pda1: pdarray, list_of_arrays: list, check_type):
    name = uncache_array(check_type,pda1.size)
    type1 = "int64"
    type2 = "int64"
    if (pda1.dtype==akfloat64):
        type1="float64"
    if (list_of_arrays[0].dtype==akfloat64):
        type2="float64"
    type3 = "int64"
    if (check_type==akfloat64):
        type3="float64"
    cmd = "vector_times_matrix_store"
    res_type = akint64
    if (type1=="float64" or type2=="float64"):
        res_type= akfloat64
    arr = create_pdarray_with_name(name, cmd, "", res_type, pda1.size, pda1.ndim, pda1.shape, pda1.dtype.itemsize)
    args = str(n)+" "+type1+" "+type2+" "+type3+" "+pda1.name+" "+arr.name
    for ar in list_of_arrays:
        args+=" "+ar.name
    arr.cmd_args = args
    repMsg = generic_msg(cmd, args, create_pdarray=True, arr_id=arr.name, my_pdarray=[list_of_arrays+[pda1, arr]])
    return arr    

def matrix_times_vector(n: int, pda1: pdarray, list_of_arrays: list):
    type1 = "int64"
    type2 = "int64"
    if (pda1.dtype==akfloat64):
        type1="float64"
    if (list_of_arrays[0].dtype==akfloat64):
        type2="float64"
    check_type = akint64
    if (type1=="float64" or type2=="float64"):
        check_type=akfloat64
    if (check_arr(check_type, pda1.size)):
        return matrix_times_vector_store(n, pda1, list_of_arrays, check_type)
    else:
        args = str(n)+" "+type1+" "+type2+" "+pda1.name
        for ar in list_of_arrays:
            args+=" "+ar.name
        cmd = "matrix_times_vector"
        res_type = np.int64
        if (type1=="float64" or type2=="float64"):
            res_type=np.float64
        arr = pdarray(cmd, args, res_type, pda1.size, pda1.ndim, pda1.shape, pda1.dtype.itemsize)
        repMsg = generic_msg(cmd, args, create_pdarray=True, arr_id=arr.name, my_pdarray=[list_of_arrays+[pda1, arr]])
        return arr

def matrix_times_vector_store(n: int, pda1: pdarray, list_of_arrays: list, check_type):
    name = uncache_array(check_type, pda1.size)
    type1 = "int64"
    type2 = "int64"
    if (pda1.dtype==akfloat64):
        type1="float64"
    if (list_of_arrays[0].dtype==akfloat64):
        type2="float64"
    type3 = "int64"
    if (check_type==akfloat64):
        type3="float64"
    cmd = "matrix_times_vector_store"
    res_type = akint64
    if (type1=="float64" or type2=="float64"):
        res_type=akfloat64
    arr = create_pdarray_with_name(name, cmd, "", res_type, pda1.size, pda1.ndim, pda1.shape, pda1.dtype.itemsize)
    args = str(n)+" "+type1+" "+type2+" "+type3+" "+pda1.name+" "+arr.name
    for ar in list_of_arrays:
        args+=" "+ar.name
    arr.cmd_args = args
    repMsg = generic_msg(cmd, args, create_pdarray=True, arr_id=arr.name, my_pdarray=[list_of_arrays+[pda1, arr]])
    return arr


def inverse(pda1: pdarray):
    mytype = "int64"
    if (pda1.dtype==akfloat64):
        mytype= "float64"
    args = mytype+" "+pda1.name
    if (check_arr(pda1.dtype, pda1.size)):
        cmd = "inverse_vector_store"
        name =uncache_array(pda1.dtype, pda1.size)
        arr = create_pdarray_with_name(name, cmd, "", pda1.dtype, pda1.size, pda1.ndim, pda1.shape, pda1.dtype.itemsize)
        args += " "+arr.name
        generic_msg(cmd, args, arr_id=arr.name, my_pdarray=[pda1, arr])
        return arr
    else:
        cmd="inverse_vector"
        arr = pdarray(cmd, args, pda1.dtype, pda1.size, pda1.ndim, pda1.shape, pda1.dtype.itemsize)
        repMsg = generic_msg(cmd, args, create_pdarray=True,arr_id=arr.name, my_pdarray=[pda1, arr])
        return arr

def betwennessCentrality(source: int, list_of_arrays: list):
    args = str(len(list_of_arrays))+" "+str(source)
    for k in list_of_arrays:
        args+=" "+k.name
    cmd = "betwenness_centrality"
    pda1 = list_of_arrays[0]
    arr = pdarray(cmd, args, akfloat64, pda1.size, pda1.ndim, pda1.shape, akfloat64.itemsize)
    generic_msg(cmd, args, create_pdarray=True, arr_id=arr.name, my_pdarray=[list_of_arrays+[arr]])
    return arr

