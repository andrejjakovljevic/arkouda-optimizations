from sqlalchemy.sql.functions import concat

import arkouda as ak
import math
import numpy as np
import time

def create_blocks_scalar(A: np.ndarray):
    k1 = np.array([0, 0, 0, 0])
    k2 = np.array([1, 0, 0, 0])
    k3 = np.array([1, 1, 0, 0])
    k4 = np.array([0, 1, 1, 0])
    out = []
    out.append(ak.array(k1))
    out.append(ak.array(k2))
    out.append(ak.array(k3))
    out.append(ak.array(k4))
    return out

def transposed(A: np.ndarray):
    k1 = np.array([0, 1, 1, 0])
    k2 = np.array([0, 0, 1, 1])
    k3 = np.array([0, 0, 0, 1])
    k4 = np.array([0, 0, 0, 0])
    out = []
    out.append(ak.array(k1))
    out.append(ak.array(k2))
    out.append(ak.array(k3))
    out.append(ak.array(k4))
    return out

def triangle_count(A: list) -> int:
    At = transposed(A)
    maxi = 0
    arr = np.zeros(len(A),np.int64)
    for i in range(len(A)):
        for j in range(len(A)):
            k = ak.sum(A[i]*At[j])
            arr[j] = k
        pdarr = ak.array(arr)
        maxi += ak.sum(pdarr*A[i])
    return maxi

def triangle_count_on_chapel(A: list) -> int:
    return ak.triangle_count(A)

x = np.random.randint(2, size=(2, 2))
ak.connect(connect_url='tcp://nlogin2:5555')
out = create_blocks_scalar(x)
start = time.perf_counter()
print(triangle_count(out))
end = time.perf_counter()
print(f"union_v1 took {end - start:0.9f} seconds")
ak.shutdown()