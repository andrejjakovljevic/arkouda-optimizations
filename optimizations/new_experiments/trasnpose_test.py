import arkouda as ak
import math
import numpy as np
from scipy.sparse import csr_matrix
import time

def create_blocks_scalar(A: np.ndarray):
    out = []
    for k in A:
        out.append(ak.array(k))
    return out

def get_matrices(filename):
    f = open(filename, "r")
    fs = []
    ss = []
    datas = []
    for x in f:
        if (x=="\n"):
            continue
        spl = x.split(' ')
        f = int(spl[0])
        s = int(spl[1])
        data = float(spl[2])
        if (s>f):
            (f, s) = (s, f)
        fs.append(f)
        ss.append(s)
        datas.append(data)
    s_mat = csr_matrix((datas,(fs, ss)), shape=(4, 4))
    s_mat_t = s_mat.transpose(axes = None, copy=True)
    return (s_mat,s_mat_t)

def triangle_count(A: list, At: list) -> int:
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
ak.connect(connect_url='tcp://bc12u11n3:5555')
out = create_blocks_scalar(x)
(s_mat, s_mat_t) = get_matrices("/home/an58/help.mtx")
dense1 = s_mat.todense().tolist()
dense2 = s_mat_t.todense().tolist()
pd_out = create_blocks_scalar(np.array(dense1,np.int64))
pd_out_t = create_blocks_scalar(np.array(dense2, np.int64))
start = time.perf_counter()
print(triangle_count(pd_out, pd_out_t))
end = time.perf_counter()
start = time.perf_counter()
print(ak.triangle_count(pd_out))
end = time.perf_counter()
print(f"transpose_v1 took {end - start:0.9f} seconds")
ak.shutdown()