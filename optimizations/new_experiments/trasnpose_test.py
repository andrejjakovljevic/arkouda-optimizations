import arkouda as ak
import math
import numpy as np
from scipy.sparse import csr_matrix
import time
import sys
import cProfile

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
    i=0
    shapes = -1
    for x in f:
        if (x[0]=='%'):
            continue
        if (i==0):
            spl = x.split(' ')
            shape_size = int(spl[0])
        if (x=="\n"):
            continue
        if (i>0):
            spl = x.split(' ')
            f = int(spl[0])-1
            s = int(spl[1])-1
            data = 1
            if (s>f):
                (f, s) = (s, f)
            fs.append(f)
            ss.append(s)
            datas.append(data)
        i+=1
    s_mat = csr_matrix((datas,(fs, ss)), shape=(shape_size, shape_size))
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

def to_profile(pd_out, pd_out_t):
    return triangle_count(pd_out, pd_out_t)

x = np.random.randint(2, size=(2, 2))
f = open("result+tr_base.txt", "a")
ak.connect(connect_url='tcp://bc9u19n6:5555')
out = create_blocks_scalar(x)
(s_mat, s_mat_t) = get_matrices("/home/an58/"+sys.argv[1]+".mtx")
dense1 = s_mat.todense().tolist()
dense2 = s_mat_t.todense().tolist()
pd_out = create_blocks_scalar(np.array(dense1,np.int64))
pd_out_t = create_blocks_scalar(np.array(dense2, np.int64))
start = time.perf_counter()
ak.startTracing()
cProfile.run('to_profile(pd_out, pd_out_t)')
ak.stopTracing()
end = time.perf_counter()
exec_time = end - start
print(f"transpose_v0 took {end - start:0.9f} seconds")
#f.write(sys.argv[1]+"\n")
#f.write(f"{exec_time:0.9f}\n")
#start = time.perf_counter()
#print(ak.triangle_count(pd_out))
#end = time.perf_counter()
#print(f"transpose_v1 took {end - start:0.9f} seconds")
ak.shutdown()
