import arkouda as ak
from scipy.sparse import csr_matrix, csc_matrix
import numpy as np
import time

def find_splice(k, pointers, indexes):
    left = pointers[k]
    if (k==len(pointers)-1):
        right = len(pointers)
    else:
        right = pointers[k+1]
    return indexes[left:right]

def get_matrices(filename):
    f = open(filename, "r")
    fs = []
    ss = []
    datas = []
    for x in f:
        if (x=="\n"):
            continue
        spl = x.split(' ')
        f = int(spl[0])-1
        s = int(spl[1])-1
        data = float(spl[2])
        if (s>f):
            (f, s) = (s, f)
        fs.append(f)
        ss.append(s)
        datas.append(data)
    s_mat = csr_matrix((datas,(fs, ss)), shape=(1138, 1138))
    s_mat_t = csc_matrix(s_mat)
    return (s_mat,s_mat_t)


ak.connect(connect_url='tcp://nlogin2:5555')
(s_mat, s_mat_t) = get_matrices("/home/an58/1138_bus.mtx")
dat_real = s_mat.data.astype(np.int64)
indexes = s_mat.indices.astype(np.int64)
pointers = s_mat.indptr.astype(np.int64)
d2 = s_mat_t.data.astype(np.int64)
indexes2 = s_mat_t.indices.astype(np.int64)
pointers2 = s_mat_t.indptr.astype(np.int64)
start = time.perf_counter()
pd_pointers = ak.array(pointers)
pd_pointers2 = ak.array(pointers2)
pd_indexes = ak.array(indexes)
pd_indexes2 = ak.array(indexes2)
k = 0
s = 0

#start = time.perf_counter()
#for i in range(len(pointers)-1):
#    right = pointers[i+1]
#    if (pointers[i]<right):
#        for j in range(pointers[i],right):
            #print(i," and ", pd_indexes[j])
#            s+= ak.intersect1d(find_splice(i, pointers, pd_indexes), find_splice(pd_indexes[j], pointers2, pd_indexes2), True).size

#print(s)
#end = time.perf_counter()
#print(f"first took {end - start:0.9f} seconds")
start = time.perf_counter()
#print(pd_pointers)
#print(pd_indexes)
#print(pd_pointers2)
#print(pd_indexes2)
s = ak.triangle_count_sparse(4, pd_pointers, pd_indexes, pd_pointers2, pd_indexes2)
print(s)
end = time.perf_counter()
print(f"second took {end - start:0.9f} seconds")
ak.shutdown()