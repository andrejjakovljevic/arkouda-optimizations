import arkouda as ak
from scipy.sparse import csr_matrix, csc_matrix
import numpy as np
import time
import sys

def find_splice(k, pointers, indexes):
    left = pointers[k]
    right = pointers[k+1]
    return indexes[left:right]

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
            #print("i=",i)
        i+=1
    s_mat = csr_matrix((datas,(fs, ss)), shape=(shape_size, shape_size))
    s_mat_t = csc_matrix(s_mat)
    return (s_mat,s_mat_t)

def half_of_tri_count(n, pointers, pointers2, pd_indexes, pd_indexes2):
    s=0
    pd_pointers = ak.array(pointers)
    pd_pointers2 = ak.array(pointers2)
    for i in range(len(pointers)-1):
        right = pointers[i+1]
        if (pointers[i]<right):
            s += ak.half_of_triangle_count(n, i, pd_pointers, pd_pointers2, pd_indexes, pd_indexes2)
    return s

#open and read the file after the appending:
f = open("result.txt", "a")
(s_mat, s_mat_t) = get_matrices("/home/an58/"+sys.argv[1]+".mtx")
dat_real = s_mat.data.astype(np.int64)
indexes = s_mat.indices.astype(np.int64)
pointers = s_mat.indptr.astype(np.int64)
d2 = s_mat_t.data.astype(np.int64)
indexes2 = s_mat_t.indices.astype(np.int64)
pointers2 = s_mat_t.indptr.astype(np.int64)
ak.connect(connect_url="tcp://bc9u23n4:5555")
start = time.perf_counter()
ak.startTracing()
pd_pointers = ak.array(pointers)
pd_pointers2 = ak.array(pointers2)
pd_indexes = ak.array(indexes)
pd_indexes2 = ak.array(indexes2)
k = 0
s = 0
start = time.perf_counter()
for i in range(len(pointers)-1):
    right = pointers[i+1]
    if (pointers[i]<right):
        for j in range(pointers[i],right):
            #new version
            s+= ak.sortIntersect1d(find_splice(i, pointers, pd_indexes), find_splice(pd_indexes[j], pointers2, pd_indexes2)).size 
            #old version
            #s+= ak.intersect1d(find_splice(i, pointers, pd_indexes), find_splice(pd_indexes[j], pointers2, pd_indexes2)).size

#print(s)
#s = ak.triangle_count_sparse(len(pointers), pd_pointers, pd_indexes, pd_pointers2, pd_indexes2)
#s = half_of_tri_count(len(pointers), pd_pointers, pd_indexes, pd_pointers2, pd_indexes2)
ak.stopTracing()
end = time.perf_counter()
print(s)
exec_time = end - start
f.write(sys.argv[1]+"\n")
f.write(f"{exec_time:0.9f}\n")
print(f"second took {end - start:0.9f} seconds")
#print(f"del took {ak.startAllDell-ak.startAllDellReal:0.9f} seconds")
ak.shutdown()