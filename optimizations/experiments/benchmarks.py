from arkouda.pdarrayfunctions import vector_times_matrix
import arkouda as ak
import numpy as np
import time
import random
from scipy.sparse import csr_matrix
import sys
import cProfile

def triangle_count_numpy(L:np.ndarray, nblocks_m, nblocks_n, nblocks_l, verbose):
    """
    Implements the triangle count algorithm
    for numpy arrays.
    """
    dim = L.shape[0]
    bm = (dim + nblocks_m - 1) // nblocks_m
    bn = (dim + nblocks_n - 1) // nblocks_n
    bl = (dim + nblocks_l - 1) // nblocks_l

    bA = create_blocks_numpy(L, bm, bl)
    bB = create_blocks_numpy(L, bl, bn)
    bM = create_blocks_numpy(L, bm, bn)

    s = 0
    for i in range(nblocks_m):
        for j in range(nblocks_n):
            for k in range(nblocks_l):
                s += np.sum(np.multiply(np.dot(bA[i * nblocks_l + k], bB[k * nblocks_n + j]), bM[i * nblocks_n + j]))
                if verbose:
                    print("i, j, k", i, j, k)
                    print("A square", i * nblocks_l + k)
                    print("B square", k * nblocks_n + j)
                    print("M square", i * nblocks_n + j)
                    print("dot", np.dot(bA[i * bl + k], bB[k * nblocks_n + j]))
                    print("mult", np.multiply(np.dot(bA[i * bl + k], bB[k * nblocks_n + j]), bM[i * bn + j]))

    return s


def create_blocks_numpy(A:np.ndarray, row_size, col_size):
    num_rows = A.shape[0] // row_size
    num_cols = A.shape[0] // col_size
    out = []

    for r in range(num_rows):
        for c in range(num_cols):
            M = np.zeros((row_size, col_size))
            for i in range(row_size):
                for j in range(col_size):
                    M[i][j] = A[r*row_size + i][c*col_size + j]
            out.append(M)

    return out


def triangle_count_scalar(L:np.ndarray, nblocks_m, nblocks_n, nblocks_l):
    dim = L.shape[0]
    bm = (dim + nblocks_m - 1) // nblocks_m
    bn = (dim + nblocks_n - 1) // nblocks_n
    bl = (dim + nblocks_l - 1) // nblocks_l

    bA = create_blocks_scalar(L, bm, bl)
    bB = create_blocks_scalar(L, bl, bn)
    bM = create_blocks_scalar(L, bm, bn)

    mxm_result = ak.zeros(1, dtype=np.int64)
    mxm_mask_result = ak.zeros(1, dtype=np.int64)

    s = 0
    for i in range(nblocks_m):
        for j in range(nblocks_n):
            for k in range(nblocks_l):
                # Note: Treated dot product and mxm the same
                ak.multAndStore(bA[i * nblocks_l + k], bB[k * nblocks_n + j], mxm_result)
                ak.multAndStore(mxm_result, bM[i * nblocks_n + j], mxm_mask_result)
                # mxm_result = bA[i * nblocks_l + k] * bB[k * nblocks_n + j]
                # mxm_mask_result = mxm_result * bM[i * nblocks_n + j]
                s += ak.sum(mxm_mask_result)
                # s += ak.sum((bA[i * nblocks_l + k] * bB[k * nblocks_n + j]) * bM[i * nblocks_n + j])

    return s


def create_blocks_scalar(A: np.ndarray, row_size, col_size):
    num_rows = A.shape[0] // row_size
    num_cols = A.shape[0] // col_size
    out = []

    for r in range(num_rows):
        for c in range(num_cols):
            M = ak.randint(0, 100, 1)
            out.append(M)

    return out


def triangle_count_1d():
    """
    Implements the triangle count algorithm
    treating the input adjacency matrix as a 1-d array.
    """
    ak.connect(connect_url='tcp://MacBook-Pro.local:5555')
    # Get lower triangular adjacency matrix a
    a = ak.randint(0, 10, 100)

    # Square lower triangular adjacency matrix
    sq = a**2

    # Element-wise multiplication of sq and  a
    c = sq * a

    # Sum columns of c
    c_prime = ak.sum(c)

    # Sum all entries of c_prime
    print(c_prime)

    ak.disconnect()


def triangle_count():
    """
    Implements the triangle count algorithm
    treating the input adjacency matrix as a regular matrix.
    """
    ak.connect(connect_url='tcp://MacBook-Pro.local:5555')
    ak.shutdown()


def betweenness_centrality_scalar():
    """
    Implements the betweenness centrality algorithm
    treating the input adjacency matrix as a scalar.
    """
    ak.connect(connect_url='tcp://MacBook-Pro.local:5555')
    ak.shutdown()


def betweenness_centrality_1d():
    """
    Implements the triangle count algorithm
    treating the input adjacency matrix as a 1d array.
    """
    #ak.connect(connect_url='tcp://MacBook-Pro.local:5555')

    # GrB_Matrix A
    A = ak.ones(50, ak.int64)
    # GrB_Index s
    s = 2

    # GrB_Index n ;
    # GrB_Matrix nrows(&n ,A ) ;
    n = A.size

    # GrB Vector new ( delta , GrB_FP32 , n ) ;
    delta = ak.ones(n, ak.int64)

    # GrB Matrix sigma ;
    # GrB Matrix new(&sigma , GrB INT32 , n , n ) ;
    sigma = ak.ones(n, ak.int64)

    # GrB Vector q;
    # GrB Vector new(&q , GrB INT32 , n ) ;
    q = ak.ones(n, ak.int64)
    # GrB Vector setElement ( q , 1 , s ) ;
    q[s] = 1

    # GrB Vector p ;
    # GrB Vector dup(&p , q ) ;
    p = q

    # GrB vxm ( q , p , GrB NULL, GrB PLUS TIMES SEMIRING INT32 , q ,A, GrB DESC RC ) ;
    q = q * A

    # GrB Index d = 0 ;
    d = 0
    # int32t sum = 0 ;
    summation = 0
    numIter = 0
    random.seed()
    r = 10

    while True:
        # Only doing one iteration
        # GrB assign ( sigma , GrB NULL, GrB NULL, q , d , GrB ALL , n , GrB NULL ) ;
        sigma[d] = q[d]
        # GrB eWiseAdd ( p , GrB NULL, GrB NULL, GrB PLUS INT32 , p , q , GrB NULL ) ;
        p = p + q
        # GrB vxm ( q , p , GrB NULL, GrB PLUS TIMES SEMIRING INT32 ,q ,A, GrB DESC RC ) ;
        q = q * A
        # GrB reduce(&sum , GrB NULL, GrB PLUS MONOID INT32 , q , GrB NULL ) ;
        summation = ak.sum(q)
        # ++d ;
        d=d+1
        if (d==r):
            break
    #print("number of iterations:",d)
    # GrB Vector t 1 ; GrB Vector new(&t1 , GrB FP32 , n ) ; for t1-t4
    # t1 = ak.zeros(n, ak.int64)
    # t2 = ak.zeros(n, ak.int64)
    # t3 = ak.zeros(n, ak.int64)
    # t4 = ak.zeros(n, ak.int64)
    for i in range(d - 1, 0, -1):
        # GrB assign ( t1 , GrB NULL, GrB NULL, 1 . 0 f , GrB ALL , n , GrB NULL ) ;
        t1 = ak.ones(n, ak.int64)
        # GrB eWiseAdd ( t1 , GrB NULL, GrB NULL, GrB PLUS MONOID FP32, t1 , ??? delta , GrB NULL ) ;
        t1 = t1 + delta
        # G rB e x t r ac t ( t2 , GrB NULL, GrB NULL, sigma , GrB ALL , n , i , GrB DESC T0 ) ;
        t2 = sigma[i]
        # GrB eWiseMult ( t2 , GrB NULL, GrB NULL, GrB DIV FP32 , t1 , t2 , GrB NULL ) ;
        t2 = t1 // t2
        # GrB mxv ( t3 , GrB NULL, GrB NULL, GrB PLUS TIMES SEMIRING FP32 , A, t2 , GrB NULL ) ;
        t3 = A * t2
        # G rB e x t r ac t ( t4 , GrB NULL, GrB NULL, sigma , GrB ALL , n , i ???1,GrB DESC T0 ) ;
        t4 = sigma[(i - 1)]
        # GrB eWiseMult ( t4 , GrB NULL, GrB NULL, GrB TIMES FP32 , t4 , t3 , GrB NULL ) ;
        t4 = sigma[(i - 1)] * t3
        # GrB eWiseAdd (??? d el t a , GrB NULL, GrB NULL, GrB PLUS FP32 , ??? d el t a , t4 , GrB NULL ) ;
        delta = delta + t4
    #ak.shutdown()

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
            fs.append(f)
            ss.append(s)
            fs.append(s)
            ss.append(f)
            datas.append(data)
            datas.append(data)
        i+=1
    s_mat = csr_matrix((datas,(fs, ss)), shape=(shape_size, shape_size))
    #s_mat_t = s_mat.transpose(axes = None, copy=True)
    return (s_mat)
def create_blocks_scalar(A: np.ndarray):
    out = []
    for k in A:
        out.append(ak.array(k))
    return out

def betweenness_centrality(A: list, source: int):
    n = len(A)
    delta = ak.zeros(n)
    sigma = []
    for i in range(n):
        sigma.append(ak.zeros(n, np.float64))
    q = ak.zeros(n, np.float64)
    q[source]=1
    p=q
    d = 0
    sum = 0
    #print("sss=",ak.inverse(p))
    while (True):
        # GrB assign ( sigma , GrB NULL, GrB NULL, q , d , GrB ALL , n , GrB NULL ) ;
        sigma[d] = q
        # GrB eWiseAdd ( p , GrB NULL, GrB NULL, GrB PLUS INT32 , p , q , GrB NULL ) ;
        p = p + q
        # GrB vxm ( q , p , GrB NULL, GrB PLUS TIMES SEMIRING INT32 ,q ,A, GrB DESC RC ) ;
        q = ak.vector_times_matrix(n,q,A)*ak.inverse(p)
        #print("q=",q)
        # GrB reduce(&sum , GrB NULL, GrB PLUS MONOID INT32 , q , GrB NULL ) ;
        sum = ak.sum(q)
        d+=1
        if (sum==0):
            break
    for i in range(d-1, 0, -1):
        # GrB assign ( t1 , GrB NULL, GrB NULL, 1 . 0 f , GrB ALL , n , GrB NULL ) ;
        t1 = ak.ones(n, ak.int64)
        # GrB eWiseAdd ( t1 , GrB NULL, GrB NULL, GrB PLUS MONOID FP32, t1 , ??? delta , GrB NULL ) ;
        t1 = t1 + delta
        # G rB e x t r ac t ( t2 , GrB NULL, GrB NULL, sigma , GrB ALL , n , i , GrB DESC T0 ) ;
        t2 = sigma[i]
        # GrB eWiseMult ( t2 , GrB NULL, GrB NULL, GrB DIV FP32 , t1 , t2 , GrB NULL ) ;
        t2 = (t1 / t2) 
        # GrB mv ( t3 , GrB NULL, GrB NULL, GrB PLUS TIMES SEMIRING FP32 , A, t2 , GrB NULL ) ;
        t3 = ak.matrix_times_vector(n,t2,A)
        # G rB e x t r ac t ( t4 , GrB NULL, GrB NULL, sigma , GrB ALL , n , i ???1,GrB DESC T0 ) ;
        t4 = sigma[(i - 1)]
        # GrB eWiseMult ( t4 , GrB NULL, GrB NULL, GrB TIMES FP32 , t4 , t3 , GrB NULL ) ;
        t4 = t4 * t3
        # GrB eWiseAdd (??? d el t a , GrB NULL, GrB NULL, GrB PLUS FP32 , ??? d el t a , t4 , GrB NULL ) ;
        delta = delta + t4
    return delta

# betweenness_centrality_1d()

x = np.array([[0, 0, 0, 0],
              [1, 0, 0, 0],
              [1, 1, 0, 0],
              [0, 1, 1, 0]])
# print(triangle_count_numpy(x, 2, 2, 2, False))


ak.connect(connect_url='tcp://bc6u11n7:5555')
# x = ak.randint(0, 10, 100)
# y = ak.randint(0, 10, 100)
# z = x + y
# print(ak.sum(z))
# y = ak.randint(0, 10, 100)
# print(y.client_name)
(s_mat) = get_matrices("/home/an58/"+sys.argv[1]+".mtx")
dense1 = s_mat.todense().tolist()
#print(s_mat)
mat = create_blocks_scalar(np.array(dense1,np.int64))
start = time.perf_counter()
ak.startTracing()
cProfile.run('ak.sum(betweenness_centrality(mat,0))')
ak.stopTracing()
end = time.perf_counter()
print(f"on chapel betwenness centrality took {end - start:0.9f} seconds")
#start = time.perf_counter()
#s = ak.sum(betweenness_centrality(mat,0))
#print(s)
#s = betweenness_centrality_1d()
#end = time.perf_counter()
#print(f"on client betwenness centrality took {end - start:0.9f} seconds")

# start = time.perf_counter()
# x = ak.randint(0, 5, 10)
# for i in range(1000):
#     x = x + 5
#     # x += 5
# end = time.perf_counter()
# print(f"loop took {end - start:0.9f} seconds")
# A = ak.randint(0, 10, 10)
# B = ak.randint(0, 10, 10)
# C = ak.randint(0, 10, 10)
# D = ak.randint(0, 10, 10)
# E = ak.randint(0, 10, 10)
# F = ak.randint(0, 10, 10)
# x = ((A+B)*(C+D)) + (E*F)
#ak.disconnect()
ak.shutdown()