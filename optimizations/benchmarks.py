import arkouda as ak
import numpy as np
import time

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


x = np.array([[0, 0, 0, 0],
              [1, 0, 0, 0],
              [1, 1, 0, 0],
              [0, 1, 1, 0]])

ak.connect(connect_url='tcp://MacBook-Pro.local:5555')
start = time.perf_counter()
print(triangle_count_scalar(x, 2, 2, 2))
end = time.perf_counter()
print(f"triangle count took {end - start:0.9f} seconds")
ak.disconnect()
# ak.shutdown()
