import arkouda as ak
import numpy as np
import time


def triangle_count_numpy(L: np.ndarray, nblocks_m, nblocks_n, nblocks_l, verbose):
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


def create_blocks_numpy(A: np.ndarray, row_size, col_size):
    num_rows = A.shape[0] // row_size
    num_cols = A.shape[0] // col_size
    out = []

    for r in range(num_rows):
        for c in range(num_cols):
            M = np.zeros((row_size, col_size))
            for i in range(row_size):
                for j in range(col_size):
                    M[i][j] = A[r * row_size + i][c * col_size + j]
            out.append(M)

    return out


def triangle_count_scalar(L: np.ndarray, nblocks_m, nblocks_n, nblocks_l):
    dim = L.shape[0]
    bm = (dim + nblocks_m - 1) // nblocks_m
    bn = (dim + nblocks_n - 1) // nblocks_n
    bl = (dim + nblocks_l - 1) // nblocks_l

    bA = create_blocks_scalar(L, bm, bl)
    bB = create_blocks_scalar(L, bl, bn)
    bM = create_blocks_scalar(L, bm, bn)

    mxm_result = ak.randint(0, 100, 1)
    mxm_mask_result = ak.randint(0, 100, 1)

    s = 0
    for i in range(nblocks_m):
        for j in range(nblocks_n):
            for k in range(nblocks_l):
                ak.multAndStore(bA[i * nblocks_l + k], bB[k * nblocks_n + j], mxm_result)
                ak.multAndStore(mxm_result, bM[i * nblocks_n + j], mxm_mask_result)

                # print("\n*** mxm_result line *** ")
                # mxm_result = bA[i * nblocks_l + k] * bB[k * nblocks_n + j]
                # print("mxm_result id is", mxm_result.name)
                # print("*** mxm_result line ***\n")
                # print("\n*** mxm_mask_result line *** ")
                # mxm_mask_result = mxm_result * bM[i * nblocks_n + j]
                # print("mxm_mask_result id is", mxm_mask_result.name)
                # print("*** mxm_mask_result line ***\n")
                s += ak.sum(mxm_mask_result)

                # mxm_result.__del__()
                # mxm_mask_result.__del__()

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

ak.connect(connect_url='tcp://andrej-X556UQ:5555')
start = time.perf_counter()
d = ak.arange(10, 15, 1)
#print(b)
for i in range(2):
    d = 3 * (d+d)
print('array=', d)
# print('nes=', d.name)
end = time.perf_counter()
print(f"triangle count took {end - start:0.9f} seconds")
# ak.disconnect()
ak.shutdown()