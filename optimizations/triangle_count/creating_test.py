import arkouda as ak
import numpy as np
import time
import sys

def create_blocks_scalar(row_size,size):
    out = []

    for r in range(row_size):
        for c in range(row_size):
            M = ak.randint(0, 10, size)
            out.append(M)

    return out

x = np.array([[0, 0, 0, 0],
              [1, 0, 0, 0],
              [1, 1, 0, 0],
              [0, 1, 1, 0]])

ak.connect(connect_url='tcp://andrej-X556UQ:5555')
start = time.perf_counter()
arg_size=int(sys.argv[1])
print(create_blocks_scalar(arg_size,100))
end = time.perf_counter()
print(f"triangle count took {end - start:0.9f} seconds")
#ak.disconnect()
ak.shutdown()
