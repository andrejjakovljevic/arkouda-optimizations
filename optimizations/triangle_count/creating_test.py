import arkouda as ak
import numpy as np
import time
import sys

def create_blocks_scalar(row_size,size):
    out = 0

    #mxm_result = ak.zeros(1, dtype=np.int64)
    for r in range(row_size):
        for c in range(row_size):
            a = ak.randint(0,10,size)
            b = ak.randint(0,10,size)
            mxm_result = a*b
            out+=ak.sum(mxm_result)

    return out

ak.connect(connect_url='tcp://andrej-X556UQ:5555')
start = time.perf_counter()
arg_size=int(sys.argv[1])
p=create_blocks_scalar(arg_size,100)
#print(p)
end = time.perf_counter()
print(f"triangle count took {end - start:0.9f} seconds")
#ak.disconnect()
ak.shutdown()
