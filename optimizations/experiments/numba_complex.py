import arkouda as ak
from numba import jit
from arkouda.pdarrayclass import pdarray
import time

@jit(forceobj=True)
def complex_numba(A: pdarray, B: pdarray):
    (A * A) + (B * B)

def complex(A, B):
    (A * A) + (B * B)


ak.connect(connect_url='tcp://MacBook-Pro.local:5555')
A = ak.randint(0, 10000, 10000)
B = ak.randint(0, 10000, 10000)

# # DO NOT REPORT THIS... COMPILATION TIME IS INCLUDED IN THE EXECUTION TIME!
# start = time.time()
# complex_numba(A, B)
# end = time.time()
# print("Numba - Elapsed (with compilation) = %s" % (end - start))
#
# # NOW THE FUNCTION IS COMPILED, RE-TIME IT EXECUTING FROM CACHE
# start = time.time()
# complex_numba(A, B)
# end = time.time()
# print("Numba - Elapsed (after compilation) = %s" % (end - start))

start = time.time()
complex(A, B)
end = time.time()
print("Elapsed (with compilation) = %s" % (end - start))
#
# start = time.time()
# complex(A, B)
# end = time.time()
# print("Elapsed (after compilation) = %s" % (end - start))

# ak.shutdown()
