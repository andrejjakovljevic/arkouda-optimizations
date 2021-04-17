from numba import jit
import numpy as np
import time

x = np.arange(100).reshape(10, 10)

@jit(nopython=True)
def go_fast(a): # Function is compiled and runs in machine code
    trace = 0.0
    for i in range(a.shape[0]):
        trace += np.tanh(a[i, i])
    return a + trace

def go_fast_non_numba(a): # Function is compiled and runs in machine code
    trace = 0.0
    for i in range(a.shape[0]):
        trace += np.tanh(a[i, i])
    return a + trace

# DO NOT REPORT THIS... COMPILATION TIME IS INCLUDED IN THE EXECUTION TIME!
start = time.time()
go_fast(x)
end = time.time()
print("Numba - Elapsed (with compilation) = %s" % (end - start))

# NOW THE FUNCTION IS COMPILED, RE-TIME IT EXECUTING FROM CACHE
start = time.time()
go_fast(x)
end = time.time()
print("Numba - Elapsed (after compilation) = %s" % (end - start))

# REGULAR VERSION
start = time.time()
go_fast_non_numba(x)
end = time.time()
print("Elapsed (with compilation) = %s" % (end - start))

# REGULAR VERSION
start = time.time()
go_fast_non_numba(x)
end = time.time()
print("Elapsed (after compilation) = %s" % (end - start))
