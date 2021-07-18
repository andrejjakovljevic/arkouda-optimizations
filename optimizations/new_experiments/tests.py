from sqlalchemy.sql.functions import concat

import arkouda as ak
import math
import numpy as np
import time

def counting_sort_by_digit(array, radix, exponent, min_value):
    bucket_index = -1
    buckets = ak.zeros(radix, dtype=ak.int64)
    output = ak.zeros(len(array), dtype=ak.int64)

    # Count frequencies
    for i in range(0, len(array)):
        bucket_index = math.floor(((array[i] - min_value) / exponent) % radix)
        buckets[bucket_index] += 1

    # Compute cumulates
    for i in range(1, radix):
        buckets[i] += buckets[i - 1]

    # Move records
    for i in range(len(array) - 1, -1, -1):
        bucket_index = math.floor(((array[i] - min_value) / exponent) % radix)
        buckets[bucket_index] -= 1
        output[buckets[bucket_index]] = array[i]

    return output

def concat(a, b):
    c = ak.zeros(a.size+b.size, dtype=ak.int64)
    c[:a.size] = a[:a.size]
    c[a.size:] = b[:b.size]
    return c

def joinTest(a, b):
    #a = radix_sort(a)
    #b = radix_sort(b)
    c1 = ak.zeros(a.size*b.size, dtype=ak.int64)
    c2 = ak.zeros(a.size*b.size, dtype=ak.int64)
    cnt1 = 0
    cnt2 = 0
    group = {}
    for i in range(0, b.size):
        if (b[i] in group.keys()):
            group[b[i]].append(i)
        else:
            group[b[i]]=[i]
    # print(group)
    for i in range(a.size):
        if (a[i] in group.keys()):
            for k in group[a[i]]:
                c1[cnt1] = i + 1
                cnt1 = cnt1 + 1
                c2[cnt2] = k + 1
                cnt2 = cnt2 + 1
    return (c1,c2)

def radix_sort(array, radix=10):
    """
    Performs an LSD radix sort on an array given a radix.
    """
    if len(array) == 0:
        return array

    # Determine minimum and maximum values
    min_value = array[0]
    max_value = array[0]
    for i in range(1, len(array)):
        if array[i] < min_value:
            min_value = array[i]
        elif array[i] > max_value:
            max_value = array[i]

    # Perform counting sort on each exponent/digit, starting at the least
    # significant digit
    exponent = 1
    while (max_value - min_value) / exponent >= 1:
        array = counting_sort_by_digit(array, radix, exponent, min_value)
        exponent *= radix
    return array

def uniqueFromSorted(array):
    cnt = 1
    for i in range(1,array.size):
        if (array[i] != array[i-1]):
            cnt=cnt+1
    ret = ak.zeros(cnt,ak.int64)
    cnt = 1
    ret[0] = array[0]
    for i in range(1,array.size):
        if (array[i] != array[i-1]):
            ret[cnt] = array[i]
            cnt = cnt+1
    return ret

def union(a, b):
    return uniqueFromSorted(radix_sort((concat(a,b))))

ak.connect(connect_url='tcp://andrej-X556UQ:5555')
a = ak.randint(0, 100, 100)
b = ak.randint(0, 100, 100)
start = time.perf_counter()
c = union(a, b)
print(c)
end = time.perf_counter()
print(f"union_v1 took {end - start:0.9f} seconds")
#print(d)
# ak.disconnect()
ak.shutdown()
