from sqlalchemy.sql.functions import concat

import arkouda as ak
import math
import numpy as np
import time

def counting_sort_by_digit(array, radix, exponent, min_value):
    bucket_index = -1
    buckets = ak.zeros(radix, dtype=ak.int64)
    output = ak.zeros(len(array), dtype=ak.int64)

    move = 1

    ak.count_frequencies(array, buckets, 4, [min_value, exponent, radix, move])
    # Count frequencies
    #for i in range(0, len(array)):
    #    bucket_index = math.floor(((array[i] - min_value) / exponent) % radix)
    #    buckets[bucket_index] += 1

    # Compute cumulates
    #print(buckets)
    ak.cumsum(buckets)
    #print(buckets)
    #for i in range(1, radix):
    #    buckets[i] += buckets[i - 1]

    # Move records
    move = -1
    ak.move_records(array, buckets, output, 4, [min_value, exponent, radix, move])
    #for i in range(len(array) - 1, -1, -1):
    #    bucket_index = math.floor(((array[i] - min_value) / exponent) % radix)
    #    buckets[bucket_index] -= 1
    #    output[buckets[bucket_index]] = array[i]

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
    min_value = array.min()
    max_value = array.max()

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
    return ak.remove_duplicates(radix_sort(ak.concatenate([a,b])))
    #return ak.remove_duplicates(radix_sort((ak.concatenate[a,b])))

ak.connect(connect_url='tcp://andrej-X556UQ:5555')
a = ak.randint(0, 10000, 100000)
b = ak.randint(0, 10000, 100000)
start = time.perf_counter()
#c = radix_sort(a)
#c = ak.sort(a)
#c=ak.union1d(a,b)
c = union(a,b)
print(c)
end = time.perf_counter()
print(f"union_v1 took {end - start:0.9f} seconds")
#print(d)
# ak.disconnect()
ak.shutdown()
