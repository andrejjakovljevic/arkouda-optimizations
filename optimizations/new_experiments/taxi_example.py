import arkouda as ak
import math
import time
import numpy as np

ak.connect(connect_url='tcp://andrej-X556UQ:5555')
# take delta for ride duration
ride_duration = ak.randint(1,100,10)
start = time.perf_counter()
print("min = ", ride_duration.min(),"max = ", ride_duration.max())
print("mean = ",ride_duration.mean(),"stdev = ",ride_duration.std())

# how long was the min/max ride to the next integer minute
min_ride = math.floor(ride_duration.min())
print("min_ride = ", min_ride)
max_ride = math.ceil(ride_duration.max())
print("max_ride = ", max_ride)
end = time.perf_counter()
print(f"MinMax took {end - start:0.9f} seconds")
# histogram the ride time bin by the minute
start = time.perf_counter()
nBins = max_ride - min_ride
cnts = ak.histogram(ride_duration, bins=nBins)
end = time.perf_counter()
print(f"histogram took {end - start:0.9f} seconds")

# create bin edges because ak.histogram doesn't
binEdges = np.linspace(ride_duration.min(), ride_duration.max(), nBins+1)
print(cnts)
ak.shutdown()