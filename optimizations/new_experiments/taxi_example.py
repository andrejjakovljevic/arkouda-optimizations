import arkouda as ak
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
import gc
import time


def ak_create_akdict_from_df(df):
    akdict = {}
    for cname in df.keys():
        if df[cname].dtype.name == 'object':
            akdict[cname] = ak.from_series(df[cname], dtype=np.str)
        else:
            akdict[cname] = ak.from_series(df[cname])

    return akdict


# returns minutes as a float
def ns_to_min(v):
    return (v / (1e9 * 60.0))


def cvt_to_int64(v):
    try:
        return np.int64(v)
    except:
        return np.int64(0)


def cvt_YN_to_bool(v):
    if v == 'Y':
        return True
    else:
        return False


ak.connect(connect_url='tcp://andrej-X556UQ:5555')

# Read in yellow taxi data
# per yellow data dictionary convert to data types Arkouda can handle
# int64, float64, bool
cvt = {'VendorID': cvt_to_int64, 'passenger_count': cvt_to_int64, 'RatecodeID': cvt_to_int64,
       'store_and_fwd_flag': cvt_YN_to_bool,
       'PULocationID': cvt_to_int64, 'DOLocationID': cvt_to_int64, 'payment_type': cvt_to_int64}
# explicitly parse date-time fields
parse_dates_lst = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
# call read_csv to parse data with these options
ydf = pd.read_csv("/home/andrej/Documents/yellow_tripdata_2020-01.csv",
                  converters=cvt, header=0, low_memory=False,
                  parse_dates=parse_dates_lst, infer_datetime_format=True)

akdict = ak_create_akdict_from_df(ydf)
# take delta for ride duration
ride_duration = akdict['tpep_dropoff_datetime'] - akdict['tpep_pickup_datetime']
# pull out ride duration in minutes
ride_duration = ns_to_min(ride_duration)
start = time.perf_counter()
for i in range(3):
    min1 = ride_duration.min()
    max1 = ride_duration.max()
    m1 = ride_duration.mean()
    s1 = ride_duration.std()

    # how long was the min/max ride to the next integer minute
    min_ride = math.floor(ride_duration.min())
    max_ride = math.ceil(ride_duration.max())
    # print('done')
end = time.perf_counter()
print(f"MinMax took {end - start:0.9f} seconds")
# histogram the ride time bin by the minute
#start = time.perf_counter()
#nBins = max_ride - min_ride
#cnts = ak.histogram(ride_duration, bins=nBins)
#end = time.perf_counter()
print("min = ", min1, "max = ", max1)
print("mean = ", m1, "stdev = ", s1)
print("min_ride = ", min_ride)
print("max_ride = ", max_ride)
#print(f"histogram took {end - start:0.9f} seconds")

# create bin edges because ak.histogram doesn't
#binEdges = np.linspace(ride_duration.min(), ride_duration.max(), nBins + 1)
#print(cnts)
ak.shutdown()