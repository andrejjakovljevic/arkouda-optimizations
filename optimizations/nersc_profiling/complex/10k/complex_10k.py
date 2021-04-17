import arkouda as ak

ak.connect(connect_url='tcp://MacBook-Pro.local:5555')
A = ak.randint(0, 10000, 10000)
B = ak.randint(0, 10000, 10000)
for x in range(1000):
    (A * A) + (B * B)
ak.shutdown()
