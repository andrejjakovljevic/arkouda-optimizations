import arkouda as ak

ak.connect(connect_url='tcp://nid00782:5555')
A = ak.randint(0, 10000, 1000000)
for x in range(1000):
    ak.argsort(A)
ak.shutdown()
