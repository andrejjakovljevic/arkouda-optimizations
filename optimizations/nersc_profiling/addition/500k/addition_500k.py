import arkouda as ak

ak.connect(connect_url='tcp://nid00786:5555')
A = ak.randint(0, 10000, 500000)
for x in range(10000):
    A + A
ak.shutdown()
