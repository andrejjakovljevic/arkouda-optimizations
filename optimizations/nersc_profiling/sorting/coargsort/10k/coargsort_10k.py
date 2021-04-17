import arkouda as ak

ak.connect(connect_url='tcp://nid00751:5555')
A = ak.randint(0, 10000, 10000)
B = ak.randint(0, 10000, 10000)
ak.coargsort([A, B])
ak.shutdown()
