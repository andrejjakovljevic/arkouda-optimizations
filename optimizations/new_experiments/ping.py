import zmq
import time

context = zmq.Context()
print("Connecting to hello world server…")
socket = context.socket(zmq.REQ)
socket.connect("tcp://192.168.151.242:5555")


start = time.perf_counter()

#  Do 10 requests, waiting each time for a response
for request in range(100000):
    #print("Sending request %s …" % request)
    socket.send(b"Hello")

    #  Get the reply.
    message = socket.recv()
    #print("Received reply %s [ %s ]" % (request, message))


end = time.perf_counter()
print(f"Stuff took {end - start:0.9f} seconds")