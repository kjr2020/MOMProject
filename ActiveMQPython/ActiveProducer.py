import time, sys, stomp, os

hosts=[('master', 61616)]

producer = stomp.Connection(host_and_ports=hosts)
producer.connect('admin', 'admin', wait=True)

#f = open('../sleeptask', 'r')

message = 'Hello World!'

while True:
    #line = f.readline()
    #if line == '': break
    #producer.send(str.encode(line.rstrip('\n')), destination="/queue/idpl-queue")
    producer.send(body=message, destination='/queue/idpl-queue')

#f.close()
producer.disconnect()