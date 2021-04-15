import pika, sys

credentials = pika.PlainCredentials('master','1234')
connection = pika.BlockingConnection(
    pika.ConnectionParameters('master', 5672, '/', credentials))

i = 0

channel = connection.channel()

channel.queue_declare(queue='idpl-queue', exclusive=False)

fw = open('../sleeptask', 'r')

line = fw.readline()

while True:
    line = fw.readline()
    if line == '': break
    channel.basic_publish(exchange='', routing_key='idpl-queue', body=str.encode(line.rstrip('\n')))
    i += 1

print(' [x] Sent ' + str(i) + ' message in Queue\n')

connection.close()