from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='master:9092', batch_size=4)

i=0

fw = open("sleeptask",'r')

startTime = time.time()


while True:
    line = fw.readline()
    if line == '': break
    producer.send('idpl-topic', str.encode(line.rstrip('\n')))

timeFile = open("ProducerResult", 'a')

timeFile.write("Producer ExecutionTime : " + str(time.time() - startTime) + '\n')

fw.close()
timeFile.close()