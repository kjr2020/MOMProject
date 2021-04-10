from kafka import KafkaConsumer, TopicPartition
import time
import sys

consumer = KafkaConsumer(bootstrap_servers='master:9092', group_id='idpl-consumer', enable_auto_commit=True,
auto_offset_reset='latest', consumer_timeout_ms=10000)

tp = TopicPartition('idpl-topic', int(sys.argv[1]))

consumer.assign([tp])
consumer.seek(tp, 0)

startTime = time.time()

for message in consumer:
    print ('Value : %s, Offest : %d' % (int(message.value), message.offset))
    time.sleep(int(message.value))
    
pw = open('ConsumerResult-'+sys.argv[1], 'a')
pw.write('Consumer' + sys.argv[1] + ' Execution Time : '+str(time.time()-startTime)+'\n')
pw.close()
consumer.close()
print('Consumer is closed..')