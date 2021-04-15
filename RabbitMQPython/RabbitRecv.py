import pika, time

credentials = pika.PlainCredentials('master','1234')
connection = pika.BlockingConnection(
    pika.ConnectionParameters('master', 5672, '/', credentials))

channel = connection.channel()

channel.queue_declare(queue='idpl-queue', exclusive=False)

channel.basic_qos(prefetch_count=10)

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    print(" [x] %d" % int(body))

channel.basic_consume(
    queue='idpl-queue', on_message_callback=callback, auto_ack=True)

channel.start_consuming()