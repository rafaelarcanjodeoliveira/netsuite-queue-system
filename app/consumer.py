import pika
import json
import time

def callback(ch, method, properties, body):
    data = json.loads(body)
    print(f"Processando: {data}")
    time.sleep(2)
    print(f"Processado: {data}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='netsuite_queue', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='netsuite_queue', on_message_callback=callback)

    print('Aguardando mensagens...')
    channel.start_consuming()

if __name__ == '__main__':
    start_consumer()
