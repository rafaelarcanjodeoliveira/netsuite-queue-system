import pika
import json

def send_message(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='netsuite_queue', durable=True)

    channel.basic_publish(
        exchange='',
        routing_key='netsuite_queue',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print("Mensagem enviada:", message)
    connection.close()

if __name__ == '__main__':
    send_message({'transacao_id': 'INV12345', 'acao': 'atualizar'})
