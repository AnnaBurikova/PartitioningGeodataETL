import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='client_manager')
channel.queue_declare(queue='manager_client')

channel.exchange_declare('00000')
channel.exchange_declare('31')

channel.queue_bind('client_manager', '00000')
channel.queue_bind('manager_client', '31')

s = ''
name_num = {}
while s != 'exit':
    channel.start_consuming()
    print('Entre one of the comands:')
    print('1. print road [road\'s number]')
    print('2. roads list')
    print('3. exit')
    s = input()
    if s[:10] != 'print road' and s!= 'roads list' and s != 'exit':
        print('Wrong comand. Try again')
    elif s != 'exit':
        channel.basic_publish(exchange='00000', routing_key='client_manager', body=s)
        for method, properties, body in channel.consume('manager_client'):
            body = json.loads(body)
            if s[:10] == 'print road':
                print(*body)
            else:
                for k in body.keys():
                    print(k, body[k])
            channel.queue_purge('client_manager')
            channel.queue_purge('manager_client')
            channel.stop_consuming()
channel.queue_delete('client_manager')
channel.queue_delete('manager_client')
channel.queue_delete(queue='manager_process_0')
channel.queue_delete(queue='manager_process_1')
channel.queue_delete(queue='manager_process_2')
channel.queue_delete(queue='manager_process_3')
channel.queue_delete(queue='process_0_manager')
channel.queue_delete(queue='process_1_manager')
channel.queue_delete(queue='process_2_manager')
channel.queue_delete(queue='process_3_manager')
channel.queue_delete(queue='manager_etl')
channel.queue_delete(queue='process_0_etl')
channel.queue_delete(queue='process_1_etl')
channel.queue_delete(queue='process_2_etl')
channel.queue_delete(queue='process_3_etl')