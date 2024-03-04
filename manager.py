import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='manager_etl')
channel.queue_declare(queue='client_manager')
channel.queue_declare(queue='manager_client')
channel.queue_declare(queue='manager_process_0')
channel.queue_declare(queue='manager_process_1')
channel.queue_declare(queue='manager_process_2')
channel.queue_declare(queue='manager_process_3')
channel.queue_declare(queue='process_0_manager')
channel.queue_declare(queue='process_1_manager')
channel.queue_declare(queue='process_2_manager')
channel.queue_declare(queue='process_3_manager')

channel.exchange_declare('0')
channel.exchange_declare('1')
channel.exchange_declare('2')
channel.exchange_declare('3')

channel.queue_bind('manager_process_0', '0')
channel.queue_bind('manager_process_1', '1')
channel.queue_bind('manager_process_2', '2')
channel.queue_bind('manager_process_3', '3')

global i
i = 0

global count
count = 0

global points
points = []

def callback2(ch, method, properties, body):
    global i
    global count
    count += 1
    global points
    process_points = list(json.loads(body).values())[0]
    points.append(process_points)
    if count == i:
        channel.basic_publish(exchange='31', routing_key='manager_client', body=json.dumps(points))
        channel.queue_purge(queue='manager_process_0')
        channel.queue_purge(queue='manager_process_1')
        channel.queue_purge(queue='manager_process_2')
        channel.queue_purge(queue='manager_process_3')
        channel.queue_purge(queue='process_0_manager')
        channel.queue_purge(queue='process_1_manager')
        channel.queue_purge(queue='process_2_manager')
        channel.queue_purge(queue='process_3_manager')
        points = []
        i = 0
        count = 0

name_num = None
process_num = None
while name_num == None:
    a, b, name_num = channel.basic_get('manager_etl')
while process_num == None:
    a, b, process_num = channel.basic_get('manager_etl')
name_num = json.loads(name_num)
process_num = json.loads(process_num)
channel.queue_purge(queue='manager_etl')
#print(name_num)
#print(process_num)

def callback(ch, method, properties, body):
    global i
    if str(body)[2:-1] == 'roads list':
        channel.basic_publish(exchange='31', routing_key='manager_client', body=json.dumps(name_num))
    if str(body)[2:12] == 'print road':
        n = int(str(body)[13:-1])
        points = []
        for key in process_num.keys():
            if n in process_num[key]:
                i += 1
        for proc_num in process_num.keys():
            if n in process_num[proc_num]:
                #print(n, proc_num)
                queue_man_pro = 'manager_process_' + str(proc_num)
                queue_pro_man = 'process_' + str(proc_num) + '_manager'
                channel.basic_publish(exchange=str(proc_num), routing_key=queue_man_pro, body=str(n))
                channel.basic_consume(queue_pro_man, on_message_callback=callback2)
    points = []

channel.basic_consume('client_manager', on_message_callback=callback, auto_ack=False)
channel.start_consuming()