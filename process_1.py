import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='process_1_etl')
channel.queue_declare(queue='manager_process_1')
channel.queue_declare(queue='process_1_manager')

channel.exchange_declare('11')

channel.queue_bind('process_1_manager', '11')

def callback(ch, method, properties, body):
    #print('proc num 1', num_point)
    n = int(str(body)[2:-1])
    points = {n : num_point[n]}
    channel.basic_publish(exchange='11', routing_key='process_1_manager', body=json.dumps(points))

num_point = {}
s = 67896
while s != None:
    a, c, s = channel.basic_get('process_1_etl')
    if a == None:
        channel.queue_purge(queue='process_1_etl')
        break
    s = s.split()
    #print(s)
    n = s[0]
    x = s[1]
    y = s[2]
    if int(str(n)[2:-1]) not in num_point.keys():
        num_point[int(str(n)[2:-1])] = [[float(x), float(y)]]
    else:
        num_point[int(str(n)[2:-1])].append([float(x), float(y)])
#print(num_point)
channel.basic_consume('manager_process_1', on_message_callback=callback)
channel.start_consuming()