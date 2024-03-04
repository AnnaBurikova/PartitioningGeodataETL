import numpy as np
import geojson
import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='process_0_etl')
channel.queue_declare(queue='process_1_etl')
channel.queue_declare(queue='process_2_etl')
channel.queue_declare(queue='process_3_etl')
channel.queue_declare(queue='manager_etl')

channel.exchange_declare('5')
channel.exchange_declare('6')
channel.exchange_declare('7')
channel.exchange_declare('8')
channel.exchange_declare('9')

channel.queue_bind('process_2_etl', '5')
channel.queue_bind('process_3_etl', '6')
channel.queue_bind('process_1_etl', '8')
channel.queue_bind('process_0_etl', '7')
channel.queue_bind('manager_etl', '9')

with open('vienna-streets.geojson') as f:
    gj = geojson.load(f)

min_x = 100000
max_x = -111111
min_y = 100000
max_y = -111111
for i in range(len(gj['features'])):
    coord = np.transpose(gj['features'][0]['geometry']['coordinates'])
    min_x = min(np.min(coord[0]), min_x)
    min_y = min(np.min(coord[1]), min_y)
    max_y = max(np.max(coord[1]), max_y)
    max_x = max(np.max(coord[0]), max_x)
avgx = (max_x + min_x) / 2
avgy = (min_y + max_y) / 2

n = 0
name_num = {}
process_num = {0: [], 1: [], 2: [], 3: []}
for i in range(len(gj['features'])):
    if 'name' in gj['features'][i]['properties']:
        name_num[n] = gj['features'][i]['properties']['name']
    else:
        name_num[n] = 'unk' + str(n)
    for x, y in gj['features'][i]['geometry']['coordinates']:
        if y < avgy and x < avgx:
            channel.basic_publish(exchange='5', routing_key='process_2_etl', body=str(n) + ' ' + "{0:.10f}".format(x) + ' ' + "{0:.10f}".format(y))
            if n not in process_num[2]:
                process_num[2].append(n)
        elif y < avgy and x > avgx:
            channel.basic_publish(exchange='6', routing_key='process_3_etl', body=str(n) + ' ' + "{0:.10f}".format(x) + ' ' + "{0:.10f}".format(y))
            if n not in process_num[3]:
                process_num[3].append(n)
        elif y > avgy and x < avgx:
            channel.basic_publish(exchange='7', routing_key='process_0_etl', body=str(n) + ' ' + "{0:.10f}".format(x) + ' ' + "{0:.10f}".format(y))
            if n not in process_num[0]:
                process_num[0].append(n)
        elif y > avgy and x > avgx:
            channel.basic_publish(exchange='8', routing_key='process_1_etl', body=str(n) + ' ' + "{0:.10f}".format(x) + ' ' + "{0:.10f}".format(y))
            if n not in process_num[1]:
                process_num[1].append(n)
    n += 1
#print(name_num)
channel.basic_publish(exchange='9', routing_key='manager_etl', body=json.dumps(name_num))
channel.basic_publish(exchange='9', routing_key='manager_etl', body=json.dumps(process_num))
#print('ok')