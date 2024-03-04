import os

os.system('python3 etl.py')
os.system('python3 process_0.py &')
os.system('python3 process_1.py &')
os.system('python3 process_2.py &')
os.system('python3 process_3.py &')
os.system('python3 manager.py &')
os.system('python3 client.py')