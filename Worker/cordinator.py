import sys
import subprocess
import time
import pika
from kazoo.client import KazooClient
zk = KazooClient(hosts='3.212.113.11:2181')
zk.start()
zk.ensure_path("/allSlaves")
global p
p = None
def doNothing(data):
    return

def changeToMaster(data):
    data,stat = zk.get(data.path)
    print(data)
    if(data.decode("utf-8")=="slave"):
        return
    global p
    print(p)
    p.kill()
    string = "python3 worker.py 0"
    string = string.split(" ")
    # out = subprocess.Popen(string)
    p = subprocess.Popen(string,
                    #  cwd="/",
                     stdout=subprocess.PIPE,
                     stderr=subprocess.STDOUT)
    print(p.pid)
if(sys.argv[1] == "0"):
    print("printing as master")
    path = zk.create(path="/allSlaves/node",value=b'master',ephemeral=True, sequence=True)
    string = "python3 -u worker.py 0"
    string = string.split(" ")
    zk.get(path, watch=doNothing)
    # out = subprocess.Popen(string)
    p = subprocess.Popen(string)
                    #  cwd="/",
                    #  stdout=subprocess.PIPE,
                    #  stderr=subprocess.STDOUT)
    print(p.pid)
    p.communicate()
    # while(1):
    #     time.sleep(1)

else:
    print("printing as slave")
    path = zk.create(path="/allSlaves/node",value=b'slave',ephemeral=True, sequence=True)
    string = "python3 -u worker.py 1"
    string = string.split(" ")
    zk.get(path, watch=changeToMaster)
    # out = subprocess.Popen(string)
    p = subprocess.Popen(string)
                    #  cwd="/",
                    #  stdout=subprocess.PIPE,
                    #  stderr=subprocess.STDOUT)
    print(p.pid)
    p.communicate()
    # while(1):
    #     time.sleep(1)
# connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
# channel = connection.channel()
# channel.queue_declare(queue='blah', durable=True)
# channel.basic_consume(queue='blah', on_message_callback=doNothing)
# channel.start_consuming()

while(True):
    pass