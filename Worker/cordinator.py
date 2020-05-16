import sys
import subprocess
import time
import pika
from kazoo.client import KazooClient
zk = KazooClient(hosts='3.212.113.11:2181')
zk.start()
zk.ensure_path("/allSlaves")
global ty
ty = None


def change_behaviour(data):
	data,stat = zk.get(data.path)
	if(data.decode("utf-8")=="slave"):
		return
	global ty
	ty.kill()
	commandexe = "python3 worker.py 0"
	commandexe = commandexe.split(" ")
	ty = subprocess.Popen(commandexe)

def passing(data):
	return
if(sys.argv[1] == "0"):
	print("entered master")
	path = zk.create(path="/allSlaves/node",value=b'master',ephemeral=True, sequence=True)
	commandexe = "python3 worker.py 0"
	commandexe = commandexe.split(" ")
	zk.get(path, watch=passing)
	ty = subprocess.Popen(commandexe)
	ty.communicate()

else:
	print("entered slave")
	path = zk.create(path="/allSlaves/node",value=b'slave',ephemeral=True, sequence=True)
	commandexe = "python3 worker.py 1"
	commandexe = commandexe.split(" ")
	zk.get(path, watch=change_behaviour)
	ty = subprocess.Popen(commandexe)
	ty.communicate()

while(True):
	pass