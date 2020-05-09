from flask import Flask
from flask import jsonify
from flask import request
from flask_pymongo import PyMongo
from bson.json_util import dumps
import requests
import json
import re
from collections import OrderedDict
import pandas as pd
from datetime import datetime
import pika
import uuid
from threading import Timer
import docker 
import os
from kazoo.client import KazooClient

zk = KazooClient(hosts='3.212.113.11:2181')
zk.start()




app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
@app.route('/')
@app.route('/index')

# connection=0
# channel=0
# def fn():
# 	global connection,channel
# 	connection = pika.BlockingConnection(
# 	    pika.ConnectionParameters(host='rmq'))
# 	channel = connection.channel()

# 	channel.exchange_declare(exchange='readnwrite', exchange_type='direct')


class forWrite(object):

	def __init__(self):
		self.connection = pika.BlockingConnection(
			pika.ConnectionParameters(host='3.212.113.11',heartbeat=0))

		self.channel = self.connection.channel()

		result = self.channel.queue_declare(queue='', exclusive=True)
		self.callback_queue = result.method.queue

		self.channel.basic_consume(
			queue=self.callback_queue,
			on_message_callback=self.on_response,
			auto_ack=True)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body

	def call(self, d):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(
			exchange='',
			routing_key='rpc_queue_write',
			properties=pika.BasicProperties(
				reply_to=self.callback_queue,
				correlation_id=self.corr_id,
			),
			body=d)
		while self.response is None:
			self.connection.process_data_events()
		return self.response




class forRead(object):

	def __init__(self):
		self.connection = pika.BlockingConnection(
			pika.ConnectionParameters(host='3.212.113.11',heartbeat=0))

		self.channel = self.connection.channel()

		result = self.channel.queue_declare(queue='', exclusive=True)
		self.callback_queue = result.method.queue

		self.channel.basic_consume(
			queue=self.callback_queue,
			on_message_callback=self.on_response,
			auto_ack=True)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body

	def call(self, d):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(
			exchange='',
			routing_key='rpc_queue_read',
			properties=pika.BasicProperties(
				reply_to=self.callback_queue,
				correlation_id=self.corr_id,
			),
			body=d)
		while self.response is None:
			self.connection.process_data_events()
		return self.response


coureads=0
coureadsprev=0
salveno=0
running_containers_info=[]
client = docker.DockerClient(base_url='unix://var/run/docker.sock')
master_info=[]


def getpid(c_id):
	x = client.containers.get(c_id)
	y = x.top()
	z = y["Processes"]
	pid = z[0][2]
	return (int(pid))


def updateinfo():
	global running_containers_info,client
	running_containers_info=[]
	running_containers = client.containers.list() 
	for i in running_containers:
		container_id = i.id 
		container_name = i.name
		if 'slave' in container_name and container_name!="slave_db":
			# cm="sudo docker inspect --format '{{.State.Pid}}'"+" " +str(container_id)[:12]
			# print("cm",cm)
			# stream = os.popen(cm) 
			# container_pid = stream.read()
			# container_pid=container_pid
			container_pid=getpid(container_id)
			print("yes")
			# cm='GET /v1.24/containers/'+container_id+'/json?size=1 HTTP/1.1'
			# resp_send = requests.get(
			# 	cm, )
			# d = json.loads(resp_send.content)
			# d=json.loads(d)
			print("cid",container_pid)
			# container_pid = int(container_pid) 
			running_containers_info.append( [container_pid,str(container_id),str(container_name)])
	running_containers_info.sort()
	# running_containers_info.reverse()
	print("rci",running_containers_info)



def startup():
	global master_info,salveno
	running_containers = client.containers.list()
	client.containers.run("worker:latest", name='master', command=["sh","-c","service mongodb start; python3 worker.py 0"], detach=True)
	# client.containers.get('master').exec_run("python3 worker.py 0", detach=True)
	running_containers = client.containers.list() 
	for i in running_containers:
		container_id = i.id 
		container_name = i.name
		if container_name=='master':
			# cm="sudo docker inspect --format '{{.State.Pid}}'"+" " +str(container_id)[:12]
			# print("cm",cm)
			# stream = os.popen(cm) 
			# container_pid = stream.read()
			# container_pid=container_pid
			container_pid=getpid(container_id)
			print("cid",container_pid)
			container_pid = int(container_pid) 
			master_info.append(container_pid)
			master_info.append(container_id)
			master_info.append(container_name)
	zk.create("worker/master",("working "+str(container_pid)).encode(),makepath=True)
	print("master created",master_info)
	global salveno,running_containers_info
	salveno+=1
	tem=client.containers.run("worker:latest", name='slave'+str(salveno),command=["sh","-c","service mongodb start; python3 worker.py 1"], detach=True)
	# client.containers.get('slave'+str(salveno)).exec_run("python3 worker.py 1", detach=True)
	zk.create("worker/slave",("working "+str(getpid(tem.id))).encode(),makepath=True)
	updateinfo()
	print("slave created",running_containers_info)


def kill():
	running_containers = client.containers.list() 
	for i in running_containers:
		container_id = i.id 
		container_name = i.name
		if container_name!='orchast':
			client.containers.get(container_id).stop()
			client.containers.get(container_id).remove()


def launch():
	global salveno,client
	salveno+=1
	tem=client.containers.run("worker:latest", name='slave'+str(salveno),command=["sh","-c","service mongodb start; python3 worker.py 1"], detach=True)
	# client.containers.get('slave'+str(salveno)).exec_run("python3 worker.py 1", detach=True)
	print ("Succesfully launched a container")
	retu=getpid(tem.id)
	zk.set("/worker/slave",("working "+str(retu)).encode())
	print("making a node")
	updateinfo()
	return getpid(tem.id)





def stop():
	global running_containers_info
	global salveno,client
	salveno-=1
	client.containers.get(running_containers_info[-1][-1]).stop()
	# print(client.containers.get(running_containers_info[-1][-1]).logs())
	client.containers.get(running_containers_info[-1][-1]).remove()
	print ("Succesfully killed a container")
	updateinfo()
	return


def read_numberof_containers():
	global running_containers_info
	return len(running_containers_info)


@zk.DataWatch("/worker/master")
def masterswatch(data,stat):
	if data:
		data1=data.decode()
		if data1=='removed':
			print("And then the master said : My watch begins :-)")
			global running_containers_info,master_info,salveno
			zk.set("/worker/slave/"+str(running_containers_info[-1][0]), b"changed")
			csl=running_containers_info.pop(-1)
			client.containers.get(csl[2]).rename('master')
			csl[-1]='master'
			master_info.extend(csl)
			salveno-=1
			retu=launch()
			print("called launch")
			print("And master watch ends :-(")



def auto_stop(c):
	t1=0
	t2=0
	temp=1
	if c<=20:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=1):
			t1=read_numberof_containers()
			if t1<=1:
				break
			stop()
	elif c>20 and c<=40:
		print ("Requests ",c)
		t1=read_numberof_containers()
		while(t1!=2):
			t1=read_numberof_containers()
			if t1<=2:
				break
			stop()
	elif c>40 and c<=60:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=3):
			t1=read_numberof_containers()
			temp=temp+1
			if t1<=3:
				break
			stop()
	elif c>60 and c<=80:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=4):
			t1=read_numberof_containers()
			if t1<=4:
				break
			stop()
	elif c>80 and c<=100:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=5):
			t1=read_numberof_containers()
			if t1<=5:
				break
			stop()

	elif c>100 and c<=120:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=6):
			if t1<=6:
				break
			stop()

	elif c>120 and c<=140:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=7):
			t1=read_numberof_containers()
			if t1<=7:
				break
			stop()

	elif c>140 and c<=160:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=8):
			t1=read_numberof_containers()
			if t1<=8:
				break
			stop()

	elif c>160 and c<=180:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=9):
			t1=read_numberof_containers()
			if t1<=9:
				break
			stop()

	elif c>180 and c<=200:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=10):
			t1=read_numberof_containers()
			if t1<=10:
				break
			stop()


def auto_start(c):
	t1=0
	t2=0
	temp=1
	if c<=20:
		print ("Requests ",c)
		t1=read_numberof_containers()
		if t1<1:
			t1=read_numberof_containers()
			launch()
	elif c>20 and c<=40:
		print ("Requests ",c)
		t1=read_numberof_containers()
		while(t1!=2):
			t1=read_numberof_containers()
			if t1>=2:
				break
			launch()
	elif c>40 and c<=60:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=3):
			t1=read_numberof_containers()
			if t1>=3:
				break
			launch()
	elif c>60 and c<=80:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=4):
			t1=read_numberof_containers()
			if t1>=4:
				break
			launch()
	elif c>80 and c<=100:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=5):
			t1=read_numberof_containers()
			if t1>=5:
				break
			launch()

	elif c>100 and c<=120:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=6):
			t1=read_numberof_containers()
			if t1>=6:
				break
			launch()

	elif c>120 and c<=140:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=7):
			t1=read_numberof_containers()
			if t1>=7:
				break
			launch()

	elif c>140 and c<=160:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=8):
			t1=read_numberof_containers()
			if t1>=8:
				break
			launch()

	elif c>160 and c<=180:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=9):
			t1=read_numberof_containers()
			if t1>=9:
				break
			launch()

	elif c>180 and c<=200:
		t1=read_numberof_containers()
		print ("Requests ",c)
		while(t1!=10):
			t1=read_numberof_containers()
			if t1>=10:
				break
			launch()


def timeout():
	print("reached fn")
	fn()
	print("fn called")
	global coureads,coureadsprev
	totalerq=coureads-coureadsprev
	coureadsprev=coureads
	print("checking start",read_numberof_containers())
	auto_start(totalerq)
	print("checking stop",read_numberof_containers())
	auto_stop(totalerq)
	

def fn():
	t = Timer(60.0, timeout)
	t.start()              

flag=0


@app.route('/api/v1/crash/master', methods=['POST'])
def crash_master():
	updateinfo()
	global master_info
	client.containers.get(master_info[-1]).stop()
	# print(client.containers.get(running_containers_info[-1][-1]).logs())
	client.containers.get(master_info[-1]).remove()
	print("Killed Master")
	zk.delete("worker/master/"+str(master_info[0]))
	print("deleted kazoo node for master")
	zk.set("/worker/master", b"removed")
	resfi=[(master_info[0])]
	master_info=[]
	return jsonify(resfi),200
	

	


@app.route('/api/v1/crash/slave', methods=['POST'])
def crash_slave():
	updateinfo()
	global running_containers_info,salveno
	client.containers.get(running_containers_info[-1][-1]).stop()
	# print(client.containers.get(running_containers_info[-1][-1]).logs())
	client.containers.get(running_containers_info[-1][-1]).remove()
	print("Killed Slave")
	zk.delete("worker/slave/"+str(running_containers_info[-1][0]))
	print("deleted kazoo node for slave")
	zk.set("/worker/slave", b"removed")
	resfi=[running_containers_info[-1][0]]
	running_containers_info.pop(-1)
	salveno-=1
	launch()
	print("launch called")
	return jsonify(resfi),200
	



@app.route('/api/v1/worker/list', methods=['GET'])
def list_worker():
	updateinfo()
	global running_containers_info
	resf=[master_info[0]]
	for i in running_containers_info:
		resf.append(i[0])
	resf.sort()
	return jsonify(resf),200





@app.route('/api/v1/db/write', methods=['POST', 'DELETE'])
def write_data():
	write_rpc = forWrite()
	if request.method == 'POST':
		try:
			data = request.get_json()
		except:
			abort_code = 400
			return jsonify({}), abort_code
		# print(data)

		data['method']='post'
		# data['who']='rides'
		# data['query']='insert'
		data2=json.dumps(data)
		file1 = open("commands.txt","a+")
		data3=data2
		file1.write(data3)
		file1.close() 
		resp=write_rpc.call(data2)
		m=json.loads(resp)
		return jsonify(m['respo']),m['rcode']
  
	elif request.method == 'DELETE':
		try:
			data = request.get_json()
		except:
			abort_code = 400
			return jsonify({}), abort_code
		# username = data["username"]
		data['method']='delete'
		# data['who']='rides'
		data2=json.dumps(data)
		file1 = open("commands.txt","a+")
		data3=data2
		file1.write(data3)
		file1.close() 
		resp=write_rpc.call(data2)
		m=json.loads(resp)
		return jsonify(m['respo']),m['rcode']
	return jsonify({}), 400


@app.route('/api/v1/db/read', methods=['POST'])
def read_data():
	global flag,coureads
	coureads+=1
	if flag==0:
		flag=1
		fn()
	read_rpc = forRead()
	# print("1")
	try:
		data = request.get_json()
	except:
		abort_code = 400
		return jsonify({}), abort_code
	# print(data)
	# data['who']='rides'
	# data["userquery"]=1
	data2=json.dumps(data)
	resp=read_rpc.call(data2)
	m=json.loads(resp)
	return jsonify(m['respo']),m['rcode']
	return jsonify({}), 400

@app.route('/api/v1/db/clear', methods=['POST'])
def clear_data():
	print("eneterd clear")
	write_rpc = forWrite()
	try:
		data = request.get_json()
	except:
		abort_code = 400
		return jsonify({}), abort_code
	# print(data)
	# data['who']='rides'
	data['method']='post'
	data['op']='clear'
	data2=json.dumps(data)
	file1 = open("commands.txt","a+")
	data3=data2
	file1.write(data3)
	file1.close() 
	print("making call to writeq")
	resp=write_rpc.call(data2)
	m=json.loads(resp)
	print("call was successful")
	return jsonify(m['respo']),m['rcode']


@app.route('/api/v1/db/copydbtoslave', methods=['POST'])
def copy_data():
	file1 = open("commands.txt","a+")
	file1.seek(0)  
	res=file1.readlines() 
	# print('ress',res)
	return jsonify(res),200




if __name__ == '__main__':
	startup()
	app.debug = True
	app.run('0.0.0.0', port=80,use_reloader=False)
	kill()
	# http_server = WSGIServer(("",5000),app)
	http_server.serve_forever()
