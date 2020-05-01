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
			pika.ConnectionParameters(host='3.212.113.11'))

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
			pika.ConnectionParameters(host='3.212.113.11'))

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
client = docker.from_env()
master_info=[]





def updateinfo():
	global running_containers_info,client
	running_containers_info=[]
	running_containers = client.containers.list() 
	for i in running_containers:
		container_id = i.id 
		container_name = i.name
		if 'slave' in container_name:
			cm="sudo docker inspect --format '{{.State.Pid}}'"+" " +str(container_id)[:12]
			print("cm",cm)
			stream = os.popen(cm) 
			container_pid = stream.read()
			container_pid=container_pid
			print("yes")
			# cm='GET /v1.24/containers/'+container_id+'/json?size=1 HTTP/1.1'
			# resp_send = requests.get(
			# 	cm, )
			# d = json.loads(resp_send.content)
			# d=json.loads(d)
			print("cid",container_pid)
			container_pid = int(container_pid) 
			running_containers_info.append( [container_pid,str(container_id),str(container_name)])
	running_containers_info.sort()
	print("rci",running_containers_info)



def startup():
	global master_info,salveno
	client.containers.run("worker:latest", name='master', command="python3 worker.py 0",network='finalprojectorchas_default', detach=True,links={'master_db':'master_db'})
	# client.containers.get('master').exec_run("python3 worker.py 0", detach=True)
	running_containers = client.containers.list() 
	for i in running_containers:
		container_id = i.id 
		container_name = i.name
		if container_name=='master':
			cm="sudo docker inspect --format '{{.State.Pid}}'"+" " +str(container_id)[:12]
			# print("cm",cm)
			stream = os.popen(cm) 
			container_pid = stream.read()
			container_pid=container_pid
			print("cid",container_pid)
			container_pid = int(container_pid) 
			master_info.append(container_pid)
			master_info.append(container_id)
			master_info.append(container_name)
	print("master created",master_info)
	global salveno,running_containers_info
	salveno+=1
	client.containers.run("worker:latest", name='slave'+str(salveno),command="python3 worker.py 1",network='finalprojectorchas_default', detach=True,links={'slave_db':'slave_db'})
	# client.containers.get('slave'+str(salveno)).exec_run("python3 worker.py 1", detach=True)
	updateinfo()
	print("slave created",running_containers_info)



def launch():
	global salveno,client
	salveno+=1
	client.containers.run("worker:latest", name='slave'+str(salveno),command="python3 worker.py 1",network='finalprojectorchas_default', detach=True,links={'slave_db':'slave_db'})
	client.containers.get('slave'+str(salveno)).exec_run("python3 worker.py 1", detach=True)
	print ("Succesfully launched a container")
	updateinfo()
	return





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
	return len(running_containers_info)+1





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
	print("checking start")
	auto_start(totalerq)
	print("checking stop")
	auto_stop(totalerq)
	

def fn():
	t = Timer(30.0, timeout)
	t.start()              

flag=0


@app.route('/api/v1/crash/master', methods=['POST'])
def crash_master():
	updateinfo()
	cmkillmas="sudo docker stop"+" " +"master"
	os.popen(cmkillmas)
	cmkillmas="sudo docker rm"+" " +"master"
	os.popen(cmkillmas)
	return jsonify({}),200
	

	


@app.route('/api/v1/crash/slave', methods=['POST'])
def crash_slave():
	updateinfo()
	cmkillsal="sudo docker stop"+" " +"slave"
	os.popen(cmkillsal)
	cmkillsal="sudo docker rm"+" " +"salve"
	os.popen(cmkillsal)
	return jsonify({}),200
	



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
	resp=write_rpc.call(data2)
	m=json.loads(resp)
	return jsonify(m['respo']),m['rcode']


if __name__ == '__main__':
	
	app.debug = True
	app.run('0.0.0.0', port=80)
	startup()
	# http_server = WSGIServer(("",5000),app)
	http_server.serve_forever()
