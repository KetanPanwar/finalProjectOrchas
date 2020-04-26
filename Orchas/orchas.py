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
			pika.ConnectionParameters(host='18.210.117.50'))

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
			pika.ConnectionParameters(host='18.210.117.50'))

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


def updateinfo():
	global running_containers_info,client
	running_containers_info=[]
	running_containers = client.containers.list() 
	for i in running_containers:
		container_id = i.id 
		container_name = i.name
		if 'salvespaw' in container_name:
			stream = os.popen( "sudo docker inspect --format '{{.State.Pid}}'" +'"'+ str(container_id)+'"') 
			container_pid = stream.read() 
			container_pid = int(container_pid) 
			running_containers_info.append( [str(container_pid),str(container_id),str(container_name)])
	running_containers_info.sort()




def launch():
	global salveno,client
	salveno+=1
	client.containers.run("slave:latest", name='salvespaw'+str(salveno), detach=True)
	client.containers.get('salvespaw'+str(salveno)).exec_run("python3 slave.py 1", detach=True)
	print ("Succesfully launched a container")
	updateinfo()
	return





def stop():
	global running_containers_info
	client.containers.get(running_containers_info[-1][-1]).stop()
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
	totalerq=coureads-coureadsprev
	coureadsprev=coureads
	auto_start(totalerq)
	auto_stop(totalerq)
	fn()

def fn():
	t = Timer(120.0, timeout)
	t.start()              

flag=0


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
	app.run('0.0.0.0', port=5000)
	# http_server = WSGIServer(("",5000),app)
	http_server.serve_forever()
