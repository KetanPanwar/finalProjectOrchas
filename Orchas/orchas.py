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
			pika.ConnectionParameters(host='rabbitmq'))

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
			pika.ConnectionParameters(host='rabbitmq'))

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
