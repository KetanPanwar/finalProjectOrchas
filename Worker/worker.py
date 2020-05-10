from bson.json_util import dumps
import requests
import json
import re
from collections import OrderedDict
import pandas as pd
from datetime import datetime
import pika
import pymongo
import sys
import csv
from kazoo.client import KazooClient
import threading 

zk = KazooClient(hosts='3.212.113.11:2181')
zk.start()
# zk.ensure_path("/sample1")




#0:master 1:slave
m=sys.argv[1:][0]
print(m,type(m))
print(sys.argv)



myclient = pymongo.MongoClient("mongodb://0.0.0.0:27017/")
mydb = myclient["RideShare"]
usercol = mydb["users"]
ridecol = mydb["rides"]



connection = pika.BlockingConnection(
	pika.ConnectionParameters(host='3.212.113.11',heartbeat=0))
channel = connection.channel()




channel.exchange_declare(exchange='syncexchange', exchange_type='fanout')
result1 = channel.queue_declare(queue='')
channel.queue_bind(exchange='syncexchange',
				   queue=result1.method.queue)



def write_data_users(data):
	method=data['method']
	del data['method']
	if method=='post':
		key = list(data.keys())[0]
		usr = {key: data[key]}
		# resp_send = requests.post("http://users:5000/api/v1/db/read",json=usr)
		# res = json.loads(resp_send.content)
		# print(res,key,usr)
		if(data["query"] == "insert"):
			del(data["query"])
			usercol.insert_one(data)
			print('returning if ye wala')
			return {'respo':'done', 'rcode':200}
		elif(data["query"] == "update"):
			del(data["query"])
			user1 = data[list(data.keys())[1]]['username']
			usercol.find_and_modify(query={key: data[key]}, update={
										  "$push": {"users": user1}})
			print('returning elif')
			return {'respo':'done', 'rcode':201}

	elif method == 'delete':
		# username = data["username"]
		if data["dtype"] == 'del_one':
			print("in-=----------")
			del(data["dtype"])
			key = list(data.keys())[0]
			usr = data[key]
			# print(key,usr)
			usercol.delete_one({key: usr})
			print('returning if1')
			return {'respo':'done', 'rcode':200}
		else:
			del(data["dtype"])
			key = list(data.keys())[0]
			usr = data[key]
			print(key, usr)
			usercol.delete_one({key: usr})
			key = "created_by"
			d = {key: usr}
			resp_send = read_data_users(d)  #--------------------------------------------------
			if (resp_send['rcode'] == 400):
				print("chaarsoo")
				print('returning if2')
				return {'respo':'done', 'rcode':400}
			res = resp_send["respo"]
			print("res:", res, type(res), type(res[0]))
			for i in res:
				usercol.delete_one(i)

			resp_send = read_data_users({}) #----------------------------------------------
			res = resp_send["respo"]
			print("res in final", res, type(res), type(res[0]))
			for i in res:
				try:
					print("one")
					us = i["users"]
				except KeyError:
					print("two")
					continue
				rId = i["rideId"]
				if usr in us:
					print("us is", us)
					us.remove(usr)
					user1 = us.copy()
					print(us, user1)
					usercol.find_and_modify(query={"rideId": rId}, update={
												  "$set": {"users": user1}})
			print("final reutrn ")
			return {'respo':'done', 'rcode':200}
			# return res1
			# mongo.db.abcd.delete_(res1)




def write_data_rides(data):
	method=data['method']
	del data['method']
	if method == 'post':
		key = list(data.keys())[0]
		usr = {key: data[key]}
		# resp_send = requests.post("http://users:5000/api/v1/db/read",json=usr)
		# res = json.loads(resp_send.content)
		# print(res,key,usr)
		if(data["query"] == "insert"):
			del(data["query"])
			ridecol.insert_one(data)
			return {'respo':'done', 'rcode':200}
		elif(data["query"] == "update"):
			del(data["query"])
			user1 = data[list(data.keys())[1]]['username']
			ridecol.find_and_modify(query={key: data[key]}, update={
										  "$push": {"users": user1}})
			return {'respo':'done', 'rcode':201}

	elif method == 'delete':
		# username = data["username"]
		if data["dtype"] == 'del_one':
			print("in-=----------")
			del(data["dtype"])
			key = list(data.keys())[0]
			usr = data[key]
			# print(key,usr)
			ridecol.delete_one({key: usr})
			return {'respo':'done', 'rcode':200}
		else:
			del(data["dtype"])
			key = list(data.keys())[0]
			usr = data[key]
			print(key, usr)
			ridecol.delete_one({key: usr})
			key = "created_by"
			d = {key: usr}
			resp_send = read_data_rides(d) #----------------------------------------------
			if (resp_send["rcode"] == 400):
				print("chaarsoo")
				return {'respo':'done', 'rcode':400}
			res = resp_send["respo"]
			print("res:", res, type(res), type(res[0]))
			res=json.loads(res)
			for i in res:
				ridecol.delete_one(i)

			resp_send = read_data_rides({})  #-----------------------------------------
			res = resp_send["respo"]
			print("res in final", res, type(res), type(res[0]))
			res=json.loads(res)
			for i in res:
				try:
					print("one")
					us = i["users"]
				except KeyError:
					print("two")
					continue
				rId = i["rideId"]
				if usr in us:
					print("us is", us)
					us.remove(usr)
					user1 = us.copy()
					print(us, user1)
					ridecol.find_and_modify(query={"rideId": rId}, update={
												  "$set": {"users": user1}})

			print("final reutrn ")
			return {'respo':'done', 'rcode':200}
			# return res1
			# mongo.db.abcd.delete_(res1)


def clear_data_users():
	# print("1")
	try:
		usercol.drop()
		return {'respo':'done', 'rcode':200}
	except:
		return {'respo':'done', 'rcode':400}



def clear_data_rides():
	# print("1")
	try:
		ridecol.drop()
		return {'respo':'done', 'rcode':200}
	except:
		abort_code = 400
		return {'respo':'done', 'rcode':400}



def read_data_users(data):
	# print("1")
	if data:
		key = list(data.keys())[0]
		usr = data[key]
		d = {key: usr}
		if key == "source" or key == "userquery":
			d = data
	else:
		d = {}
	# print(usr)
	# print(d)
	if d and d.get("userquery", -1) == 1:
		par = usercol.find({}, {"_id": 0, "username": 1})
	else:
		par = usercol.find(d, {"_id": 0})
	print(par)
	# print(dumps(par))
	res = dumps(par)
	print(res)
	if(res=='[]'):
		print("return if")
		return {'respo':'', 'rcode':400}
	#print("Bale baale",res,type(res[0]))
	print("ret json",res)
	return {'respo':res, 'rcode':200}
	
	print("retu res",res[0])
	return {'respo':res[0], 'rcode':400}






def read_data_rides(data):
	# print("1")
	if data:
		key = list(data.keys())[0]
		usr = data[key]
		d = {key: usr}
		if key == "source" or key == "userquery":
			d = data
	else:
		d = {}
	# print(usr)
	# print(d)
	if d and d.get("userquery", -1) == 1:
		par = ridecol.find({}, {"_id": 0, "rideId": 1})
	else:
		par = ridecol.find(d, {"_id": 0})
	print(par)
	# print(dumps(par))
	res = dumps(par)
	print(res)
	if(res == '[]'):
		print("return if")
		return {'respo':'', 'rcode':400}
	#print("Bale baale",res,type(res[0]))
	print("ret json",res)
	return {'respo':res, 'rcode':200}
	
	print("retu res",res[0])
	return {'respo':res[0], 'rcode':400}



def coun_users():
	cou1=usercol.find({},{"_id":0,"count":1})
	res = json.loads(dumps(cou1))
	cou=-1
	for i in res:
		if (i):
			cou=i["count"]
	if cou==-1:
		usercol.insert_one({"count":1})
	else:
		usercol.find_and_modify(query={"count":cou},update={"$set" : {"count":cou+1}})
	return {'respo':'done', 'rcode':200}


def coun_rides():
	cou1=ridecol.find({},{"_id":0,"count":1})
	res = json.loads(dumps(cou1))
	cou=-1
	for i in res:
		if (i):
			cou=i["count"]
	if cou==-1:
		ridecol.insert_one({"count":1})
	else:
		ridecol.find_and_modify(query={"count":cou},update={"$set" : {"count":cou+1}})
	return {'respo':'done', 'rcode':200}




def reset_request_count_users():
	print("hey")
	cou1=usercol.find({},{"_id":0,"count":1})
	res = json.loads(dumps(cou1))
	for i in res:
		if (i):
			cou=i["count"]
	if cou==-1:usercol.insert_one({"count":0})
	else:usercol.find_and_modify(query={"count":cou},update={"$set" : {"count":0}})
	return {'respo':'done', 'rcode':200}



def reset_request_count_rides():
	print("hey")
	cou1=ridecol.find({},{"_id":0,"count":1})
	res = json.loads(dumps(cou1))
	for i in res:
		if (i):
			cou=i["count"]
	if cou==-1:ridecol.insert_one({"count":0})
	else:ridecol.find_and_modify(query={"count":cou},update={"$set" : {"count":0}})
	return {'respo':'done', 'rcode':200}



def get_request_count_users():
	print("hey")
	cou1=usercol.find({},{"_id":0,"count":1})
	res = json.loads(dumps(cou1))
	cou=-1
	for i in res:
		if (i):
			cou=i["count"]
	if cou==-1:
		return {'respo':0, 'rcode':200}
	return {'respo':cou, 'rcode':200}


def get_all_entries_users():
	print("hey")
	cou1=usercol.find()
	res = dumps(cou1)
	return {'respo':res, 'rcode':200}
	


def get_all_entries_rides():
	print("hey")
	cou1=ridecol.find()
	res = dumps(cou1)
	return {'respo':res, 'rcode':200}




def get_request_count_rides():
	print("hey")
	cou1=ridecol.find({},{"_id":0,"count":1})
	res = json.loads(dumps(cou1))
	cou=-1
	for i in res:
		if (i):
			cou=i["count"]
	if cou==-1:
		return {'respo':0, 'rcode':200}
	return {'respo':cou, 'rcode':200}






def callback_master(ch, method, props, body):
	print("entered master")
	data=json.loads(body)
	print(" [x] Received %r" % body)
	print(" [x] Done")
	data=json.loads(body)
	who=data['who']
	del data['who']
	if who=='users':
		print("its user")
		op=data['op']
		del data['op']
		if op=='write':
			print('caling write',data)
			ret=write_data_users(data)
			print("data Received")
			h=json.dumps(ret)
			print("data is ",h)
		elif op=='clear':
			ret=clear_data_users()
			h=json.dumps(ret)
		elif op=='coun':
			ret=coun_users()
			h=json.dumps(ret)
		elif op=='reset':
			ret=reset_request_count_users()
			h=json.dumps(ret)
		ch.basic_publish(exchange='',
					 routing_key=props.reply_to,
					 properties=pika.BasicProperties(correlation_id = \
														 props.correlation_id),
					 body=h)
		ch.basic_ack(delivery_tag=method.delivery_tag)
		print("response sent")
	

	elif who=='rides':
		print("its rides")
		op=data['op']
		del data['op']
		if op=='write':
			ret=write_data_rides(data)
			h=json.dumps(ret)
			print("data is ",h)
		elif op=='clear':
			ret=clear_data_rides()
			h=json.dumps(ret)
		elif op=='coun':
			ret=coun_rides()
			h=json.dumps(ret)
		elif op=='reset':
			ret=reset_request_count_rides()
			h=json.dumps(ret)
		ch.basic_publish(exchange='',
					 routing_key=props.reply_to,
					 properties=pika.BasicProperties(correlation_id = \
														 props.correlation_id),
					 body=h)
		ch.basic_ack(delivery_tag=method.delivery_tag)
		print("response sent")
	print("entering sync")
	channel.basic_publish(exchange='syncexchange', routing_key='', body=body)
	print("exiting sync")

re=0
def callback_slave(ch, method, props, body):
	global re
	re+=1
	with open('innovators.csv', 'w', newline='') as file:
		writer = csv.writer(file)
		writer.writerow([re])
	data=json.loads(body)
	print(" [x] Received %r" % body)
	print(" [x] Done")
	data=json.loads(body)
	who=data['who']
	del data['who']
	if who=='users':
		print("its user",data)
		if data.get('entire',0)==1:
			ret=get_all_entries_users()
		elif data.get('readcou',0)==1:
			ret=get_request_count_users()
		else:
			ret=read_data_users(data)
	elif who=='rides':
		if data.get('entire',0)==1:
			ret=get_all_entries_rides()
		elif data.get('readcou',0)==1:
			ret=get_request_count_rides()
		else:
			ret=read_data_rides(data)
	print("ret slave",ret)
	# h=ret
	h=json.dumps(ret)
	print(h)
	ch.basic_publish(exchange='',
					 routing_key=props.reply_to,
					 properties=pika.BasicProperties(correlation_id = \
														 props.correlation_id),
					 body=h)
	ch.basic_ack(delivery_tag=method.delivery_tag)
	print("response sent")


def callback_slave_sync(ch, method, properties, body):
	print("entered slave sync")
	data=json.loads(body)
	print(" [x] Received %r" % body)
	print(" [x] Done")
	data=json.loads(body)
	who=data['who']
	del data['who']
	if who=='users':
		print("its user")
		op=data['op']
		del data['op']
		if op=='write':
			print('caling write',data)
			ret=write_data_users(data)
			print("data Received")
			h=json.dumps(ret)
			print("data is ",h)
		elif op=='clear':
			ret=clear_data_users()
			h=json.dumps(ret)
		elif op=='coun':
			ret=coun_users()
			h=json.dumps(ret)
		elif op=='reset':
			ret=reset_request_count_users()
			h=json.dumps(ret)
		# ch.basic_publish(exchange='',
		# 			 routing_key=props.reply_to,
		# 			 properties=pika.BasicProperties(correlation_id = \
		# 												 props.correlation_id),
		# 			 body=h)
		# ch.basic_ack(delivery_tag=method.delivery_tag)
		print("response sent")
	

	elif who=='rides':
		print("its rides")
		op=data['op']
		del data['op']
		if op=='write':
			ret=write_data_rides(data)
			h=json.dumps(ret)
			print("data is ",h)
		elif op=='clear':
			ret=clear_data_rides()
			h=json.dumps(ret)
		elif op=='coun':
			ret=coun_rides()
			h=json.dumps(ret)
		elif op=='reset':
			ret=reset_request_count_rides()
			h=json.dumps(ret)
		# ch.basic_publish(exchange='',
		# 			 routing_key=props.reply_to,
		# 			 properties=pika.BasicProperties(correlation_id = \
		# 												 props.correlation_id),
		# 			 body=h)
		# ch.basic_ack(delivery_tag=method.delivery_tag)
		print("response sent")

def callback_slave_data_up(data):
	print(" [x] Received %r" % data)
	print(" [x] Done")
	who=data['who']
	del data['who']
	if who=='users':
		print("its user")
		op=data['op']
		del data['op']
		if op=='write':
			print('caling write',data)
			ret=write_data_users(data)
			print("data Received")
			h=json.dumps(ret)
			print("data is ",h)
		elif op=='clear':
			ret=clear_data_users()
			h=json.dumps(ret)
		elif op=='coun':
			ret=coun_users()
			h=json.dumps(ret)
		elif op=='reset':
			ret=reset_request_count_users()
			h=json.dumps(ret)
		print("response sent")
	

	elif who=='rides':
		print("its rides")
		op=data['op']
		del data['op']
		if op=='write':
			ret=write_data_rides(data)
			h=json.dumps(ret)
			print("data is ",h)
		elif op=='clear':
			ret=clear_data_rides()
			h=json.dumps(ret)
		elif op=='coun':
			ret=coun_rides()
			h=json.dumps(ret)
		elif op=='reset':
			ret=reset_request_count_rides()
			h=json.dumps(ret)
		print("response sent")





if m=='1':
	resp_send = requests.post("http://52.72.92.96:80/api/v1/db/copydbtoslave", json={})
	s = json.loads(resp_send.content)
	# print(s,type(s),type(s[0]))
	l=[]
	if s:l=s[0].split('}')
	fnl=[]
	for i in l:
		if i:fnl.append(i+'}')
	for i in fnl:
		print(i)
		print("hey")
		mio=json.loads(i)
		print(mio,type(mio))
		callback_slave_data_up(mio)


if m=='0':
	msg=zk.get("/worker/master")[0]
	print(msg)
	zk.set("/worker/master",b"")
	master_pid=msg.decode().split()[1]
	zk.create(path=("/worker/master/"+master_pid),value=b'working')
if m=='1':
	msg=zk.get("/worker/slave")[0]
	print(msg)
	zk.set("/worker/slave",b"")
	slave_pid=msg.decode().split()[1]
	zk.create(path=("/worker/slave/"+slave_pid),value=b'working')

	@zk.DataWatch("/worker/slave/"+slave_pid)
	def slaveswatch(data,stat):
		if data:
			data1=data.decode()
			if data1=='changed':
				print("And then the slave said : My watch begins :-)")
				zk.delete("/worker/slave/"+slave_pid)
				print("deleted kazoo node for slave")
				zk.create("/worker/master/"+slave_pid, b"working")
				print("znode converted to master")
				# channel.stop_consuming()
				change_behaviour()


def change_behaviour():
	global channel,connection,t1
	t1.stop()
	# channel.stop_consuming()
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='3.212.113.11',heartbeat=0))
	channel=connection.channel()
	channel.exchange_declare(exchange='syncexchange', exchange_type='fanout')
	result1 = channel.queue_declare(queue='')
	channel.queue_bind(exchange='syncexchange',
				   queue=result1.method.queue)
	result = channel.queue_declare(queue='rpc_queue_write')
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(queue='rpc_queue_write', on_message_callback=callback_master)
	print("behavious changed")
	channel.start_consuming()


if m=='0':
	result = channel.queue_declare(queue='rpc_queue_write')
if m=='1':
	result = channel.queue_declare(queue='rpc_queue_read')



if m=='1':
	channel.basic_consume(
	queue=result1.method.queue, on_message_callback=callback_slave_sync)



channel.basic_qos(prefetch_count=1)
if m=='0':
	channel.basic_consume(queue='rpc_queue_write', on_message_callback=callback_master)
if m=='1':
	channel.basic_consume(queue='rpc_queue_read', on_message_callback=callback_slave)

print(" [x] Awaiting RPC requests")
# channel.start_consuming()
t1=threading.Thread(target=channel.start_consuming)
t1.start()
print("reached here")