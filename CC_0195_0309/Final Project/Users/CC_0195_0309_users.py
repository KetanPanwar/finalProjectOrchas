# from gevent import monkey
#  monkey.patch_all()
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
# from gevent.pywsgi import WSGIServer

app = Flask(__name__)
# app.config['MONGO_URI'] = 'mongodb://user_db:27017/RideShareuser'
app.config['JSON_SORT_KEYS'] = False
@app.route('/')
@app.route('/index')
def index():
	return "Hello, World!"


ride_id = 1248

dataset = pd.read_csv("AreaNameEnum.csv")
dataset = dataset.iloc[:, :].values
n_places = len(dataset)

# for i in dataset:
#         print(i[0],i[1])

# mongo = PyMongo(app)

# @app.route('/api/v1/try', methods=['PUT'])
# def get_all_docs():
#         req_data = request.get_json()
#         mongo.db.abcd.insert_one(req_data)
#         return "Inserted"


def coun():
	data={}
	data['op']='coun'
	data['method']='post'
	data['who']='users'
	resp_send = requests.post("http://52.72.92.96:80/api/v1/db/write", json=data)
	# cou1=mongo.db.abcd.find({},{"_id":0,"count":1})
	# res = json.loads(dumps(cou1))
	# cou=-1
	# for i in res:
	#     if (i):
	#         cou=i["count"]
	# if cou==-1:
	#     mongo.db.abcd.insert_one({"count":1})
	# else:
	#     mongo.db.abcd.find_and_modify(query={"count":cou},update={"$set" : {"count":cou+1}})


def validate_pswd(password):
	if len(password) != 40:
		return False
	for i in password:
		if(not i.isdigit() and i not in 'abcdef' and i not in 'ABCDEF'):
			return False
	return True


@app.route('/api/v1/users', methods=['PUT', 'POST', 'DELETE', 'HEAD'])
def add_user():
	coun()
	if request.method == 'PUT':
		try:
			data = request.get_json()
		except:
			print('abort code')
			abort_code = 400
			return jsonify({}), abort_code
		usr = data["username"]
		key = list(data.keys())[0]
		usr = {key: data[key]}
		usr['who']='users'
		resp_send = requests.post("http://52.72.92.96:80/api/v1/db/read", json=usr)
		if(resp_send.status_code == 400):
			pattern = re.compile(r'\b[0-9a-f]{40}\b')
			#match = re.match(pattern, data["password"])
			match = validate_pswd(data["password"])
			if match:
				data['op']='write'
				data['method']='post'
				data['query']='insert'
				data['who']='users'
				resp_send = requests.post(
					"http://52.72.92.96:80/api/v1/db/write", json=data)
				return jsonify({}), 201
			else:
				print('else1')
				return jsonify({}), 400
		else:
			print('else2')
			return jsonify({}), 400
		return jsonify({}), 500
	else:
		return jsonify({}), 405




@app.route('/api/v1/users/<username>', methods=['DELETE', 'GET', 'PUT', 'POST', 'HEAD'])
def remove_user(username):
	coun()
	if request.method == 'DELETE':
		data = {"username": username}
		data['who']='users'
		resp_send = requests.post(
			"http://52.72.92.96:80/api/v1/db/read", json=data)
		if(resp_send.status_code == 400):
			return jsonify({}), 400
		elif(resp_send.status_code == 200):
			data['op']='write'
			data["dtype"] = "del_two"
			data['who']='rides'
			data['method']='delete'
			resp_del = requests.delete(
				"http://52.72.92.96:80/api/v1/db/write", json=data)
			data['who']='users'
			resp_del = requests.delete(
				"http://52.72.92.96:80/api/v1/db/write", json=data)
		return jsonify({}), 200
	else:
		return jsonify({}), 405


# @app.route('/api/v2/users/<username>', methods=['GET'])
# def get_all_docs2(username):
#         par = mongo.db.abcd.find({"username" : username},{ "_id": 0 })
#         # k=dumps(par)
#         # i=k.index("username")
#         # return jsonify(k[i-1:-2])
#         s = dumps(par)
#         s = s[1:-1]
#         return s
#         # return jsonify(par)
#         # return "Deleted"
@app.route('/api/v1/users', methods=['GET', 'DELETE', 'HEAD'])
def list_all_users():
	print("hey")
	coun()

	if request.method == 'GET':
		data={}
		data = {"userquery": 1}
		data['who']='users'
		resp_send = requests.post(
			"http://52.72.92.96:80/api/v1/db/read", json=data)
		s = dumps(resp_send.content)
		print(s)
		# print(s,resp_send.content)
		res = json.loads(resp_send.content)
		res = json.loads(res)
		print(res)
		res1 = jsonify(res)
		print(type(res))
		# for i in res:
		# 	print(res[i])

		qres = []
		for i in res:
			if (i):
				qres.append(i['username'])

		# for i in res:
		#     print(i)
		if len(qres) == 0:
			return jsonify({}), 204
		res2 = jsonify(qres)
		return res2
	else:
		return jsonify({}), 405


@app.route('/api/v1/_count', methods=['PUT', 'GET', 'HEAD'])
def get_request_count():
	print("hey")

	if request.method == 'GET':
		data={}
		data['who']='users'
		data['readcou']=1
		resp_send = requests.post(
			"http://52.72.92.96:80/api/v1/db/read", json=data)
		res = json.loads(resp_send.content)
		# res=json.loads(res)
		return jsonify([res]), 200


		cou1=mongo.db.abcd.find({},{"_id":0,"count":1})
		res = json.loads(dumps(cou1))
		cou=-1
		for i in res:
			if (i):
				cou=i["count"]
		if cou==-1:return jsonify(0)
		res2 = jsonify(cou)
		return res2 , 200
	else:
		return jsonify({}), 405

@app.route('/api/v1/_count', methods=['PUT', 'GET', 'DELETE', 'HEAD'])
def reset_request_count():
	print("hey")

	if request.method == 'DELETE':
		data={}
		data['op']='reset'
		data['method']='post'
		data['who']='users'
		resp_send = requests.post("http://52.72.92.96:80/api/v1/db/write", json=data)


		# cou1=mongo.db.abcd.find({},{"_id":0,"count":1})
		# res = json.loads(dumps(cou1))
		# for i in res:
		# 	if (i):
		# 		cou=i["count"]
		# if cou==-1:mongo.db.abcd.insert_one({"count":0})
		# else:mongo.db.abcd.find_and_modify(query={"count":cou},update={"$set" : {"count":0}})
		return jsonify({}),200
	else:
		return jsonify({}), 405




@app.route('/api/v1/showdbsown', methods=['PUT', 'GET', 'HEAD'])
def get_all_entries():
	print("hey")

	if request.method == 'GET':
		data={}
		data['who']='users'
		data['entire']=1
		resp_send = requests.post(
			"http://52.72.92.96:80/api/v1/db/read", json=data)
		res = json.loads(resp_send.content)
		res=json.loads(res)
		return jsonify(res)


		cou1=mongo.db.abcd.find()
		res = json.loads(dumps(cou1))
		res2 = jsonify(res)
		return res2
	else:
		return jsonify({}), 405


@app.route('/api/v1/db/write', methods=['POST', 'DELETE'])
def write_data():
	if request.method == 'POST':
		try:
			data = request.get_json()
		except:
			abort_code = 400
			return jsonify({}), abort_code
		# print(data)
		key = list(data.keys())[0]
		usr = {key: data[key]}
		# resp_send = requests.post("http://users:80/api/v1/db/read",json=usr)
		# res = json.loads(resp_send.content)
		# print(res,key,usr)
		if(data["query"] == "insert"):
			try:
				data = request.get_json()
			except:
				abort_code = 400
				return jsonify({}), abort_code
			del(data["query"])
			mongo.db.abcd.insert_one(data)
			return jsonify({}), 200
		elif(data["query"] == "update"):
			del(data["query"])
			user1 = data[list(data.keys())[1]]['username']
			mongo.db.abcd.find_and_modify(query={key: data[key]}, update={
										  "$push": {"users": user1}})
			return jsonify({}), 201

	elif request.method == 'DELETE':
		try:
			data = request.get_json()
		except:
			abort_code = 400
			return jsonify({}), abort_code
		# username = data["username"]
		if data["dtype"] == 'del_one':
			print("in-=----------")
			del(data["dtype"])
			key = list(data.keys())[0]
			usr = data[key]
			# print(key,usr)
			mongo.db.abcd.delete_one({key: usr})
			return jsonify({})
		else:
			del(data["dtype"])
			key = list(data.keys())[0]
			usr = data[key]
			print(key, usr)
			mongo.db.abcd.delete_one({key: usr})
			key = "created_by"
			d = {key: usr}
			resp_send = requests.post(
				"http://34.194.124.13:80/api/v1/db/read", json=d)
			if (resp_send.status_code == 400):
				print("chaarsoo")
				return jsonify({}), 400
			res = json.loads(resp_send.content)
			print("res:", res, type(res), type(res[0]))
			for i in res:
				mongo.db.abcd.delete_one(i)

			resp_send = requests.post(
				"http://34.194.124.13:80/api/v1/db/read", json={})
			res = json.loads(resp_send.content)
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
					mongo.db.abcd.find_and_modify(query={"rideId": rId}, update={
												  "$set": {"users": user1}})

			return ""
			# return res1
			# mongo.db.abcd.delete_(res1)


@app.route('/api/v1/db/read', methods=['POST'])
def read_data():
	# print("1")
	try:
		data = request.get_json()
	except:
		abort_code = 400
		return jsonify({}), abort_code
	# print(data)
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
		par = mongo.db.abcd.find({}, {"_id": 0, "username": 1})
	else:
		par = mongo.db.abcd.find(d, {"_id": 0})
	# print(dumps(par))
	res = json.loads(dumps(par))
	if(len(res) == 0):
		return jsonify({}), 400
	#print("Bale baale",res,type(res[0]))

	return json.dumps(res)

	return res[0]


@app.route('/api/v1/db/clear', methods=['POST'])
def clear_data():
	# print("1")
	data={}
	data['method']='post'
	data['op']='clear'
	data['who']='users'
	resp_send = requests.post(
			"http://52.72.92.96:80/api/v1/db/clear", json=data)
	return jsonify({}), 200


if __name__ == '__main__':
	app.debug = True
	app.run('0.0.0.0', port=5000)
	# http_server = WSGIServer(("",80),app)
	http_server.serve_forever()
