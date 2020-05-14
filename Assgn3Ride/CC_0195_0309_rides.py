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
# app.config['MONGO_URI'] = 'mongodb://rides_db:27017/RideSharerides'
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
	data['who']='rides'
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





@app.route('/api/v1/rides', methods=['PUT', 'POST', 'DELETE', 'HEAD'])
def create_new_ride():
	coun()
	if request.method == 'POST':
		global ride_id
		ride_id += 1
		try:
			data = request.get_json()
		except:
			abort_code = 400
			return jsonify({}), abort_code
		usr = data["created_by"]
		print(usr)
		usrd = {"username": usr}
		print(usrd)
		print("Username", data["created_by"])
		print("timestamp", data['timestamp'])
		print("source", data['source'], type(data['source']))
		print("destination", data['destination'], type(data['destination']))
		resp_send = requests.get(
			 "http://Assignment3-1703098.us-east-1.elb.amazonaws.com:80/api/v1/users",
	headers={'Origin': '52.203.103.216'},)
		d=json.loads(resp_send.content)
		response = resp_send.json()
		# resp_send = requests.post(
		#     "http://Assignment3-1703098.us-east-1.elb.amazonaws.com:80/api/v1/db/read", json=usrd)
		# print(resp_send.content)
		# s = dumps(resp_send.content)
		# print(s)
		# res = json.loads(s)
		# print(res, len(res))
		# if(resp_send.status_code == 200):
		if (usr in response):
			print("entered 200")
			times = data["timestamp"]
			source = data["source"]
			destination = data["destination"]

			def datet(s):
				try:
					print("inside try")
					dat = datetime.strptime(s, '%d-%m-%Y:%S-%M-%H').time()
					dat_t = datetime.strptime(s, '%d-%m-%Y:%S-%M-%H')
				except ValueError:
					print("inside error")
					return -1
				return dat_t
			time_s = datet(times)
			if(time_s == -1):
				print("times -1")
				return jsonify({}), 400
			if time_s < datetime.now():
				print("times less")
				return jsonify({}), 400
			source=int(source)
			destination=int(destination)
			if( source >= 1 and source <= n_places and destination >= 1 and destination <= n_places and source != destination):
				sourced = dataset[source][1]
				destinationd = dataset[destination][1]
				print("sd", source, destination, sourced, destinationd)
			else:
				print("inside else")
				return jsonify({}), 400
			fata = OrderedDict()
			fata["rideId"] = ride_id
			fata["created_by"] = usr
			fata["users"] = [usr]
			fata["timestamp"] = times
			fata["source"] = sourced
			fata["destination"] = destinationd
			fata["query"] = "insert"
			fata["op"]="write"
			fata["method"]="post"
			fata["who"]="rides"
			resp_send = requests.post(
				"http://52.72.92.96:80/api/v1/db/write", json=fata)
			print("recieved")
			return jsonify({}), 201
		else:
			print("inside final else")
			return jsonify({}), 400
		return jsonify({}), 500
	else:
		return jsonify({}), 405


@app.route('/api/v1/rides', methods=['PUT', 'GET', 'DELETE', 'HEAD'])
def display_up_rides():
	print("hey")
	coun()

	if request.method == 'GET':
		source = request.args.get('source')
		destination = request.args.get('destination')
		source = int(source)
		destination = int(destination)
		if(type(source) == int and type(destination) == int and source >= 1 and source <= n_places and destination >= 1 and destination <= n_places):
			source = dataset[source][1]
			destination = dataset[destination][1]
		else:
			return jsonify({}), 400
		if(source == destination):
			return jsonify({}), 400
		data = {"source": source, "destination": destination}
		print(source, destination)
		data['who']='rides'
		resp_send = requests.post(
			"http://52.72.92.96:80/api/v1/db/read", json=data)
		s = dumps(resp_send.content)
		print(s)
		# print(s,resp_send.content)
		res = json.loads(resp_send.content)
		print(res)
		if res:
			res=json.loads(res)
			res1 = jsonify(res)
		# print(type(res),type(res[0]))

		def datet(s):
			dat = datetime.strptime(s, '%d-%m-%Y:%S-%M-%H')
			return dat
		qres = []
		for i in res:
			dat = datet(i['timestamp'])
			temp = {}
			if (dat > datetime.now()):
				temp["rideId"] = i["rideId"]
				temp["username"] = i["created_by"]
				temp["timestamp"] = i["timestamp"]
				qres.append(temp)

		# for i in res:
		#     print(i)
		if len(qres) == 0:
			return jsonify({}), 204
		res2 = jsonify(qres)
		return res2
	else:
		return source, destination
		return jsonify({}), 405


@app.route('/api/v1/rides/<rideId>', methods=['PUT', 'GET', 'POST', 'DELETE', 'HEAD'])
def details_of_rides(rideId):
	coun()
	if request.method == 'GET':
		data = {"rideId": int(rideId)}
		data['who']='rides'
		resp_send = requests.post(
			"http://52.72.92.96:80/api/v1/db/read", json=data)
		s = dumps(resp_send.content)
		# print(s,resp_send.content)
		res = json.loads(resp_send.content)

		print(res,type(res))
		if(len(res) == 0):
			return jsonify({}), 204
		else:
			res=json.loads(res)
			fata = OrderedDict()
			res = res[0]
			fata["rideId"] = res["rideId"]
			fata["created_by"] = res["created_by"]
			fata["users"] = res["users"]
			fata["timestamp"] = res["timestamp"]
			fata["source"] = res["source"]
			fata["destination"] = res["destination"]
			return jsonify(dict(fata.items())), 200
	elif(request.method == "POST"):
		try:
			data = request.get_json()
		except:
			abort_code = 400
			return jsonify({}), abort_code
		usr = {"username": data["username"]}
		data1 = {"rideId": int(rideId)}
		# print("ent",type(data1))
		usr['who']='users'
		resp_send = requests.post("http://52.72.92.96:80/api/v1/db/read", json=usr)
		data1["who"]='rides'
		resp_send1 = requests.post(
			"http://52.72.92.96:80/api/v1/db/read", json=data1)
		if(resp_send.status_code == 200 and resp_send1.status_code == 200):
			od = OrderedDict()
			od["rideId"] = int(rideId)
			od["username"] = usr
			od["query"] = "update"
			od['op']="write"
			od["who"]="rides"
			od["method"]='post'
			d = json.loads(resp_send1.content)
			d=json.loads(d)
			print(d,type(d))

			# return d['users']
			if usr["username"] not in d[0]["users"]:
				resp_send = requests.post(
					"http://52.72.92.96:80/api/v1/db/write", json=od)
				print(resp_send.status_code)
				return jsonify({}), 200
			else:
				return jsonify({}), 400
		else:
			return jsonify({}), 204
	elif(request.method == 'DELETE'):
		data = {"rideId": int(rideId)}
		data["who"]='rides'
		resp_send1 = requests.post(
			"http://52.72.92.96:80/api/v1/db/read", json=data)
		if resp_send1.status_code == 400:
			return jsonify({}), 400
		elif (resp_send1.status_code == 200):
			data["dtype"] = "del_one"
			data["who"]='rides'
			data['op']='write'
			data["method"]='delete'
			resp_send = requests.delete(
				"http://52.72.92.96:80/api/v1/db/write", json=data)
			return jsonify({}), resp_send.status_code

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


@app.route('/api/v1/rides/count', methods=['PUT', 'GET', 'DELETE', 'HEAD'])
def count_no_of_rides():
	print("hey")
	coun()

	if request.method == 'GET':
		data={}
		data = {"userquery": 1}
		data["who"]="rides"
		resp_send = requests.post(
			"http://52.72.92.96:80/api/v1/db/read", json=data)
		s = dumps(resp_send.content)
		print(s)
		# print(s,resp_send.content)
		res = json.loads(resp_send.content)
		res=json.loads(res)
		print(res)
		res1 = jsonify(res)
		# print(type(res),type(res[0]))
		resl=0
		for i in res:
			if (i):
				resl+=1

		# for i in res:
		#     print(i)
		res2 = jsonify(resl)
		return res2
	else:
		return jsonify({}), 405


@app.route('/api/v1/_count', methods=['PUT', 'GET', 'HEAD'])
def get_request_count():
	print("hey")

	if request.method == 'GET':
		data={}
		data['who']='rides'
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
		data['who']='rides'
		resp_send = requests.post("http://52.72.92.96:80/api/v1/db/write", json=data)


		# cou1=mongo.db.abcd.find({},{"_id":0,"count":1})
		# res = json.loads(dumps(cou1))
		# for i in res:
		#   if (i):
		#       cou=i["count"]
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
		data['who']='rides'
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
		# resp_send = requests.post("http://users:5000/api/v1/db/read",json=usr)
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
				"http://52.203.103.216:80/api/v1/db/read", json=d)
			if (resp_send.status_code == 400):
				print("chaarsoo")
				return jsonify({}), 400
			res = json.loads(resp_send.content)
			print("res:", res, type(res), type(res[0]))
			for i in res:
				mongo.db.abcd.delete_one(i)

			resp_send = requests.post(
				"http://52.203.103.216:80/api/v1/db/read", json={})
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
		par = mongo.db.abcd.find({}, {"_id": 0, "rideId": 1})
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
	data['who']='rides'
	resp_send = requests.post(
			"http://52.72.92.96:80/api/v1/db/clear", json=data)
	# res = json.loads(resp_send.content)
		# res=json.loads(res)
	return jsonify({}), 200


if __name__ == '__main__':
	app.debug = True
	app.run('0.0.0.0', port=5050)
	#http_server = WSGIServer(("",5000),app)
	# http_server.serve_forever()
