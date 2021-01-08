import math
import os
import threading
import time
from math import sin, cos, sqrt, atan2, radians
from math import radians, cos, sin, asin, sqrt
from logging.handlers import RotatingFileHandler

import geopy.distance
from threading import Thread
import logging


class Restaurant:
	def __init__(self):
		self.minute = 1
		self.start_location = (28.6037837, 77.0569728)
		self.branches = [(28.5921784, 77.0598052), (28.5904073, 77.0558999),
						 (28.5885043, 77.0592473), (28.5882877, 77.0546661)]
		self.delivery_boys = {
			"boy1": {"name": "Saurav", "bike": "Bullet", "location": self.start_location, "status": 0, "TKC": 0, "TE": 0, "TOH": 0, "TOD": 0},
			"boy2": {"name": "Sonu", "bike": "Kawasaki", "location": self.start_location, "status": 0, "TKC": 0, "TE": 0, "TOH": 0, "TOD": 0},
			"boy3": {"name": "Prateek", "bike": "Pulsar", "location": self.start_location, "status": 0, "TKC": 0, "TE": 0, "TOH": 0, "TOD": 0}}
		self.orders_list = [1, "Rajiv", 28.5989514, 77.0516513, 200], \
						   [2, "Pooja", 28.5963233, 77.0452676, 124], \
						   [3, "Simran", 28.5947972, 77.0362554, 343], \
						   [4, "Dhruv", 28.6156326, 77.0297967, 552], \
						   [5, "Sammy", 28.6046691, 77.0315347, 232], \
						   [6, "Rahul", 28.5966247, 77.0591508, 551], \
						   [7, "Koko", 28.5975384, 77.0644401, 232], \
						   [8, "Preeti", 28.5682006, 77.0680664, 120], \
						   [9, "Payal", 28.5714937, 77.0722989, 60], \
						   [10, "Baabul", 28.5651052, 77.0466785, 220]

	def order(self):
		for ordr in self.orders_list:
			logger1.info(f"{ordr[0]}\\\\\\{ordr[1]}\\\\\\{ordr[2]},{ordr[3]}\\\\\\-\\\\\\-\\\\\\order_received\\\\\\-")
			loc = (ordr[2], ordr[3])
			dist, branch = self.get_nearest_branch(loc)
			d_time = dist
			threads = []
			print("threadcount",threading.active_count())
			while threading.active_count() > 3:
				print("All delivery boys are occupied. Waiting...")
				time.sleep(1)
			db_dist, d_boy = self.get_delivery_boy(self.branches[branch], ordr)
			tdist = dist+db_dist
			self.delivery_boys[d_boy]["status"] = 1
			threads.append(Thread(target=self.deliver, args=(tdist, math.ceil(db_dist), math.ceil(d_time), branch, d_boy, ordr)))
			threads[-1].start()

			print("waiting for next order")
			time.sleep(1*self.minute)
		for k, v in self.delivery_boys.items():
			logger2.info(f"{v['name']}----{v['TKC']}----{v['TE']}----{v['TOH']}----{v['TOD']}")

	def deliver(self, distance,atime, dtime, branch, dboy,order_details):
		time.sleep(atime*self.minute)
		logger1.info(f"{order_details[0]}\\\\\\{order_details[1]}\\\\\\{order_details[2]},{order_details[3]}\\\\\\{self.delivery_boys[dboy]['name']}\\\\\\-\\\\\\order_picked\\\\\\-")
		time.sleep(dtime*self.minute)
		self.delivery_boys[dboy]["location"] = self.branches[branch]
		self.delivery_boys[dboy]["status"] = 0
		print("delivered")
		logger1.info(f"{order_details[0]}\\\\\\{order_details[1]}\\\\\\{order_details[2]},{order_details[3]}\\\\\\{self.delivery_boys[dboy]['name']}\\\\\\{order_details[4]}\\\\\\order_delivered\\\\\\{atime+dtime}min")
		self.delivery_boys[dboy]["TKC"] += distance
		self.delivery_boys[dboy]["TOD"] += (atime+dtime)
		self.delivery_boys[dboy]["TE"] += (order_details[4])
		self.delivery_boys[dboy]["TOH"] += 1

	def get_delivery_boy(self, b_location, order_details):
		min = 999999
		for k, v in self.delivery_boys.items():
			if v["status"] == 0:
				dist = self.get_distance(b_location, v["location"])
				if min > dist:
					min = dist
					boy = k
		logger1.info(f"{order_details[0]}\\\\\\{order_details[1]}\\\\\\{order_details[2]},{order_details[3]}\\\\\\{self.delivery_boys[boy]['name']}\\\\\\-\\\\\\delivery_boy_assigned\\\\\\-")
		return min, boy

	def get_distance(self, coords_1, coords_2):
		return geopy.distance.geodesic(coords_1, coords_2).km

	def get_nearest_branch(self, ord_loc):
		min = 9999999
		branch = -1
		for i, x in enumerate(self.branches):
			dist = self.get_distance(x, ord_loc)
			if min > dist:
				min = dist
				branch = i
		return min, branch


def create_rotating_logger(name,path):
	"""
	Creates a rotating log
	"""
	logger = logging.getLogger(name)
	# Assigning Level
	logger.setLevel(logging.DEBUG)
	# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

	# add a rotating handler
	handler = RotatingFileHandler(path, mode='a+', maxBytes=2*1048576, backupCount=20)
	# handler.setFormatter(formatter)
	logger.addHandler(handler)

	return logger



if __name__ == '__main__':
	restaurant = Restaurant()
	LOG_PATH = os.path.join(os.getcwd(), "Logs")
	logger1 = create_rotating_logger("First Log File", os.path.join(LOG_PATH, "LogFile1.log"))
	logger2 = create_rotating_logger("Second Log File", os.path.join(LOG_PATH, "LogFile2.log"))
	restaurant.order()

