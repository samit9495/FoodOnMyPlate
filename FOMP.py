import errno
import math
import os
import threading
import time
from logging.handlers import RotatingFileHandler
import geopy.distance
import logging


class Restaurant:
	def __init__(self):
		self.minute = 2  # to run the code faster I am keeping minutes second to 2 instead of 60 seconds. To run it for exact timings change this to 60
		self.start_location = (28.6037837, 77.0569728) # Head Branch Location
		self.branches = [(28.5921784, 77.0598052), (28.5904073, 77.0558999),
						 (28.5885043, 77.0592473), (28.5882877, 77.0546661)]   # All branch locations
		self.delivery_boys = {
			"boy1": {"name": "Saurav", "bike": "Bullet", "location": self.start_location, "status": 0, "TKC": 0, "TE": 0, "TOH": 0, "TOD": 0},
			"boy2": {"name": "Sonu", "bike": "Kawasaki", "location": self.start_location, "status": 0, "TKC": 0, "TE": 0, "TOH": 0, "TOD": 0},
			"boy3": {"name": "Prateek", "bike": "Pulsar", "location": self.start_location, "status": 0, "TKC": 0, "TE": 0, "TOH": 0, "TOD": 0}}    # Delivery boys details and other parameters

		self.orders_list = [1, "Rajiv", 28.5989514, 77.0516513, 200], \
						   [2, "Pooja", 28.5963233, 77.0452676, 124], \
						   [3, "Simran", 28.5947972, 77.0362554, 343], \
						   [4, "Dhruv", 28.6156326, 77.0297967, 552], \
						   [5, "Sammy", 28.6046691, 77.0315347, 232], \
						   [6, "Rahul", 28.5966247, 77.0591508, 551], \
						   [7, "Koko", 28.5975384, 77.0644401, 232], \
						   [8, "Preeti", 28.5682006, 77.0680664, 120], \
						   [9, "Payal", 28.5714937, 77.0722989, 60], \
						   [10, "Baabul", 28.5651052, 77.0466785, 220]   # List of all the orders stored in list of list

	def order(self):
		"""
		Function to create and assign an order based on the order list.
		:return: None
		"""
		for ordr in self.orders_list:
			logger1.info(f"{ordr[0]}\\\\\\{ordr[1]}\\\\\\{ordr[2]},{ordr[3]}\\\\\\-\\\\\\-\\\\\\order_received\\\\\\-")
			loc = (ordr[2], ordr[3])
			dist, branch = self.get_nearest_branch(loc)   # getting the nearest branch
			d_time = dist
			threads = []
			while threading.active_count() > 3:  # checking. there should  not be more than 3 active orders at a time. if all the delivery boys are occupied it will wait for him to be free
				# print("All delivery boys are occupied. Waiting...")
				time.sleep(1)
			db_dist, d_boy = self.get_delivery_boy(self.branches[branch], ordr) # getting nearest delivery boy to the selected branch
			tdist = dist+db_dist # total distance to be covered by the delivery boy
			self.delivery_boys[d_boy]["status"] = 1
			threads.append(threading.Thread(target=self.deliver, args=(tdist, math.ceil(db_dist), math.ceil(d_time), branch, d_boy, ordr)))
			"""
			using the math.ceil function to convert distance in decimal to get time in minutes.
			According to this if the distance  is 1.6 km then, I am considering the time taken will be 2 mins.
			"""
			threads[-1].start()
			time.sleep(1*self.minute)
			"""
			************************************PLEASE NOTE**************************************
			Assuming every order comes after 1 minute, if I consider it as 5 minutes then the result are not coming as expected.
			because the distance between the coordinates is coming almost 1 km for every coordinates and previous order get delivered 
			before the next order comes. which means when the next order comes all the delivery boys are available.
			Maybe I have used a different approach to find the distance between coordinates.

			"""
		for k, v in self.delivery_boys.items():
			logger2.info(f"{v['name']}----{v['TKC']}----{v['TE']}----{v['TOH']}----{v['TOD']}")

	def deliver(self, distance,atime, dtime, branch, dboy,order_details):
		"""
		Function to deliver the  order based on data gathered. This function is started inside a child thread
		:param distance: total distance covered by Delivery boy
		:param atime: time taken by delivery boy arrive at the branch to pick up the order
		:param dtime: time taken by delivery boy deliver the order from the branch
		:param branch: branch index from which order is to be delivered
		:param dboy: which delivery boy is assigned
		:param order_details: contains all the details of the current order like name, location, amount etc
		:return: None
		"""
		time.sleep(atime*self.minute)
		logger1.info(f"{order_details[0]}\\\\\\{order_details[1]}\\\\\\{order_details[2]},{order_details[3]}\\\\\\{self.delivery_boys[dboy]['name']}\\\\\\-\\\\\\order_picked\\\\\\-")
		time.sleep(dtime*self.minute)
		self.delivery_boys[dboy]["location"] = self.branches[branch]
		self.delivery_boys[dboy]["status"] = 0
		logger1.info(f"{order_details[0]}\\\\\\{order_details[1]}\\\\\\{order_details[2]},{order_details[3]}\\\\\\{self.delivery_boys[dboy]['name']}\\\\\\{order_details[4]}\\\\\\order_delivered\\\\\\{atime+dtime}min")
		self.delivery_boys[dboy]["TKC"] += distance
		self.delivery_boys[dboy]["TOD"] += (atime+dtime)
		self.delivery_boys[dboy]["TE"] += (order_details[4])
		self.delivery_boys[dboy]["TOH"] += 1

	def get_delivery_boy(self, b_location, order_details):
		"""
		Function to get the delivery boy present nearest to the branch
		:param b_location: branch location from where Delivery boy should pick up the order
		:param order_details: all the current order details
		:return:
		"""
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
		"""
		Get the distance between coordinates
		:param coords_1: coordinates of location 1
		:param coords_2: coordinates of location 2
		:return: distance in km
		"""
		return geopy.distance.geodesic(coords_1, coords_2).km

	def get_nearest_branch(self, ord_loc):
		"""
		Function to get the nearest branch to the users location from which order is placed.
		:param ord_loc: order location from where the order is placed
		:return: branch index with the minimum distance
		"""
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

def make_dir(*paths):
	"""
	Creates all required directories if not present, mentioned in the path.

	:param paths: path from which directories are to be created
	:return: None
	"""
	for pt in paths:
		if not (os.path.isdir(pt)):
			try:
				os.makedirs(pt, mode=0o777, exist_ok=True)
			except OSError as exception:
				if exception.errno != errno.EEXIST:
					raise

if __name__ == '__main__':
	restaurant = Restaurant()
	LOG_PATH = os.path.join(os.getcwd(), "Logs")
	make_dir(LOG_PATH)
	logger1 = create_rotating_logger("First Log File", os.path.join(LOG_PATH, "LogFile1.log"))
	logger2 = create_rotating_logger("Second Log File", os.path.join(LOG_PATH, "LogFile2.log"))
	restaurant.order()

