from bson import ObjectId

from BuildDB.build_db import *
import math
import sys
from datetime import datetime, timedelta
from shutil import copy2

from matplotlib import pyplot as plt

from BuildDB.build_db import *

if sys.platform == 'win32':
	directory =  'C:\\Users\\gfried\\Desktop\\Research\\PoliticalScience\\code\\data\\partial'
elif sys.platform == 'linux2':
	directory =  '/home/ubuntu/code/data'

src_dir =  'C:\\Users\\gfried\\Desktop\\Research\\PoliticalScience\\code\\data\\unzipped'
dst_dir  = 'C:\\Users\\gfried\\Desktop\\Research\\PoliticalScience\\code\\data\\ussd'


def collect_ussd_sms_files(src_dir,dst_dir):
	## create a directory with only sms and ussd files
	for root, dirs, files in os.walk(src_dir):
	    for file in files:
	    	file_path = ''.join((root,"\\",file))
	    	with open(file_path, 'rb') as f:
	        	first_line = json.loads(f.readline())
	        	if first_line['transport_type'] == 'ussd' or first_line['transport_type'] == 'sms':
	        		src_file = ''.join((src_dir,'\\',file))
	        		copy2(src_file, dst_dir)
	     

print "creating inbound and outbound data structures...\n"
inb,outb = list_inbound_outbound(directory)
print "creating database connection and collection...\n"
conn, db, conversations_collection = mongo_database_setup()
print "storing data into a database...\n"
df = import_data(inb,outb)
print "storing ids...\n"
object_ids = mongod_insert_df(df, conversations_collection)




channel1 = ['*120*7692*2#']
channel2 = ['*120*7692*3#']
channel3 = ['*120*4729#']

graph1 = ['2014-04-08', '2014-04-19', '2014-04-24', '2014-04-28']
graph2 = ['2014-04-09', '2014-04-20', '2014-04-25', '2014-04-29']
graph3 = ['2014-04-10', '2014-04-21', '2014-04-26', '2014-04-30']


channels = [channel1,channel2,channel3]
graphs = [graph1,graph2,graph3]

pcm_0 = {'2014-04-08':{}, '2014-04-19':{}, '2014-04-24':{}, '2014-04-28':{}}
pcm_1 = {'2014-04-09':{}, '2014-04-20':{}, '2014-04-25':{}, '2014-04-29':{}}
pcm_2 = {'2014-04-10':{}, '2014-04-21':{}, '2014-04-26':{}, '2014-04-30':{}}

pcms = [pcm_0,pcm_1,pcm_2]

min_date = '2014-04-08'
max_date = '0000-00-00 00:00:00.000000'


## determine pcm bursts 
for id_ in object_ids:


	doc = conversations_collection.find_one({"_id": ObjectId(id_)})
	
	user = doc['user']
	conversation = doc['conversation']
	num = len(conversation) 
	msg = ''.join(('msg',str(num)))
	t = conversation[msg]['timestamp'] 
	if t > max_date: max_date = t


	## construct values for figure 1 on response frequency:

	if len(conversation)>=2 and conversation['msg1']['transport_type'] == 'ussd':

		channel = conversation['msg1']['from_addr']

		if channel in channels:

			num = channels.index(channel)
			graph = graphs[num]

			timestamp = conversation['msg2']['timestamp']

			dname = ''.join(('pcm_',num))
			if graph[0] <= timestamp < graph[1]:

				if timestamp in dname[graph[0]]:
					dname[graph[0]][timestamp]  = dname[graph[0]][timestamp] + 1

				else:
					dname[graph[0]][timestamp]  = 1
				
			elif graph[1] <= timestamp < graph[2]:
				if timestamp in dname[graph[1]]:
					dname[graph[1]][timestamp]  = dname[graph[1]][timestamp] + 1

				else:
					dname[graph[1]][timestamp]  = 1

			elif graph[2] <= timestamp < graph[3]:
				if timestamp in dname[graph[2]]:
					dname[graph[2]][timestamp]  = dname[graph[2]][timestamp] + 1

				else:
					dname[graph[2]][timestamp]  = 1

			elif graph[3] <= timestamp:
				if timestamp in dname[graph[3]]:
					dname[graph[3]][timestamp]  = dname[graph[3]][timestamp] + 1

				else:
					dname[graph[3]][timestamp]  = 1



## FIGURE 1:


"""
Control
Join VIP:Voice to help make elections 2014 free and fair. Dial
*120*7692*2# Standard rates charged
Treatment 1 { Lottery
Join VIP:Voice 2 help make elections 2014 free & fair. Dial
*120*7692*3# & stand a chance 2 win R55 airtime
Treatment 2 { Subsidy
Join VIP:Voice to help make elections 2014 free and fair. Dial
*120*4729# 2 participate for free

"""

start_date = datetime.strptime(min_date,"%Y-%m-%d").date()
end_date = datetime.strptime(max_date.split(' ')[0],"%Y-%m-%d").date()
range_dates = []

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

for single_date in daterange(start_date, end_date):
    range_dates.append(single_date.strftime("%Y-%m-%d"))


#graph1 (channel1)

fig, ax1 = plt.subplots()
x_axis = range_dates 

#build up y axis:
l = len(range_dates)
y_axis = [0]*l
y_axis1 = y_axis
y_axis1[0] = math.log(13333333)
y_axis1[11] = math.log(1000000)
y_axis1[16] = math.log(1000000)
y_axis1[20] = math.log(1000000)

ax1.plot_date(x_axis, y_axis1,'b-')

ax1.set_xlabel('date')
ax1.set_ylabel('pcm release over channel1 (log)', color='b')
ax1.tick_params('y',colors='b')

ax2 = ax1.twinx()

y_axis2 = y_axis

for pcm in pcm_0.values():
	for d,n in pcm.iteritems():
		day = d[-2:]
		y_axis2[day-8-1] = n


ax2.plot_date(x_axis,y_axis2,'r-')
ax2.set_ylabel('first response',color='r')
ax2.tick_params('y',colors='r')

fig.tight_layout()
plt.show()


#graph2 (channel2):

fig, ax1 = plt.subplots()
x_axis = range_dates 

#build up y axis:
l = len(range_dates)
y_axis = [0]*l
y_axis1 = y_axis
y_axis1[1] = math.log(13333333)
y_axis1[12] = math.log(1000000)
y_axis1[17] = math.log(1000000)
y_axis1[21] = math.log(1000000)

ax1.plot_date(x_axis, y_axis1,'b-')

ax1.set_xlabel('date')
ax1.set_ylabel('pcm release date over channel 2(log)', color='b')
ax1.tick_params('y',colors='b')

ax2 = ax1.twinx()

y_axis2 = y_axis

for pcm in pcm_0.values():
	for d,n in pcm.iteritems():
		day = d[-2:]
		y_axis2[day-8-1] = n


ax2.plot_date(x_axis,y_axis2,'r-')
ax2.set_ylabel('first response',color='r')
ax2.tick_params('y',colors='r')

fig.tight_layout()
plt.show()

#graph3 (channel3):
fig, ax1 = plt.subplots()
x_axis = range_dates 

#build up y axis:
l = len(range_dates)
y_axis = [0]*l
y_axis1 = y_axis
y_axis1[2] = math.log(13333333)
y_axis1[13] = math.log(1000000)
y_axis1[18] = math.log(1000000)
y_axis1[22] = math.log(1000000)

ax1.plot_date(x_axis, y_axis1,'b-')

ax1.set_xlabel('date')
ax1.set_ylabel('pcm release date over channel 3 (log)', color='b')
ax1.tick_params('y',colors='b')

ax2 = ax1.twinx()

y_axis2 = y_axis

for pcm in pcm_0.values():
	for d,n in pcm.iteritems():
		day = d[-2:]
		y_axis2[day-8-1] = n


ax2.plot_date(x_axis,y_axis2,'r-')
ax2.set_ylabel('first response',color='r')
ax2.tick_params('y',colors='r')

fig.tight_layout()
plt.show()

