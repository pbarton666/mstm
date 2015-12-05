"""This latest attempt reads in mapping data from an input file and does the following for each scenario.  
      
      Each scenario and income group is in its own table.  
	  
	  Data columns will be created for each cost category (e.g., WalkTIme), purpose (e.g., HBW) by OD pair.  
	  
	  Other data columns aggregate these costs  by type e.g. (Transit_Time, Highway_Time, Highway_Auto-miles).    
	  
	  Except for tolls, these are not yet monotized, but do embody the allocation to 	  the 'home zones' 
	  of the travelers and reflect a  'discount' for time spent in various activities to reflect the fact that 
	  walk/wait time is valued at the fully burdened wage of the traveler and rid time is value at half that.   
	  Highway times are adjusted for occupancy.    
	  
	  As result, we should be able to provide the average wage of travelers to the aggregated time 
	  without further adjustments when we monotize costs/benefits.
"""

##TODO: for non-home-based trips, do we just attribute trips to the origin?
##TODO: for mid-day and night-time highway trips we attribute 50% of the trips to the home base and 50% to the destination.  Right?

import csv
import os
import sys
import numpy as np
import psycopg2

import logger_setup
from logger_setup import logger
from  database import *
from mappings import *  #constants for file locations, etc.

ZONES=1179
DB = 'naacp_benefits'
CREATE_NEW_TABLES=True #create fresh tables if True, build on old if False

import logging
import logging.handlers

#set to 'WARN' to capure only data loading issues.  'DEBUG' is verbose.
LOG_LEVEL='DEBUG'   

#**********************************************************

logger.setLevel(LOG_LEVEL)

def grabinfo(line):
	"gleans information from a line of the mapping file, adjusting where needed (screens out annotations, etc.)"
	ret_dict=line
	#the trip file may have a 'transpose' flag; the first bit is the file name
	ret_dict['trip_file']=line['trips'].split()[0]
	#the cost file may have annotations for time discounts, etc.; the first bit is the file name
	ret_dict['cost_file']=line['cost'].split()[0]
	return ret_dict

def make_OD_array(rows=None, cols=None, start_row=1, max_row=99999):
	""" returns two-column np array with origin, destination pais"""
	arr=[]
	rwa=[]
	ca=[]
	#we'll assume a square array unless we're doing a partition
	if not cols:
		cols = rows

	for r in range(start_row, min(start_row+rows, max_row+1)):
		rwa.extend([r]*cols)
		for c in range(1, cols+1):
			ca.extend([c])

	npa = np.zeros((rows*cols, 2))		
	npa[:,0]=rwa
	npa[:,1]=ca
	return npa



def npa_from_file(file_name):
	"creates a np array from a csv file; strips headers from first row and first column"
	return  np.genfromtxt( file_name, delimiter=',', dtype='float')

def get_costs(cost_per_trip=None,  trips=None, transpose=None,   pct_hb=None, hov_adj=None):
	"""Calculates the costs for some transport segment; returns an aggregate np array w/ OD columns - creating if needed
	      Returns a tuple of np arrays:  (trips*cost/trip array, trips)     both  stripped of OD headers  """
	
	#adjust the cost per trip to account for hov occupancy (probably best to adjust for wage pct elsewhere)
	cost_per_trip=cost_per_trip[1:,1:]
	trips=trips[1:,1:]
	
	if hov_adj:
		cost_per_trip = cost_per_trip * int(hov_adj)
	
	#transpose trip array if required and  multiply by cost_per_trip
	if transpose:
		trips_used= trips.T
	else:
		trips_used=trips
		
	return  (trips_used * cost_per_trip, trips_used)

def row_sum(npa=None):
	"returns vector of row sums of square matrix"
	return np.sum(npa, 1)

def column_sum(npa=None):
	"returns vector of column sums of square matrix"
	return np.sum(npa, 0)

def get_full_filename(location=None, filename=None):
	"location identifies the main scenario directory; the first bit of the file name determines its subdirectory"
	first_bit=filename.split('_')[0]
	subdir=subdir_map[first_bit]
	##NB:  this is one-off for the MSTM directory layout
	if  'rootdir' in subdir:
		return os.path.join(ROOT_DIR, subdir.split('|')[1], filename)
	else:
		return(os.path.join(location, subdir, filename))

def get_cs_delta(base_trips=None, test_trips=None, base_costs=None, test_costs=None):
	return .5 * (base_trips + test_trips)*(base_costs - test_costs)

def main():
	"main entry point - loops over scenarios"
	
	#This isn't the most efficient way to do it, but it's the most transparent:  we'll loop through each base:scenario pair.  For each, we'll read
	#  the input file a line at a time and draw our consumer surplus benefits
	
	arr_dict={}
	
	for s in scenarios[1:]:
		#grab a reader for the input file
		reader=csv.DictReader(open(map_file))
		
		#process one line at a time from the setup file
		for line_ix, line in enumerate(reader, start=1):
			
			# the info comes in as a dict - this cleans up the content, removing comments, etc.  Return a dict.
			dmap = grabinfo(line)
			
			#these set processing parameters
			transpose=dmap['transpose']				#use transposed trip matrix?
			hov_adj=dmap['hov_adj']						#occupancy adjustment (hov2 implies double time costs, say)
			pct_hb=dmap['pct_hb'] 							#fraction of benefits occuring to origin node ('home base')		
			
			#these set storage parameters
			arr_name=dmap['aggregate_to']
			column_name= arr_name
			table_name=s['name']+"_"+dmap['dbtable']			
			
			#get information for the base case
			base_dir=scenarios[0]['location']		#root directory location
			base_name=scenarios[0]['name']		#name for this scenari
			
			#Build fully specified path names built from locations in mappyings.py; subdirectory determined by file name
			#   then create np arrays out of them
			base_cost_file=get_full_filename(location=base_dir, filename=dmap['cost_file'])
			base_trips_file=get_full_filename(location=base_dir,  filename=dmap['trip_file'])
			try:
				base_trips = npa_from_file( base_trips_file)
				base_cost_per_trip = npa_from_file( base_cost_file)	
			except:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				print('Scenario {}: could not open requisite files \n {} {}'.format(s['name'], exc_type, exc_value))
				continue
			
			#Costs (trips*cost_per_trip) for the base case - returns  base costs, base trips as  square np 
			#   arrays w/o OD headers, trips transposed if needed
			base_costs, base_trips=get_costs( base_cost_per_trip , base_trips,  transpose,  hov_adj )
			
			#Process the scenario costs and trips the same way
			test_dir = s['location']
			#grab the files and put them in np arrays
			test_cost_file=get_full_filename(location=test_dir, filename=dmap['cost_file'])
			test_trip_file=get_full_filename(location=test_dir,  filename=dmap['trip_file'])
			test_trips = npa_from_file( test_trip_file)
			test_cost_per_trip = npa_from_file( test_cost_file)					
			test_name=s['name']
			#Scenario case trips*cost/trip and trips used 
			test_costs, test_trips=get_costs( cost_per_trip=test_cost_per_trip , trips=test_trips,  transpose=transpose,   hov_adj=hov_adj  )			
			
			#With all costs gathered, calculate the change in consumer surplus in square np array; produces a square np array
			cs_delta = get_cs_delta(base_trips, test_trips, base_costs, test_costs)
			
			#From the cs_delta matrix, assign benefits to the origin and destination node; produces a vector of nodes w/o OD headers
			#  For home-based transit trips, both outbound and return accrue to home node, as do am and pm highway trips.
			#  For mid-day and night-time highway trips, the benefit is split between origin and dest nodes.
			benefits_by_zone = calculate_benefits(cs_delta, pct_hb)
			

	
			#the balance of this block stores the benefits and other information for posterity/more analysis
			
			#We'll aggregate the cs_delta by benefit type, denominated in natural units (minutes, dollars, miles).  We can use scalars to transform
					#minutes to dollars, miles to CO2, etc. as a post-processing step.
					
			#We'll create an aggregation array if requested in the 'aggregate to' column.  	This bit of code adds the array to the arr_dict if needed,
			#   then creates a null np array if needed.   It adds a column of current benefits_by_zone to the array if the array is null; it increments 
			#  the array by the benefits just calculated otherwise.
			
			#to hold the results, we'll make a dict   arr_dict{'aggregate_to':  {'data':npa_of_data,
			#                                                                                                                              'column': 'aggregate_to',  
			#                                                                                                                              'table': 'db_table}}
			#... where the 'aggregate_to' value comes from the input spreadsheet and serves as the name of the database column.
			
			#create the dict if needed (it doesnt yet exist)
			if not arr_name in arr_dict:
				arr_dict[arr_name]={}
				arr_dict[arr_name]['data']=None
				
			#update the dict with current state of the roll-up array
			arr_dict[arr_name]={ 'data': sum_col_to_np_array(npa=arr_dict[arr_name]['data'], 
			                                                                                     vector=benefits_by_zone, 
			                                                                                     max_index_val=len(base_trips)),
			                                          'column': column_name,
			                                           'table': table_name
			                                           }
			logger.debug('{} -versus- {} \n\t {}  \n\t {} \n\t base trips: {}  test trips: {}  sum dlta cs: {}  (summary stats - not used in benefit calcs)'.format(
				         base_name, test_name, 
			             dmap['trips'],
			             dmap['cost'].split()[0],
			             np.sum(base_trips), np.sum(test_trips),
			            np.sum(cs_delta)))
		
		#store the arrays in db tables	(TODO: move out one level)
		store_data(arr_dict=arr_dict, db=DB)

def store_data(arr_dict=None, db = DB, create_new_tables=CREATE_NEW_TABLES, zones=ZONES):
	"""Stores data from all the benefits calculations made to date into a relational database.
	      The arr_dict contains all the arrays to be stored, the db table, and the db column.  This
	      will create new tables if requested and take care of creating the necessary columns called
	      for by the arr_dict information"""
	
	#Create a new db if needed.  A bit convoluted in psql because you need to establish a new connection
	#   to address a different db.  (In MySQL, it's as simple as going:  USE database;)

	if True:  
		conn = psycopg2.connect(
		    user=login_info['user'],
		    password=login_info['password']
		)
		curs = conn.cursor()    

		#if the database is not available, try to create it
		try:
			curs.execute('END')
			curs.execute('CREATE DATABASE {}'.format(db))
			logger.debug('creating new database')
		except psycopg2.ProgrammingError:      
			pass  #datbase already exists	

		#make a new connection to the right database
		conn.close()
		conn = psycopg2.connect(database = db,
		                        user=login_info['user'],
		                        password=login_info['password']
		                        )
		curs = conn.cursor()   		

		#kill existing tables if instructed
		if create_new_tables:
			for arr in arr_dict:
				#print('trying {}'.format(arr_dict[arr]['table']))
				curs.execute("END")
				curs.execute('DROP TABLE IF EXISTS {}'.format(arr_dict[arr]['table']))		
				conn.commit()
				#print('done with {}'.format(arr_dict[arr]['table']))

	#loop through our arrays dict, augmenting the database where needed
	for arr in arr_dict:
		
		#for readability, make pointers to elements of this dict entry
		arr_table=arr_dict[arr]['table']
		arr_data=arr_dict[arr]['data']
		arr_column=arr_dict[arr]['column']

		#create a table if we need to; populate it with the zone data
		try:
			curs.execute('END')
			curs.execute('SELECT * from {} LIMIT 1'.format(arr_table))
			conn.commit()
		except: #table does not exist
			curs.execute('CREATE TABLE {} ({} float  PRIMARY KEY )'.format(arr_table, 'zone', 'zone'))
			for row in range(1, zones + 1):
				curs.execute("INSERT INTO {} ({}) VALUES ({})".format(arr_table, 'zone', row))
			conn.commit()
			curs.execute("END")

		#create a data column if we need to; popluate it with array data
		try:
			curs.execute("INSERT INTO {} ({}) VALUES ({}) WHERE {}=1".format(arr_table, 'zone', '-666', 'zone'))
		except: #col does not exist
			curs.execute('END')
			curs.execute("ALTER TABLE {} ADD {} float  DEFAULT 0.0".format(arr_table, arr_column))
			for row in arr_data:
				curs.execute("UPDATE {} SET {}={} WHERE {}={}".format(arr_table, arr_column, row[1], 'zone', row[0]))
			conn.commit()			
		
	
def create_empty_np_array(max_index_val=ZONES):
	"creates a one-column np array with (1x np_rows) and index values (1, 2, 3 ...)"
	index_values=[i for i in range(1, max_index_val+1)]
	npa = np.zeros((max_index_val, 1))
	npa[:,0]=index_values	
	return npa

def accrete_col_to_np_array(npa=None, vector=None, max_index_val=ZONES):
	"Adds a vector to a np array,  If np array does  not exist, create an array (1x np_rows) with index values (1, 2, 3 ...)"
	if  npa is None:
		npa=create_empty_np_array(max_index_val=max_index_val)
	npa=np.c_[npa, vector]
	return npa

	
def sum_col_to_np_array(npa=None, vector=None, max_index_val=ZONES):
	"Adds a vector to to values in last colum of a np array,  If np array does  not exist, create it w/ index and vector value"
	if  npa is None:
		npa=create_empty_np_array(max_index_val=max_index_val)
		npa=accrete_col_to_np_array(npa=npa, vector=vector)
		return npa
	npa[:,-1]+=vector
	return npa
	
def calculate_benefits(cs_delta=None, pct_hb=None):
	"rolls up benefits accruing to specific transportation nodes, given a matrix of consumer surplus deltas"
	#Assign this to some origin or destination.    Origin benefit = SUM(columns); Dest benefit = SUM(rows)
	benefits_to_origin=int(pct_hb)/100
	origin_cs_delta = row_sum(cs_delta * benefits_to_origin)
	dest_cs_delta=column_sum(cs_delta * (1-benefits_to_origin))

	#Regardless if whether it's an 'origin' or a 'destination' the benefits from each trip accrue to some Zone
	#   We can simply add the 'origin' and 'destination' vectors to combine
	cs_delta_by_zone = origin_cs_delta + dest_cs_delta	
	return cs_delta_by_zone


if __name__=='__main__':
	main()
	
