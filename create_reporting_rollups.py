#create_reporting_rollups.py
#
#Builds reporting tables based on total costs (costs * cost/trip)
#
#Uses most of the routines from analyze_main.py (truth be known, this could logically be combined
#  with analyze_main.py, but this keeps a clean separation for transparency).
#
#The input data files and other run-time settings are in mappings.py (same as analyze_main.py).  The
#   only difference is that this uses data in the 'rollup_dbtable' column to identify the database table and
#  'rollup_to'column of the input to specify the database column.

from analyze_main import store_data, create_one_col_np_array, accrete_col_to_np_array
from analyze_main import sum_col_to_np_array,calculate_benefits, make_OD_array
from analyze_main import npa_from_file, prep_data, get_total_costs, row_sum
from analyze_main import column_sum, get_full_filename, get_cs_delta, grabinfo

import csv
import os
import sys
from datetime import datetime

import numpy as np
import psycopg2

from  database import *  #login credentials
from mappings import *  #constants for file locations, run-time settings, database name, etc.
import logger_setup
from logger_setup import logger

logger.setLevel(LOG_LEVEL)
start=datetime.now()

def get_total_cost_delta(base_trips=None, test_trips=None, base_costs=None, test_costs=None):
	return  test_trips*test_costs - base_trips*base_costs

def main():
	"main entry point - loops over scenarios"

	msg='{} Starting total cost calculations using input file {}'
	logger.info(msg.format(datetime.now().strftime("%b %d %Y %H:%M:%S"), map_file))
	print(msg.format(datetime.now().strftime("%b %d %Y %H:%M:%S"), map_file))

	#This isn't the most efficient way to do it, but it's the most transparent:  we'll loop through each base:scenario pair.  For each, we'll read
	#  the input file a line at a time and draw our consumer surplus benefits

	arr_dict={}
	base_scenario = scenarios[0]

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
			arr_name=dmap['rollup_to']                                                        ## updated for total costs
			column_name= arr_name
			table_name=s['name']+"_"+dmap['rollup_dbtable']		   ## updated for total costs
			
			#unless this line has both a data table and column specified, go to the next line
			if not arr_name or not column_name:
				continue

			#get information for the base case
			base_dir=base_scenario['location']		#root directory location
			base_name=base_scenario['name']		#name for this scenari

			#Build fully specified path names built from locations in mappyings.py; subdirectory determined by file name
			#   then create np arrays out of them
			base_cost_file=get_full_filename(location=base_dir, filename=dmap['cost_file'])
			base_trips_file=get_full_filename(location=base_dir,  filename=dmap['trip_file'])

			#try to create npa arrays from the raw data files; if they don't exist go on to the next line
			try:
				base_trips_raw = npa_from_file( base_trips_file)
				base_cost_per_trip_raw = npa_from_file( base_cost_file)	
			except:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				msg='Scenario {}: could not open requisite files \n {} {} specified in line {} of {}'
				logger.warn(msg.format(s['name'], exc_type, exc_value, line_ix, map_file))
				continue

			#Costs and trips for the base case - returns  base costs, base trips as  square np 
			#   arrays w/o OD headers, trips transposed if needed
			base_costs, base_trips=prep_data( base_cost_per_trip_raw , base_trips_raw,  transpose,  hov_adj )

			#Process the scenario costs and trips the same way
			test_dir = s['location']
			#grab the files and put them in np arrays
			test_cost_file=get_full_filename(location=test_dir, filename=dmap['cost_file'])
			test_trip_file=get_full_filename(location=test_dir,  filename=dmap['trip_file'])
			test_trips_raw = npa_from_file( test_trip_file)
			test_cost_per_trip_raw = npa_from_file( test_cost_file)					
			test_name=s['name']
			#Scenario case trips*cost/trip and trips used 
			test_costs, test_trips=prep_data( cost_per_trip=test_cost_per_trip_raw , trips=test_trips_raw,  transpose=transpose,   hov_adj=hov_adj  )			

			#With all costs gathered, calculate the change in consumer surplus in square np array; produces a square np array
			##cs_delta = get_cs_delta(base_trips, test_trips, base_costs, test_costs)
			## updated for total costs
			total_cost_delta=get_total_cost_delta(base_trips, test_trips, base_costs, test_costs)

			#From the cs_delta matrix, assign benefits to the origin and destination node; produces a vector of nodes w/o OD headers
			#  For home-based transit trips, both outbound and return accrue to home node, as do am and pm highway trips.
			#  For mid-day and night-time highway trips, the benefit is split between origin and dest nodes.
			## updated for total costs
			#benefits_by_zone = calculate_benefits(cs_delta, pct_hb)
			costs_by_zone = calculate_benefits(total_cost_delta, pct_hb)

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
			                                                 vector= costs_by_zone, 
			                                                 max_index_val=len(base_trips)),
			                                           'column': column_name,
			                                           'table': table_name
			                                           }
			logger.debug('{} -versus- {} line {}\n\t {}  \n\t {} \n\t base trips: {}  test trips: {}  sum dlta costs: {}  (summary stats - not used in benefit calcs)'.format(
			    line_ix,
			    base_name, test_name, 
			    dmap['trip_file'],
			    dmap['cost_file'].split()[0],
			    np.sum(base_trips), np.sum(test_trips),
			    np.sum(total_cost_delta)))

		#store the arrays in db tables	
		store_data(arr_dict=arr_dict, db=DB)

	finish = datetime.now()
	msg='Finished at {}.  Processed {} files in {}.'
	elapsed=str(finish-start).split('.')[0]
	print(msg.format(datetime.now().strftime("%b %d %Y %H:%M:%S"), line_ix, elapsed))
	logger.info(msg.format(datetime.now().strftime("%b %d %Y %H:%M:%S"), line_ix, elapsed))
	
if __name__=='__main__':
	main()