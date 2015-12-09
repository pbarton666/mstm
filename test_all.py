import os
import csv
from mappings import *

from create_reporting_rollups import main as  rollups_main
from analyze_main import main as analyze_main

#override mappings.py objects
#these are from mappings.py	

#database for results
DB = 'test_mstm_db'
#from mappings import scenarios, subdir_map
ZONES=3
ROOT_DIR='/home/pat/workspace/mstm_test'
scenarios = [
        {'name': 'nored_nohwy',
         'location': os.path.join(ROOT_DIR, 'MSTM_Output_2030_NoRedLine - NoHwyImprovements')
         },

        {'name': 'yesred_no_hwy',
         'location': os.path.join(ROOT_DIR, 'MSTM_Output_2030_RedLine - NoHwyImprovement')
         },

        {'name': 'nored_yeshwy',
         'location': os.path.join(ROOT_DIR, 'MSTM_Output_2030_ NoRedLine - HwyImprovement')
         },               
    ]

#subdirectory locations (these relative to scenario root, the first bit of the file name is the key)
subdir_map={'MODE': 'ModeChoice_OD_byPurposeIncomeMode',  
                'TRANSIT':'TRANSIT_OD_TimeCost' ,
                'FARE':'rootdir|Fares',   ##Yes, this is ugly, but signals that this is scenario-independent and found off root
                'TOD':'TOD_OD_byPurposeIncomeTODOccupancy',
                'LOADED': 'Loaded_HWY_OD_TimeCost'}	

map_file = 'test_map_file.csv'

#override some mappings settings for these tests

trips0=[  [ 0,   1,    2,   3],  
              [1,   11, 12, 13],      
              [2,   21, 22, 23],       
              [3,   31, 32, 33]   ]
trips1=[  [ 0,   1,    2,   3],  
              [1,   111, 112, 113],      
              [2,   121, 122, 123],       
              [3,   131, 132, 133]   ]
trips2=[  [ 0,   1,    2,   3],  
              [1,   211, 212, 213],      
              [2,   221, 222, 223],       
              [3,   231, 232, 233]   ]
trips3=[  [ 0,   1,    2,   3],  
              [1,   311, 312, 313],      
              [2,   321, 322, 323],       
              [3,   331, 332, 333]   ]	
costs=[ [0,    1,    2,    3], 
            [1, .10, .20, .30],   
            [2, .40, .50, .60],  
            [3, .70, .80, .90]    ]

def write_files():

	file_vals=[ ('TRANSIT_PK_WexpBus_XferWaitTime.csv', 'costs'),
	                    ('MODE_HBW_Inc1_WexpBus.csv', 'trips'),
		                ('MODE_HBW_Inc1_SR2.csv',  'trips'),   
	                    ('MODE_HBW_Inc1_DA.csv', 'trips'),
		       
		                 ('TRANSIT_PK_WexpBus_XferWaitTime.csv', 'costs'),
		                 ('TRANSIT_PK_WexpBus_WalkTime.csv' , 'costs'),
	                     ('LOADED_HWY_DISTANCE_SOV_AM.csv',  'costs'),
	                     ('LOADED_HWY_TIME_SOV_AM.csv',  'costs'),  	
	                     ('LOADED_HWY_TIME_HOV_AM.csv',  'costs'),  	
		                 ('FARE_WexpBus_PK.csv', 'costs')
		                 ]
	#print('TRANSIT_PK_WexpBus_WalkTime.csv'  in file_vals)

		
	for index, s in enumerate(scenarios):
		name=s['name']+"_"
		location=s['location']
		if not os.path.exists(location):
			os.mkdir(location)
		for fn, atype in file_vals:
			subdir_name= subdir_map[fn.split('_')[0]]
			if '|' in subdir_name:
				subdir_name=subdir_name.split('|')[1] #strips the real directory name
				subdir=os.path.join(os.path.sep.join(location.split(os.path.sep)[:-1]), subdir_name)
				a=1
			else:
				subdir = os.path.join(location, subdir_map[fn.split('_')[0]])
			if not os.path.exists(subdir):
				os.mkdir(subdir)
			if atype=='costs':
				arr=costs
			else:
				if index==0: arr=trips0
				if index==1: arr=trips1
				if index==2: arr=trips2
			with open(os.path.join(subdir, fn), 'w') as file:
				#print('writing file {}'.format(os.path.join(subdir, fn)))
				writer=csv.writer(file, delimiter=',')
				for row in arr:
					writer.writerow(row)
				
"""This file is pretty hard to read, but produces rollups to tables Inc1_NTDlta_Benefits and Inc1_NTDlta_Costs, testing various adjustments and algorithms:

MODE_HBW_Inc1_WexpBus.csv	'+	        TRANSIT_PK_WexpBus_XferWaitTime.csv  	'→	Inc1_XferWaitTime
MODE_HBW_Inc1_WexpBus.csv	'+	        TRANSIT_PK_WexpBus_WalkTime.csv 	        '→	Inc1_WalkTime
MODE_HBW_Inc1_WexpBus.csv	'+	        FARE_WexpBus_PK.csv                                          '→	Inc1_Fare                         outbound leg
MODE_HBW_Inc1_WexpBus.csv (^T)	'+	FARE_WexpBus_PK.csv                                          '→	Inc1_Fare                         return leg (using transpose trip mx)
MODE_HBW_Inc1_DA.csv	'+	                        LOADED_HWY_DISTANCE_SOV_AM.csv 	    '→	Inc1_DISTANCE
MODE_HBW_Inc1_DA.csv	'+	                        LOADED_HWY_TIME_SOV_AM.csv 	                 →	Inc1_TIME
MODE_HBW_Inc1_SR2.csv	'+	                    LOADED_HWY_TIME_HOV_AM.csv                   '→	Inc1_TIME
"""
my_map=[\
        ["inc","purpose","mode","cost","tod","hov","leg","transpose","peak","pct_wage","category","pct_hb","hov_adj","trip_file","cost_file","dbtable","aggregate_to","rollup_dbtable","rollup_to"],
        ["Inc1","HBW","WexpBus","XferWaitTime","","","Outbound",0,1,100,"Time",100,"","MODE_HBW_Inc1_WexpBus.csv","TRANSIT_PK_WexpBus_XferWaitTime.csv  (100%|100%)","Inc1_NTDlta_Benefits","Inc1_XferWaitTime","Inc1_NTDlta_Costs","Inc1_XferWaitTime"],
		["Inc1","HBW","WexpBus","WalkTime","","","Outbound",0,1,100,"Time",100,"","MODE_HBW_Inc1_WexpBus.csv","TRANSIT_PK_WexpBus_WalkTime.csv  (100%|100%)","Inc1_NTDlta_Benefits","Inc1_WalkTime","Inc1_NTDlta_Costs","Inc1_WalkTime"],
		["Inc1","HBW","WexpBus","Fare","","","Outbound",0,1,100,"Money",100,"","MODE_HBW_Inc1_WexpBus.csv","FARE_WexpBus_PK.csv  (100%|100%)","Inc1_NTDlta_Benefits","Inc1_Fare","Inc1_NTDlta_Costs","Inc1_Fare"],
		["Inc1","HBW","WexpBus","Fare","","","Return",1,1,100,"Money",0,"","MODE_HBW_Inc1_WexpBus.csv (^T)","FARE_WexpBus_PK.csv  (100%|0%)","Inc1_NTDlta_Benefits","Inc1_Fare","Inc1_NTDlta_Costs","Inc1_Fare"],
		["Inc1","HBW","DA","DISTANCE","AM","SOV","Outbound",0,"","-","Hwy_distance",100,1,"MODE_HBW_Inc1_DA.csv","LOADED_HWY_DISTANCE_SOV_AM.csv (-%|100%)","Inc1_NTDlta_Benefits","Inc1_DISTANCE","Inc1_NTDlta_Costs","Inc1_DISTANCE"],
		["Inc1","HBW","DA","TIME","AM","SOV","Outbound",0,"","-","Hwy_time",100,1,"MODE_HBW_Inc1_DA.csv","LOADED_HWY_TIME_SOV_AM.csv (-%|100%)","Inc1_NTDlta_Benefits","Inc1_TIME","Inc1_NTDlta_Costs","Inc1_TIME"],
		["Inc1","HBW","SR2","TIME","AM","HOV","Outbound",0,"","-","Hwy_time",100,2,"MODE_HBW_Inc1_SR2.csv","LOADED_HWY_TIME_HOV_AM.csv (-%|100%)","Inc1_NTDlta_Benefits","Inc1_TIME","Inc1_NTDlta_Costs","Inc1_TIME"]
        ]

"""The XferWait time costs are just SUM(trips*cost/trip)
yesred_nohwy						
trips1=[  [ 0,   1,    2,   3],  		costs=[ [0,    1,    2,    3], 		trips0=[  [ 0,   1,    2,   3],  		costs=[ [0,    1,    2,    3], 
              [1,   111, 112, 113],      		            [1, .10, .20, .30],   		              [1,   11, 12, 13],      		            [1, .10, .20, .30],   
              [2,   121, 122, 123],       	x	            [2, .40, .50, .60],  	-	              [2,   21, 22, 23],       	x	            [2, .40, .50, .60],  
              [3,   11, 132, 133]   ]		            [3, .70, .80, .90]    ]		              [3,   31, 32, 33]   ]		            [3, .70, .80, .90]    ]
						
=450						
						
nored_yeshwy						
trips2=[  [ 0,   1,    2,   3],  		costs=[ [0,    1,    2,    3], 		trips0=[  [ 0,   1,    2,   3],  		costs=[ [0,    1,    2,    3], 
              [1,   211, 212, 213],      		            [1, .10, .20, .30],   		              [1,   11, 12, 13],      		            [1, .10, .20, .30],   
              [2,   221, 222, 223],       		            [2, .40, .50, .60],  		              [2,   21, 22, 23],       	x	            [2, .40, .50, .60],  
              [3,   231, 232, 233]   ]		            [3, .70, .80, .90]    ]		              [3,   31, 32, 33]   ]		            [3, .70, .80, .90]    ]
						
=900						

So, for yesred_nohwy we'd expect  xferWaitTime = walkTime = 450   (single-leg journeys, as specified in the map file).  Fare =900 (applied to two-leg journey).  Distance = 450
(single-leg journey).   Time=1350 (two commute combined, one with double occupancy - won't affect distance, but will double time)

TODO:  write tests for these values, time permitting.  Presently using "eyeball" test method on output from sum_costs_from_db.

"""
def main():
	write_files()
	#create the map file
	with open(map_file, 'w') as file:
		#writer=csv.writer(file, delimiter=',', quoting=csv.QUOTE_ALL)
		writer=csv.writer(file, delimiter=',')
		for line in my_map:
			writer.writerow(line)
	a=1
	
	rollups_main(scenarios=scenarios, DB=DB, ROOT_DIR=ROOT_DIR, ZONES=ZONES, map_file=map_file)

if __name__=='__main__':
	main()
	import sum_costs_from_db
	sum_costs_from_db.sum_costs(test=True)