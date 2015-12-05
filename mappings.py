
"""mappings.py

Contains file name encodings and mapping to model concepts

"""

import os
#incomes=['inc1', 'inc2', 'inc3', 'inc4', 'inc5']
#rail_modes=['wrail', 'wcrail', 'drail', 'dcrail']
#bus_modes=['wexpbus', 'wbus', 'dexpbus', 'dbus']
#drive_modes=['da', 'sr2', 'sr3']
#purposes_pk=['hbw', 'nhbw']
#purposes_op=['obo', 'hbs', 'hbo']
#purposes = purposes_pk + purposes_op

##Some trip purposes inherently involve a round trip, others one way.
##   Home-based trips are assumed round-trip
#purposes_round_trip=['hbw', 'hbo', 'hbs']
#purposes_one_way=['obo', 'nhbw']

##transit metrics. These exclude fares (picked up from fares_fares table)    
#transit_time_metrics=['xferwaittime', 'walktime', 'railtime', 'initialwaittime', 'bustime', 'autotime']
#transit_other_metrics=['autodistance']
#transit_metrics=transit_time_metrics + transit_other_metrics

#occupancy_hwy_loaded=['hov', 'sov']
#occupancy_hwy_tod=['sov', 'hov2', 'hov3']

#tod_hwy_loaded = ['am', 'pm', 'md', 'nt']
#tod_transit_times=['pk', 'op']
#tod_fares=tod_transit_times
#metrics=['toll', 'time', 'distance']    

##mappings to purpose
#purpose_peak_flag={}  #mapped to abbreviations in tables
#for p in purposes_pk:
	#purpose_peak_flag[p] = 'pk'
#for p in purposes_op:
	#purpose_peak_flag[p] = 'op'

#	scenario mappings
ROOT_DIR= '/home/pat/Dropbox/Maryland_TDM_RedLine_ProjectShare'
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
            'FARE':'rootdir|Fares',   ##Yes, this is ugly, but signals that this is scenario-independe and found off root
            'TOD':'TOD_OD_byPurposeIncomeTODOccupancy',
            'LOADED': 'Loaded_HWY_OD_TimeCost'}

#map file (maps costs and trips)
map_file = 'map_file.csv'

matrix_size=1179



#hwy_costs = 'Loaded_HWY_OD_TimeCost'
#transit_costs='TRANSIT_OD_TimeCost'
#hwy_trips='ModeChoice_OD_byPurposeIncomeMode'
#transit_trips='TOD_OD_byPurposeIncomeTODOccupancy'