import os
import numpy as np

import analyze_main

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
            'FARE':'rootdir|Fares',   ##Yes, this is ugly, but signals that this is scenario-independent and found off root
            'TOD':'TOD_OD_byPurposeIncomeTODOccupancy',
            'LOADED': 'Loaded_HWY_OD_TimeCost'}

def npa_from_file(file_name):
	"""Creates a np array from a csv file, first checking if a pickled version is available"""
	try:
		return np.load(file_name + ".pkl")
	except:
		return  np.genfromtxt( file_name, delimiter=',', dtype='float')

scen=scenarios[0]
loc=scen['location']
loaded_dir=os.path.join(loc, subdir_map['LOADED'])
mode_dir=os.path.join(loc, subdir_map['MODE'])
hwy_dir=os.path.join(loc, subdir_map['TOD'])

work_hwy_trips_files=['MODE_HBW_Inc3_DA.csv']
tod_trip_files=['TOD_HBW_Inc3_AM_SOV.csv',
                              'TOD_HBW_Inc3_PM_SOV.csv',
                              'TOD_HBW_Inc3_MD_SOV.csv',
                              'TOD_HBW_Inc3_NT_SOV.csv']

tod_trips=[]
for f in tod_trip_files:
	tod_trips.append(os.path.join(hwy_dir, f))
	
print('If we sum up the trips represented by these files  ...')
for f in tod_trips:
	print(f)
print()	
print('    ...they should roughly equal the trips in')
print(os.path.join(mode_dir, work_hwy_trips_files[0]))

#here we go
mode_trips=npa_from_file(os.path.join(mode_dir, work_hwy_trips_files[0]))[1:,1:]
hwy_trips= npa_from_file(os.path.join(hwy_dir, tod_trip_files[0]))[1:,1:] +\
                                npa_from_file(os.path.join(hwy_dir, tod_trip_files[1]))[1:,1:] +\
                                npa_from_file(os.path.join(hwy_dir, tod_trip_files[2]))[1:,1:] +\
                                 npa_from_file(os.path.join(hwy_dir, tod_trip_files[3]))[1:,1:] 

print('\nRight?   However...\n')
print('Total trips from the mode choice model: {}'.format(mode_trips.sum()))
print('Total trips from the OTD model: {}\n'.format(hwy_trips.sum()))

print('... not even in the same time zone.  But I do not want to belabor since these data are getting updated.')
