#mappings.py
#
#Contains file name encodings, mapping to model concepts, and run-time parameters
#
#
#main setup file - one row per trip, cost/trip matrix combination
import os
import logging

map_file = 'map_file.csv'

#database for results
DB = 'naacp_benefits'

#Set following to True to delete existing data tables.  If set to False, any new columns 
#   created will overwrite existing ones of the same name.  If the columns do not yet
#   exist, they will be created

CREATE_NEW_TABLES=True #create fresh tables if True, build on old if False

#set to 'WARN' to capure only data loading issues.  'DEBUG' is verbose.
#options:  ERROR, CRITICAL, WARNING, INFO, DEBUG
LOG_LEVEL=logging.DEBUG


#	specify the shape and location of the raw data files here

#assuming square matrices, the number of transportation zones (rows/columns)
ZONES=1179

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


