
from mappings import scenarios, map_file
from mappings import DB 
from  database import *
import psycopg2
import csv

#grab all the data tables from the map file
reader=csv.DictReader(open(map_file))
cs_tables=set()
totals_tables=set()

for line in reader:
	cs_tables.add(line['dbtable'])
	totals_tables.add(line['rollup_dbtable'])
	


#make a db connection (let main app create the db if needed)
conn = psycopg2.connect(database = DB,
                            user=login_info['user'],
                            password=login_info['password']
                            )

curs = conn.cursor() 

for s in scenarios[1:]:
	name=s['name']
	print('Costs deltas for {}: (relative to the "no highway, no Red Line" case)\n'.format(name))
	for table_set in [cs_tables, totals_tables]:
		for table_name in table_set:
			for income in range(1,6):
				tname=name+"_"+"inc"+str(income)+"_"+table_name
				curs.execute("SELECT column_name FROM information_schema.columns WHERE table_name ='{}'".format(tname))
				cols =curs.fetchall()
				print("Income group {}".format(income))
				for col in cols:
					curs.execute("SELECT SUM({}) FROM {}".format(col, tname))
					total=curs.fetchall()
					curs.execute("END")
					print("\t{}\t\t{}".format(total))
	
	
	

#nored_yeshwy_inc1_dlta_costs
